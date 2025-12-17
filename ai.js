import { getModelId, safeParseJsonLoosely, promiseWithTimeout, normalizeTF, selectSmartContextRows, inferLikelyCurrentTF } from './utils.js';
import { TF_VALIDITY_MS, TF_ORDER } from './config.js';
import { getAllAnalyses, saveAnalysis } from './database.js';

// --- FAST FALLBACK for Timeout (Free-tier 30s limit protection) ---
export function createFallbackAnalysis(userId, detectedTf, existingContext = null) {
  // If we have existing context, preserve as much data as possible
  if (existingContext && typeof existingContext === 'object') {
    const existingData = existingContext.detailed_technical_data || existingContext;
    return {
      detected_tf: detectedTf || existingContext?.detected_tf || 'Unknown',
      tfs_used_for_confluence: existingContext?.tfs_used_for_confluence || [],
      request_update_for_tf: existingContext?.request_update_for_tf || null,
      reasoning_trace: ['Timeout: Using cached analysis from previous successful analysis.'],
      detailed_technical_data: {
        trend_bias: existingData?.trend_bias || 'Unknown',
        structure: existingData?.structure || { parent_bias: 'Unknown', market_structure: 'Unknown' },
        value: existingData?.value || { at_key_level: false, key_levels_summary: 'Pending full analysis' },
        trigger: existingData?.trigger || { candlestick_patterns: [], divergence: 'unknown' },
        trade_setup: existingData?.trade_setup || { action: 'WAIT', confidence: 'Low', risk_flags: [] }
      },
      user_response_text: `⏳ **สถานะ: WAIT (Low)**\n⏱️ **TF ปัจจุบัน:** ${detectedTf || 'Unknown'}\n\n📌 **ข้อมูลจากการวิเคราะห์ครั้งก่อน**\nกำลังประมวลผล... กรุณารอสักครู่`
    };
  }
  
  // Fallback if no context available
  return {
    detected_tf: detectedTf || 'Unknown',
    tfs_used_for_confluence: [],
    request_update_for_tf: null,
    reasoning_trace: ['Timeout: Using cached/fallback analysis. Please re-upload for full analysis.'],
    detailed_technical_data: {
      trend_bias: 'Unknown',
      structure: { parent_bias: 'Unknown', market_structure: 'Unknown' },
      value: { at_key_level: false, key_levels_summary: 'Pending full analysis' },
      trigger: { candlestick_patterns: [], divergence: 'unknown' },
      trade_setup: { action: 'WAIT', confidence: 'Low', risk_flags: ['Timeout - Incomplete Analysis'] }
    },
    user_response_text: '⚠️ **สถานะ: WAIT (Low)**\n⏱️ **TF ปัจจุบัน:** ' + (detectedTf || 'Unknown') + '\n\n⚠️ การวิเคราะห์ใช้เวลานาน กรุณาส่งรูปกราฟใหม่เพื่อให้ได้ผลลัพธ์ที่ถูกต้อง'
  };
}

// --- CORE ANALYSIS (UPDATED FOR TOP-DOWN + FRESHNESS AWARENESS) ---

export async function analyzeChartStructured(userId, base64Image, existingRows, env, options = {}) {
  const modelId = getModelId(env);
  const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/' + modelId + ':generateContent?key=' + env.GEMINI_API_KEY;
  const signal = options.signal;
  const mimeType = options.mimeType || 'image/jpeg';

  // 1) ENRICH WITH FRESHNESS INFO (like trade style)
  const enrichedRows = (existingRows || []).map(row => {
    const tf = normalizeTF(row.tf);
    const maxAge = TF_VALIDITY_MS[tf] || (24 * 60 * 60 * 1000);
    const ageMs = Date.now() - Number(row.timestamp || 0);
    const isFresh = ageMs <= maxAge;
    const freshnessPercent = Math.max(0, Math.min(100, Math.round((1 - ageMs / maxAge) * 100)));
    const ageMins = Math.floor(ageMs / 60000);
    return {
      ...row,
      tf,
      isFresh,
      freshnessPercent,
      ageMins
    };
  });

  // 2) FILTER VALID (FRESH) DATA
  const validRows = enrichedRows.filter(row => row.isFresh);

  // 3) SMART CONTEXT (reduce noise): send only relevant Parent TF chain
  const likelyTf = inferLikelyCurrentTF(validRows);
  const contextRows = selectSmartContextRows(validRows, likelyTf);

  // Prepare Context string with FRESHNESS PERCENTAGES
  let existingContextStr = "No valid higher timeframe data available.";
  if (contextRows.length > 0) {
    existingContextStr = "=== VALID EXISTING DATA (SMART CONTEXT: PARENT TFs ONLY WITH FRESHNESS) ===\n";
    existingContextStr += "Context selection based on last-updated TF: " + (likelyTf || 'Unknown') + "\n";
    existingContextStr += "Freshness Scale: 🟢 >75% (Fresh) | 🟡 50-75% (Aging) | 🔴 <50% (Stale)\n";
    existingContextStr += "--------------------------------\n";
    contextRows.forEach(row => {
      const data = JSON.parse(row.analysis_json);
      const freshBadge = row.freshnessPercent >= 75 ? '🟢' : row.freshnessPercent >= 50 ? '🟡' : '🔴';
      existingContextStr += `
        [TF: ${row.tf}] ${freshBadge} Freshness: ${row.freshnessPercent}%
        - Updated: ${row.ageMins} mins ago
        - Trend Bias: ${data.trend_bias || 'Unknown'}
        - Setup Action: ${data.trade_setup?.action || 'N/A'}
        - Entry Zone: ${data.trade_setup?.entry_zone || 'N/A'}
        - Key Levels: ${data.value?.key_levels_summary || data.key_levels?.summary || 'N/A'}
        --------------------------------
      `;
    });
  }

  // UPDATED SYSTEM PROMPT: Optimized for speed + Strict Top-Down + Freshness Awareness
  const systemInstruction = {
    role: "user",
    parts: [{ text: `ROLE: Expert Technical Analyst (Thai ONLY).
METHOD: Strict Top-Down (Structure -> Value -> Trigger) + Confluence + Freshness Check.

CONTEXT:
${existingContextStr}

FRESHNESS RULE: 🟢 >75%=Fresh | 🟡 50-75%=Aging | 🔴 <50%=Stale
IF critical HTF <50% fresh → add to request_update_for_tf
IF all Parent TFs <30% → respond WAIT with request_update_for_tf

HARD RULES (CRITICAL):
1. NO counter-trend vs HTF unless at major key level + clear reversal trigger. Exception = Counter-trend (High Risk)
2. No Man's Land (no value zone) = WAIT
3. Indicators confirm only, don't set direction
4. If HTF <50% fresh and critical → flag for update
5. Output Thai ONLY

ANALYSIS (Hierarchical):
- P1: Market Structure + HTF bias check + freshness concern
- P2: Are we at Value zone? (S/R, Fib, Key levels)
- P3: Entry Trigger confirmed? (Patterns + Indicators)

ANTI-HALLUCINATION: If unsure → use null/"unknown". No guessing prices.

OUTPUT: JSON ONLY with this exact structure:
{
  "detected_tf": "TF detected from image",
  "tfs_used_for_confluence": ["list", "of", "TFs"],
  "request_update_for_tf": null or ["HTF_LIST"],
  "reasoning_trace": ["P1: ...", "P2: ...", "P3: ..."],
  "detailed_technical_data": {
    "trend_bias": "Bullish/Bearish/Sideway",
    "structure": { "parent_bias": "...", "market_structure": "HH/HL/LH/LL/Range" },
    "value": { "at_key_level": true/false, "key_levels_summary": "" },
    "trigger": { "candlestick_patterns": [], "divergence": "none/bullish/bearish", "indicator_snapshot": {} },
    "trade_setup": { "action": "BUY/SELL/WAIT/HOLD", "entry_zone": null, "target_price": null, "stop_loss": null, "confidence": "High/Medium/Low", "risk_flags": [] }
  },
  "user_response_text": "THAI ONLY output in EXACT format:\n\n📢 **สถานะ: [ACTION] (Confidence)**\n⏱️ **TF:** [Detected]\n📚 **Confluence:** [TF list]\n\n🔍 **Top-Down:**\n1️⃣ **Structure:** [HTF bias + current structure + conflict]\n2️⃣ **Value:** [Key levels or 'No Man's Land']\n3️⃣ **Trigger:** [Patterns/Indicators]\n\n🎯 **Setup:**\n- **Entry:** [Zone]\n- **TP:** [Price]\n- **SL:** [Price]\n\n💡 **สรุป:** [Confluence strength + risks. Max 10-20 sentences]"
}`
    }]
  };

  const userMessage = {
    role: "user",
    parts: [
      { text: "Analyze this chart strictly using Top-Down Analysis logic. Output THAI ONLY." },
      { inline_data: { mime_type: mimeType, data: base64Image } }
    ]
  };

  const payload = {
    contents: [systemInstruction, userMessage],
    generationConfig: {
      temperature: 0.2,
      topK: 40,
      topP: 0.8,
      maxOutputTokens: Number(env.AI_MAX_OUTPUT_TOKENS || 1200)
    }
  };

  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal
  });

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    throw new Error('AI API Error: ' + response.status + ' ' + (errText ? ('- ' + errText.slice(0, 200)) : ''));
  }

  try {
    const data = await response.json();
    const rawText = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
    return safeParseJsonLoosely(rawText);
  } catch (e) {
    console.error("JSON Parse Error", e);
    return {
      detected_tf: 'Unknown',
      tfs_used_for_confluence: [],
      request_update_for_tf: null,
      reasoning_trace: ["ParseError: Invalid JSON from model"],
      detailed_technical_data: { note: "Analysis failed" },
      user_response_text: "⚠️ เกิดข้อผิดพลาดในการประมวลผล (JSON Structure Error)"
    };
  }
}

// --- CHAT WITH CONTEXT (DB-FIRST AGENTIC) ---

export async function chatWithGeminiText(userId, userText, env) {
  const modelId = getModelId(env);
  const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/' + modelId + ':generateContent?key=' + env.GEMINI_API_KEY;

  // 1) Detect requested TF (if any)
  const tfRegex = new RegExp('(M1|M5|M15|M30|H1|H4|1D|D1|1W|WEEK|DAY|HOUR)', 'i');
  const match = userText.match(tfRegex);

  let targetTF = null;
  if (match) targetTF = normalizeTF(match[0]);

  // 2) Fetch ALL data from D1 (DB-First)
  const rows = await getAllAnalyses(userId, env);

  // 3) Determine freshness per TF (still enforce safety)
  const enriched = (rows || []).map(r => {
    const tf = normalizeTF(r.tf);
    const maxAge = TF_VALIDITY_MS[tf] || (24 * 60 * 60 * 1000);
    const ageMs = Date.now() - r.timestamp;
    const isFresh = ageMs <= maxAge;
    let data = {};
    try { data = JSON.parse(r.analysis_json || '{}'); } catch (_) { data = {}; }

    return {
      tf,
      isFresh,
      ageMins: Math.floor(ageMs / 60000),
      timestamp_readable: r.timestamp_readable,
      data
    };
  });

  // 4) If user explicitly asks for a TF, require fresh data for that TF
  if (targetTF) {
    const hasFresh = enriched.some(x => x.tf === targetTF && x.isFresh);
    if (!hasFresh) {
      return '❌ ไม่พบข้อมูล ' + targetTF + ' ที่เป็นปัจจุบัน\n\n📸 กรุณาส่งรูปกราฟ Timeframe **' + targetTF + '** เข้ามาก่อน เพื่อให้ผมวิเคราะห์ต่อได้ครับ';
    }
  }

  // 5) Build DB state summary (include all TFs; mark stale vs fresh)
  let marketState = "";
  if (enriched.length > 0) {
    // Prefer HTF -> LTF order
    enriched.sort((a, b) => TF_ORDER.indexOf(a.tf) - TF_ORDER.indexOf(b.tf));

    const lines = enriched.map(x => {
      const d = x.data || {};
      const setup = d.trade_setup || d.detailed_technical_data?.trade_setup || {};
      const value = d.value || d.detailed_technical_data?.value || {};
      const trigger = d.trigger || d.detailed_technical_data?.trigger || {};
      const structure = d.structure || d.detailed_technical_data?.structure || {};

      const freshness = x.isFresh ? "🟢 Fresh" : "🔴 Stale";
      const keyLevels = value?.key_levels_summary || d.key_levels?.summary || '-';
      const trig = [
        trigger?.divergence ? 'Div=' + trigger.divergence : null,
        Array.isArray(trigger?.candlestick_patterns) && trigger.candlestick_patterns.length ? 'Patterns=' + trigger.candlestick_patterns.join(',') : null
      ].filter(Boolean).join(' | ') || '-';

      return [
        '- TF ' + x.tf + ' (' + freshness + ', Age=' + x.ageMins + 'm, Updated=' + (x.timestamp_readable || '-') + ')',
        '  Trend=' + (d.trend_bias || structure?.parent_bias || 'Unknown') + ' | Action=' + (setup?.action || 'N/A'),
        '  Entry=' + (setup?.entry_zone || '-') + ' | TP=' + (setup?.target_price || '-') + ' | SL=' + (setup?.stop_loss || '-'),
        '  P1(Structure)=' + (structure?.market_structure || '-') + ' | P2(Value)=' + keyLevels + ' | P3(Trigger)=' + trig
      ].join('\n');
    }).join('\n');

    marketState = '=== CURRENT MARKET STATE (Database: ALL TFs) ===\n' + lines + '\n===============================================';
  } else {
    marketState = '=== CURRENT MARKET STATE ===\nNo technical data available in database.\nUser must upload charts first.\n================================';
}

  // 6) LLM Response Generation (Hard Rules + DB-first + Thai-only output)
  const payload = {
    contents: [{
      role: "user",
      parts: [{ text: 'Role: Assistant Trader & Technical Analyst (Thai Language ONLY - NO ENGLISH).\n\n' + marketState + '\n\nUser Question: "' + userText + '"\n\nHard Rules:\n- Answer STRICTLY in Thai language only. NO English at all.\n- Answer strictly based on the Database state above (no hallucinated prices/trends).\n- Respect Top-Down: do not recommend counter-trend against the highest available Parent TF bias, unless the DB explicitly shows price at a major HTF key level + clear reversal trigger.\n- If data is missing/stale for any critical TF to answer safely, ask the user to upload that TF.\n\nOutput format:\n- Provide answer in Thai only (concise, actionable, no internal reasoning steps).\n- Think the analysis internally, output only the conclusion.\n- Use emoji for clarity (🔍 🎯 ⚠️ etc.)\n      ' }]
    }],
    generationConfig: {
      temperature: 0.2,
      maxOutputTokens: Number(env.AI_MAX_OUTPUT_TOKENS || 1200)
    }
  };

  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    if (!response.ok) return "⚠️ AI Error: Unable to process request.";
    const data = await response.json();
    return data.candidates?.[0]?.content?.parts?.[0]?.text || "ขออภัย ไม่สามารถตอบคำถามได้ขณะนี้";
  } catch (e) {
    console.error(safeError(e));
    return "⚠️ System Error during chat.";
  }
}

// --- TRADE STYLE ANALYSIS ---

function pickMostRecentRowByTF(rows, candidateTFs) {
  let best = null;
  for (const t of candidateTFs) {
    const tf = normalizeTF(t);
    const r = (rows || []).find(x => normalizeTF(x.tf) === tf);
    if (r && (!best || Number(r.timestamp || 0) > Number(best.timestamp || 0))) best = r;
  }
  return best;
}

export function selectRowsForTradeStyle(validRows, mode) {
  // validRows: raw DB rows already filtered by age rules (TF_VALIDITY_MS)
  const byTF = new Map((validRows || []).map(r => [normalizeTF(r.tf), r]));
  const picked = new Map();

  const add = (row) => {
    if (!row) return;
    const tf = normalizeTF(row.tf);
    if (!tf) return;
    picked.set(tf, row);
  };

  if (mode === 'SCALP') {
    // Trigger: prefer freshest LTF
    const ltfCandidates = ['M1', 'M5', 'M15', 'M30'];
    const ltf = pickMostRecentRowByTF(validRows, ltfCandidates) || byTF.get('M15') || byTF.get('M5') || byTF.get('M1');
    add(ltf);

    const likelyTf = ltf ? normalizeTF(ltf.tf) : 'M5';
    // Parents for confluence (HTF chain)
    const parents = selectSmartContextRows(validRows, likelyTf);
    for (const r of parents) add(r);

    // Ensure we keep at least H1 if exists (helps filter noise)
    add(byTF.get('H1') || null);

  } else if (mode === 'SWING') {
    // Structure: prefer 1D/H4
    add(byTF.get('1D') || null);
    add(byTF.get('H4') || null);
    add(byTF.get('1W') || null);

    // Trigger: add freshest mid TF if available
    const trig = pickMostRecentRowByTF(validRows, ['H1', 'M30', 'M15']);
    add(trig);

  } else {
    // Unknown mode -> fallback to safer HTF chain
    const fallback = selectSmartContextRows(validRows, 'M15');
    for (const r of fallback) add(r);
  }

  const arr = Array.from(picked.values());
  arr.sort((a, b) => TF_ORDER.indexOf(normalizeTF(a.tf)) - TF_ORDER.indexOf(normalizeTF(b.tf)));
  return arr;
}

export function buildTradeStyleContext(enrichedSelected, mode) {
  const header = '=== DB CONTEXT FOR ' + mode + ' (Selected TFs Only / Smart Context) ===';
  const lines = (enrichedSelected || []).map(x => {
    const d = x.data || {};
    const detailed = d.detailed_technical_data || {};
    const tradeSetup = detailed.trade_setup || d.trade_setup || {};
    const structure = detailed.structure || d.structure || {};
    const value = detailed.value || d.value || {};
    const trigger = detailed.trigger || d.trigger || {};
    const indicators = detailed.indicators || d.indicators || {};

    // Keep compact to reduce token usage, include freshness percentage
    const freshnessBadge = (x.freshnessPercent !== undefined) 
      ? (x.freshnessPercent >= 75 ? '🟢 ' + x.freshnessPercent + '%' : x.freshnessPercent >= 50 ? '🟡 ' + x.freshnessPercent + '%' : '🔴 ' + x.freshnessPercent + '%')
      : (x.isFresh ? '🟢 Fresh' : '🔴 Stale');
    const recommendStr = (x.recommendUpdate) ? ' [REQUEST UPDATE]' : '';
    
    return [
      '[TF ' + x.tf + '] ' + freshnessBadge + ' | Age=' + x.ageMins + 'm | Updated=' + (x.timestamp_readable || '-') + recommendStr,
      '- TrendBias: ' + (d.trend_bias || detailed.trend_bias || '-'),
      '- Structure: ' + JSON.stringify(structure || {}).slice(0, 380),
      '- Value: ' + JSON.stringify(value || {}).slice(0, 380),
      '- Trigger: ' + JSON.stringify(trigger || {}).slice(0, 380),
      '- Indicators: ' + JSON.stringify(indicators || {}).slice(0, 380),
      '- Setup: ' + JSON.stringify(tradeSetup || {}).slice(0, 380),
      '---'
    ].join('\n');
  });

  return [header, lines.join('\n')].join('\n');
}

export async function analyzeTradeStyleWithGemini(mode, contextStr, env) {
  const modelId = getModelId(env);
  const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/' + modelId + ':generateContent?key=' + env.GEMINI_API_KEY;

  const modeThai = (mode === 'SCALP') ? 'เล่นสั้น (Scalp)' : 'เล่นสวิง (Swing)';

  const baseText = '\nRole: Expert Technical Analyst (Thai).\nTask: Create a trading plan for mode = "' + modeThai + '" using ONLY the DB context provided.\nMethodology: Strict Top-Down (Structure -> Value -> Trigger) + Confluence.\n\n' + contextStr + '\n\n*** FRESHNESS & UPDATE POLICY ***\nEach TF has a freshness percentage (shown as 🟢/🟡/🔴 %):\n- 🟢 (>75%): Fresh, high confidence\n- 🟡 (50-75%): Aging, use with caution\n- 🔴 (<50%): Stale, data may be outdated\n- [REQUEST UPDATE] flag: AI marked this TF as recommended for update\n\nYou MUST evaluate:\n1) If TF is <50% freshness and is CRITICAL for your decision (HTF bias, key structure, entry trigger), add to request_update_for_tf.\n2) If TF is <50% freshness but supporting only, you may proceed with caution (add to risk_notes).\n3) Priority: HTF (1D/H4) freshness > MTF (H1/M30) > LTF (M15/M5) for structure decisions.\n\n*** HARD RULES (MUST FOLLOW) ***\n1) Strict Top-Down: Direction must follow Higher TF (1D/H4) first.\n2) No counter-trend trades. Exception ONLY when price is at a clearly major Support/Resistance or key Fib zone; if exception applies, you MUST label "Counter-trend (Risky)" and reduce confidence.\n3) If price is in No Man\'s Land (no clear value zone), output action = WAIT.\n4) Indicators (RSI/MACD/Stoch/MA/Volume) are CONFIRMATION only, not direction setters.\n5) Use only what exists in DB context. If missing critical TFs for safe call, set request_update_for_tf accordingly.\n6) If critical HTF data is too stale (<30% freshness), prefer WAIT action with request_update_for_tf.\n\n*** MODE-SPECIFIC GUIDANCE ***\n- SCALP: prioritize precise trigger on LTF, tight invalidation, quick TP; still filter by HTF bias. Ensure H1 is fresh (>50%).\n- SWING: prioritize HTF structure/value; TP wider; trigger can be from H1/M30. Ensure 1D/H4 is fresh (>50%).\n\n*** OUTPUT FORMAT (JSON ONLY + THAI LANGUAGE ONLY) ***\n{\n  "mode": "SCALP|SWING",\n  "tfs_used_for_confluence": ["1D","H4","H1","M15"],\n  "request_update_for_tf": ["1D", "H4"] | null,\n  "reasoning_trace": [\n    "P1 Structure: ... (freshness concern if any)",\n    "P2 Value: ...",\n    "P3 Trigger: ..."\n  ],\n  "trade_plan": {\n    "action": "BUY|SELL|WAIT|HOLD",\n    "entry_zone": "...",\n    "target_price": "...",\n    "stop_loss": "...",\n    "confidence": "High|Medium|Low",\n    "probability_pct": 0\n  },\n  "risk_notes": [\n    "Any data freshness concerns",\n    "Counter-trend warnings if applicable"\n  ],\n  "user_response_text": "CRITICAL: Output THAI LANGUAGE ONLY. Use exactly this format:\\n\\n⚡ **โหมด:** ' + modeThai + '\\n📢 **สถานะ:** [ACTION] (Confidence/Probability%)\\n📚 **TF ที่ใช้:** [List TFs]\\n\\n🔍 **Top-Down Analysis:**\\n1️⃣ **Structure (ภาพใหญ่):** [HTF bias, current structure, any conflict]\\n2️⃣ **Area of Value:** [Key levels/Fib zones]\\n3️⃣ **Entry Trigger:** [Patterns/Indicators]\\n\\n🎯 **Setup:**\\n- **Entry:** [Zone]\\n- **TP:** [Target]\\n- **SL:** [Stop]\\n\\n💡 **สรุป:** [Confluence strength + risk warnings. Max 10-20 sentences. Do NOT output reasoning steps - think privately.]"\n}\n      ';

  const instruction = {
    role: "user",
    parts: [{
      text: baseText
    }]
  };

  const payload = {
    contents: [instruction],
    generationConfig: {
      temperature: 0.2,
      topP: 0.8,
      maxOutputTokens: 1200
    }
  };

  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const errText = await response.text().catch(() => '');
    throw new Error('AI API Error: ' + response.status + ' ' + (errText ? ('- ' + errText.slice(0, 200)) : ''));
  }

  const data = await response.json();
  const rawText = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
  return safeParseJsonLoosely(rawText);
}

// --- DB RE-ANALYZE ---

export async function analyzeDBStructured(userId, dbRows, env, options = {}) {
  const modelId = getModelId(env);
  const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/' + modelId + ':generateContent?key=' + env.GEMINI_API_KEY;

  // Build compact DB context (HTF -> LTF)
  const rows = (dbRows || []).slice().sort((a, b) => TF_ORDER.indexOf(normalizeTF(a.tf)) - TF_ORDER.indexOf(normalizeTF(b.tf)));
  const lines = rows.map(r => {
    const d = r.data || JSON.parse(r.analysis_json || '{}');
    const tf = normalizeTF(r.tf);
    const fresh = (r.isFresh === undefined) ? true : !!r.isFresh;
    return '[TF ' + tf + '] ' + (fresh ? '🟢 Fresh' : '🔴 Stale') + ' | Updated=' + (r.timestamp_readable || '-') + ' | Trend=' + (d.trend_bias || '-') + ' | Entry=' + ((d.trade_setup||{}).entry_zone || '-') + ' | TP=' + ((d.trade_setup||{}).target_price || '-') + ' | SL=' + ((d.trade_setup||{}).stop_loss || '-');
  }).join('\n');

  const instruction = {
    role: 'user',
    parts: [{ text: 'Role: Expert Technical Analyst (Thai).\nTask: Re-evaluate the provided DB state for each TF listed below.\nDo NOT invent prices. Use only the given DB values. For large TFs (H4/1D/1W) you MAY request an update by returning them in request_update_for_tf.\n\nDB CONTEXT:\n' + lines + '\n\nOUTPUT: Return JSON ONLY with shape:\n{ "results": [ { "detected_tf": "M15", "tfs_used_for_confluence": ["H4","H1"], "request_update_for_tf": null|[...], "reasoning_trace": [...], "detailed_technical_data": {...}, "user_response_text": "..." } ] }' }]
  };

  const payload = { contents: [instruction], generationConfig: { temperature: 0.2, maxOutputTokens: Number(env.AI_MAX_OUTPUT_TOKENS || 1600) } };

  const resp = await fetch(apiUrl, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
  if (!resp.ok) {
    const t = await resp.text().catch(() => '');
    throw new Error('AI API Error: ' + resp.status + ' ' + (t ? ('- ' + t.slice(0, 200)) : ''));
  }
  const data = await resp.json();
  const rawText = data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
  return safeParseJsonLoosely(rawText);
}

export async function reanalyzeFromDB(userId, env, requestUrl) {
  try {
    const all = (await getAllAnalyses(userId, env)) || [];
    const rows = all.filter(r => !String(r.tf || '').startsWith('_'));

    // Enrich freshness
    const enriched = rows.map(r => {
      const tf = normalizeTF(r.tf);
      const ts = Number(r.timestamp || 0);
      const ageMs = ts ? (Date.now() - ts) : Number.MAX_SAFE_INTEGER;
      const maxAge = TF_VALIDITY_MS[tf];
      const isFresh = !maxAge ? true : ageMs <= maxAge;
      let data = {};
      try { data = JSON.parse(r.analysis_json || '{}'); } catch (_) { data = {}; }
      return { tf: tf, timestamp: ts, timestamp_readable: r.timestamp_readable, ageMs, isFresh, data, analysis_json: r.analysis_json };
    });

    const freshRows = enriched.filter(r => r.isFresh);
    if (freshRows.length === 0) {
      // Nothing fresh to re-analyze: write marker and exit
      const now = Date.now();
      const nowR = new Date(now).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
      await saveAnalysis(userId, '_REANALYZE', now, nowR, { status: 'no_fresh_data', inspected_count: enriched.length }, env);
      return;
    }

    // Ask LLM to re-evaluate DB context
    const result = await analyzeDBStructured(userId, freshRows, env);
    const results = Array.isArray(result?.results) ? result.results : [];

    const now = Date.now();
    const nowReadable = new Date(now).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });

    for (const r of results) {
      try {
        const detectedTF = normalizeTF(r.detected_tf || r.tf || 'Unknown');
        const detailed = r.detailed_technical_data || r.detailed || {};
        const toStore = {
          detected_tf: detectedTF,
          tfs_used_for_confluence: r.tfs_used_for_confluence || [],
          request_update_for_tf: r.request_update_for_tf || null,
          trend_bias: detailed.trend_bias || (detailed.structure && detailed.structure.parent_bias) || 'Unknown',
          trade_setup: detailed.trade_setup || {},
          reasoning_trace: r.reasoning_trace || [],
          structure: detailed.structure || {},
          value: detailed.value || {},
          trigger: detailed.trigger || {},
          indicators: detailed.indicators || {},
          key_levels: detailed.key_levels || {},
          raw_extraction: detailed.raw_extraction || {},
          notes: detailed.notes || null,
          reanalysis: true
        };

        if (Array.isArray(r.request_update_for_tf) && r.request_update_for_tf.length > 0) {
          toStore.trade_setup = { ...(toStore.trade_setup || {}), action: 'WAIT', confidence: 'Low' };
          const rt = Array.isArray(toStore.reasoning_trace) ? toStore.reasoning_trace : [];
          rt.push('Decision: WAIT (ต้องการภาพ TF เพิ่มเติม: ' + r.request_update_for_tf.join(', ') + ')');
          toStore.reasoning_trace = rt;
        }

        await saveAnalysis(userId, detectedTF, now, nowReadable, toStore, env);
      } catch (e) {
        console.error('Failed to save reanalysis result for row:', e);
      }
    }

    // Save summary marker
    await saveAnalysis(userId, '_REANALYZE', now, nowReadable, { status: 'done', count: results.length, finishedAt: now }, env);
  } catch (e) {
    console.error('Reanalyze failed:', e);
    try {
      const at = Date.now();
      const r = new Date(at).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
      await saveAnalysis(userId, '_REANALYZE', at, r, { status: 'error', error: String(e?.message || e) }, env);
    } catch (_) {}
  }
}
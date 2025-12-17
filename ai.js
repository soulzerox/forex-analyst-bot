import { getModelId, safeParseJsonLoosely, promiseWithTimeout, normalizeTF, selectSmartContextRows, inferLikelyCurrentTF } from './utils.js';
import { TF_VALIDITY_MS, TF_ORDER } from './config.js';
import { getAllAnalyses, saveAnalysis } from './database.js';

// --- CORE ANALYSIS (UPDATED FOR TOP-DOWN) ---

export async function analyzeChartStructured(userId, base64Image, existingRows, env, options = {}) {
  const modelId = getModelId(env);
  const apiUrl = 'https://generativelanguage.googleapis.com/v1beta/models/' + modelId + ':generateContent?key=' + env.GEMINI_API_KEY;
  const signal = options.signal;
  const mimeType = options.mimeType || 'image/jpeg';

  // 1) FILTER STALE DATA
  const validRows = (existingRows || []).filter(row => {
    const maxAge = TF_VALIDITY_MS[row.tf];
    if (!maxAge) return true;
    const age = Date.now() - row.timestamp;
    return age <= maxAge;
  });

  // 2) SMART CONTEXT (reduce noise): send only relevant Parent TF chain
  const likelyTf = inferLikelyCurrentTF(validRows);
  const contextRows = selectSmartContextRows(validRows, likelyTf);

  // Prepare Context string
  let existingContextStr = "No valid higher timeframe data available.";
  if (contextRows.length > 0) {
    existingContextStr = "=== VALID EXISTING DATA (SMART CONTEXT: PARENT TFs ONLY) ===\n";
    existingContextStr += "Context selection based on last-updated TF: " + (likelyTf || 'Unknown') + "\n";
    existingContextStr += "--------------------------------\n";
    contextRows.forEach(row => {
      const data = JSON.parse(row.analysis_json);
      const ageMins = Math.floor((Date.now() - row.timestamp) / 60000);
      existingContextStr += `
        [TF: ${row.tf}]
        - Updated: ${ageMins} mins ago
        - Trend Bias: ${data.trend_bias || 'Unknown'}
        - Setup Action: ${data.trade_setup?.action || 'N/A'}
        - Entry Zone: ${data.trade_setup?.entry_zone || 'N/A'}
        - Key Levels: ${data.value?.key_levels_summary || data.key_levels?.summary || 'N/A'}
        --------------------------------
      `;
    });
  }

  // UPDATED SYSTEM PROMPT: Strict Top-Down + Hard Rules + CoT (internal)
  const systemInstruction = {
    role: "user",
    parts: [{ text: `
      Role: Expert Technical Analyst (Thai Language).
      Methodology: Strict Top-Down Analysis (Structure -> Value -> Trigger) with Confluence.

      ${existingContextStr}

      *** HARD RULES (NON-NEGOTIABLE) ***
      1) **ห้ามเทรดสวนเทรนใหญ่ (HTF) เด็ดขาด** ไม่ว่าจะขาขึ้นหรือขาลง
         - ถ้า TF ปัจจุบันให้สัญญาณ BUY แต่ TF ใหญ่ (Parent TFs ใน Context) เป็น Bearish ชัดเจน => ต้องตอบ "WAIT" เป็นหลัก
         - ถ้า TF ปัจจุบันให้สัญญาณ SELL แต่ TF ใหญ่เป็น Bullish ชัดเจน => ต้องตอบ "WAIT" เป็นหลัก
      2) **ข้อยกเว้นเดียว**: เทรดสวนเทรนได้เฉพาะ "ชนแนวรับ/แนวต้านสำคัญจริงๆ (HTF Key Level)" + มี "สัญญาณกลับตัวชัดเจน (Trigger)"
         - ในกรณีข้อยกเว้น ต้องติดป้ายเตือน **Counter-trend (High Risk)** และห้ามให้ความมั่นใจสูง
      3) Indicators ใช้เพื่อ "ยืนยัน" เท่านั้น ห้าม override โครงสร้างตลาด (Structure)

      *** ANALYSIS LOGIC (HIERARCHY OF IMPORTANCE) ***

      1. PRIORITY 1: Market Structure (Big Picture)
         - Identify TF of the NEW image first.
         - Read Smart Context for Parent TFs (e.g., H4 for M15/M5; 1D for H4; etc.).
         - Determine Main Bias (HH/HL = Up, LH/LL = Down, else Sideway).
         - Conflict Check: if signals conflict with Parent TF => default WAIT unless exception (Hard Rule #2).

      2. PRIORITY 2: Area of Value (Key Levels)
         - Is price at a major Support/Resistance, Supply/Demand, or Key Fibonacci zone (61.8/50.0)?
         - If "No Man's Land" => WAIT.

      3. PRIORITY 3: Entry Trigger (Signals)
         - Only after P1 & P2: check RSI/MACD/Stoch, volume hints, candlestick patterns (Pinbar/Engulfing), divergence.
         - Confirm with confluence only.

      *** ACCURACY / ANTI-HALLUCINATION ***
      - If any value cannot be read from the image with confidence, use null or "unknown" (do NOT guess).
      - Think step-by-step internally (CoT) to reduce mistakes, but do NOT output your private step-by-step reasoning.
      - Instead, fill "reasoning_trace" with concise bullet points referencing PRIORITY 1/2/3 evidence and the decision.

      *** OUTPUT INSTRUCTION ***
      - Identify detected TF.
      - List ALL Timeframes used in this analysis (Current + From Smart Context).
      - Provide Thai response strictly following Top-Down logic and Hard Rules.
      - Store detailed data (P1/P2/P3 extraction) in detailed_technical_data for reuse by other TF.

      *** OUTPUT FORMAT (JSON ONLY) ***
      {
        "detected_tf": "e.g. M15",
        "tfs_used_for_confluence": ["H4", "H1", "M15"],
        "request_update_for_tf": ["H4"], 
        "reasoning_trace": [
          "P1: ...",
          "P2: ...",
          "P3: ...",
          "Decision: ... (Counter-trend risk / Confluence)"
        ],
        "detailed_technical_data": {
          "trend_bias": "Bullish/Bearish/Sideway",
          "structure": {
            "parent_bias": "Bullish/Bearish/Sideway/Unknown",
            "market_structure": "HH/HL or LH/LL or Range",
            "key_structure_points": {
              "last_swing_high": null,
              "last_swing_low": null
            },
            "notes": ""
          },
          "value": {
            "at_key_level": true,
            "key_levels_summary": "",
            "support_levels": [],
            "resistance_levels": [],
            "supply_demand_zones": [],
            "fibonacci": {
              "in_zone": true,
              "levels": {
                "0.5": null,
                "0.618": null
              }
            }
          },
          "trigger": {
            "candlestick_patterns": [],
            "divergence": "none/bullish/bearish/unknown",
            "indicator_snapshot": {
              "rsi": null,
              "macd": null,
              "stoch": null,
              "volume": null
            }
          },
          "trade_setup": {
            "action": "BUY/SELL/WAIT/HOLD",
            "entry_zone": null,
            "target_price": null,
            "stop_loss": null,
            "confidence": "High/Medium/Low",
            "risk_flags": []
          },
          "raw_extraction": {
            "price_axis_hint": null,
            "time_axis_hint": null,
            "ohlc": [],
            "notes": ""
          }
        },
        "user_response_text": "Generate a concise Thai response using this format:

📢 **สถานะ: [ACTION] (Confidence Level)**
⏱️ **TF ปัจจุบัน:** [Detected TF]
📚 **ข้อมูลประกอบ (Confluence):** [List TFs used]

🔍 **Top-Down Analysis:**
1️⃣ **Structure (ภาพใหญ่):** [HTF bias + current structure + conflict/warning]
2️⃣ **Area of Value:** [Key Levels/Fib/S-R]
3️⃣ **Entry Trigger:** [Patterns/Indicators confirming]

🎯 **Setup:**
- **Entry:** [Zone]
- **TP:** [Target]
- **SL:** [Stop]

💡 **สรุป:** [Confluence + Counter-trend risk or why WAIT]."
      `
    }]
  };

  const userMessage = {
    role: "user",
    parts: [
      { text: "Analyze this chart strictly using Top-Down Analysis logic and Hard Rules." },
      { inline_data: { mime_type: mimeType, data: base64Image } }
    ]
  };

  const payload = {
    contents: [systemInstruction, userMessage],
    generationConfig: {
      temperature: 0.2,
      maxOutputTokens: Number(env.AI_MAX_OUTPUT_TOKENS || 1800)
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

  // 6) LLM Response Generation (Hard Rules + DB-first)
  const payload = {
    contents: [{
      role: "user",
      parts: [{ text: 'Role: Assistant Trader & Technical Analyst (Thai Language).\n\n' + marketState + '\n\nUser Question: "' + userText + '"\n\nHard Rules:\n- Answer strictly based on the Database state above (no hallucinated prices/trends).\n- Respect Top-Down: do not recommend counter-trend against the highest available Parent TF bias, unless the DB explicitly shows price at a major HTF key level + clear reversal trigger.\n- If data is missing/stale for any critical TF to answer safely, ask the user to upload that TF.\n\nOutput format:\n- Provide a short "🧠 ขั้นคิด (สรุปเป็นขั้น)" explaining how you used the DB (P1->P2->P3).\n- Then provide the final answer in Thai (concise, actionable).\n      ' }]
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

    // Keep compact to reduce token usage
    return [
      '[TF ' + x.tf + '] ' + (x.isFresh ? '🟢 Fresh' : '🔴 Stale') + ' | Age=' + x.ageMins + 'm | Updated=' + (x.timestamp_readable || '-'),
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

  const baseText = '\nRole: Expert Technical Analyst (Thai).\nTask: Create a trading plan for mode = "' + modeThai + '" using ONLY the DB context provided.\nMethodology: Strict Top-Down (Structure -> Value -> Trigger) + Confluence.\n\n' + contextStr + '\n\n*** HARD RULES (MUST FOLLOW) ***\n1) Strict Top-Down: Direction must follow Higher TF (1D/H4) first.\n2) No counter-trend trades. Exception ONLY when price is at a clearly major Support/Resistance or key Fib zone; if exception applies, you MUST label "Counter-trend (Risky)" and reduce confidence.\n3) If price is in No Man\'s Land (no clear value zone), output action = WAIT.\n4) Indicators (RSI/MACD/Stoch/MA/Volume) are CONFIRMATION only, not direction setters.\n5) Use only what exists in DB context. If missing critical TFs for safe call, set request_update_for_tf accordingly.\n\n*** MODE-SPECIFIC GUIDANCE ***\n- SCALP: prioritize precise trigger on LTF, tight invalidation, quick TP; still filter by HTF bias.\n- SWING: prioritize HTF structure/value; TP wider; trigger can be from H1/M30.\n\n*** CHAIN-OF-THOUGHT RULE ***\nThink step-by-step internally, but do NOT reveal internal chain-of-thought. Output only the JSON below.\nIn reasoning_trace, provide SHORT bullet summary derived from PRIORITY 1/2/3 (not hidden reasoning).\n\n*** OUTPUT FORMAT (JSON ONLY) ***\n{\n  "mode": "SCALP|SWING",\n  "tfs_used_for_confluence": ["1D","H4","H1","M15"],\n  "request_update_for_tf": ["1D"] | null,\n  "reasoning_trace": [\n    "P1 Structure: ...",\n    "P2 Value: ...",\n    "P3 Trigger: ..."\n  ],\n  "trade_plan": {\n    "action": "BUY|SELL|WAIT|HOLD",\n    "entry_zone": "...",\n    "target_price": "...",\n    "stop_loss": "...",\n    "confidence": "High|Medium|Low",\n    "probability_pct": 0\n  },\n  "risk_notes": [\n    "..."\n  ],\n  "user_response_text": "Generate a concise Thai response:\\n\\n⚡ **โหมด:** ' + modeThai + '\\n📢 **สถานะ:** [ACTION] (Confidence/Probability)\\n📚 **TF ที่ใช้:** ...\\n\\n🔍 **Top-Down:**\\n1️⃣ Structure: ...\\n2️⃣ Value: ...\\n3️⃣ Trigger: ...\\n\\n🎯 **Setup:**\\n- **Entry:** ...\\n- **TP:** ...\\n- **SL:** ...\\n\\n💡 **สรุป:** ...\\n⚠️ **ความเสี่ยง:** ..."\n}\n      ';

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
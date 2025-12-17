/**
 * Cloudflare Worker for LINE Bot (Top-Down Analysis Version)
 * Features: 
 * - Multi-Timeframe Technical Analysis (Top-Down Logic)
 * - Strict TF Freshness Check
 * - Interactive Data Management
 * - Database: Cloudflare D1
 * - Secure Signature Verification
 * - Agentic Q&A (DB-First)
 */

// --- CONFIGURATION ---
// MODEL_ID จะอ่านจาก env ภายในฟังก์ชัน getModelId() แทนค่าคงที่

// ระยะเวลาหมดอายุของข้อมูลแต่ละ TF (Milliseconds)
const TF_VALIDITY_MS = {
  'M1': 1 * 60 * 1000,          // 1 Minute
  'M5': 5 * 60 * 1000,          // 5 Minutes
  'M15': 15 * 60 * 1000,        // 15 Minutes
  'M30': 30 * 60 * 1000,        // 30 Minutes
  'H1': 60 * 60 * 1000,         // 1 Hour
  'H4': 4 * 60 * 60 * 1000,     // 4 Hours
  '1D': 24 * 60 * 60 * 1000,    // 1 Day
  'D1': 24 * 60 * 60 * 1000,    // Alias for 1D
  '1W': 7 * 24 * 60 * 60 * 1000 // 1 Week
};

export default {
  async fetch(request, env, ctx) {
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    // Internal endpoint for background analysis (Free-plan friendly: self-invocation)
    const url = new URL(request.url);
    const isInternal = (url.pathname === '/internal/analyze') || (url.searchParams.get('__internal') === 'analyze');
    if (isInternal) {
      return await handleInternalAnalyze(request, env, ctx);
    }

    try {
      // 1. Verify LINE Signature
      const signature = request.headers.get('x-line-signature');
      const bodyText = await request.text(); // Read raw body for verification

      if (!env.LINE_CHANNEL_SECRET) {
        console.error("Missing LINE_CHANNEL_SECRET in env");
        return new Response('Server Config Error', { status: 500 });
      }

      const isValid = await verifyLineSignature(bodyText, signature, env.LINE_CHANNEL_SECRET);
      if (!isValid) {
        console.warn("Invalid Signature");
        return new Response('Unauthorized', { status: 401 });
      }

      await initDatabase(env);

      // Parse JSON after verification
      const body = JSON.parse(bodyText);
      const events = body.events;

      if (!events || events.length === 0) {
        return new Response('OK', { status: 200 });
      }

      for (const event of events) {
        ctx.waitUntil(handleEvent(event, env, ctx, request.url));
      }

      return new Response('OK', { status: 200 });

    } catch (err) {
      console.error(safeError(err));
      return new Response('Internal Server Error', { status: 500 });
    }
  }
};

// --- HELPER: Signature Verification & Utils ---

async function verifyLineSignature(body, signature, secret) {
  if (!signature) return false;
  const encoder = new TextEncoder();
  const keyData = encoder.encode(secret);
  const key = await crypto.subtle.importKey(
    'raw', keyData, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const bodyData = encoder.encode(body);
  const signatureBuffer = await crypto.subtle.sign('HMAC', key, bodyData);
  const signatureArray = Array.from(new Uint8Array(signatureBuffer));
  const calculatedSignature = btoa(String.fromCharCode.apply(null, signatureArray));
  return calculatedSignature === signature;
}

function getModelId(env) {
  // Normalize Model ID: remove 'models/' prefix if present
  const rawId = env.MODEL_ID || 'gemma-3-27b-it';
  return rawId.replace(/^models\//, '');
}

function redactSecrets(obj) {
  if (typeof obj !== 'object' || obj === null) return obj;
  const redacted = { ...obj };
  const secrets = ['gemini_api_key', 'line_channel_access_token', 'line_channel_secret', 'authorization'];
  
  for (const key in redacted) {
    if (secrets.some(s => key.toLowerCase().includes(s))) {
      redacted[key] = '[REDACTED]';
    } else if (typeof redacted[key] === 'object') {
      redacted[key] = redactSecrets(redacted[key]);
    }
  }
  return redacted;
}

function safeError(err) {
  // Return error string without potentially sensitive stack traces if needed, or just standard logging
  // Here we just ensure it's a string and maybe add a tag
  return `[ERROR] ${err.toString()}`;
}
// --- TF HIERARCHY & CONTEXT (Smart Context) ---

const TF_ORDER = ['1W', '1D', 'H4', 'H1', 'M30', 'M15', 'M5', 'M1'];

const PARENT_TF_MAP = {
  'M1':  ['M5', 'M15', 'H1', 'H4'],
  'M5':  ['M15', 'H1', 'H4'],
  'M15': ['H1', 'H4', '1D'],
  'M30': ['H1', 'H4', '1D'],
  'H1':  ['H4', '1D'],
  'H4':  ['1D', '1W'],
  '1D':  ['1W'],
  'D1':  ['1W'], // alias
  '1W':  []
};

function normalizeTF(tf) {
  if (!tf) return null;
  const t = String(tf).trim().toUpperCase();

  // Canonicalize common aliases
  if (t === 'D1') return '1D'; // REQUIREMENT: D1/1D -> store as 1D only
  if (t === 'DAY') return '1D';
  if (t === 'WEEK') return '1W';
  if (t === 'HOUR') return 'H1';

  // Keep only known TFs if possible
  if (TF_VALIDITY_MS[t]) return t;
  return t; // fall back (still stored), but may be "Unknown_TF"
}

function inferLikelyCurrentTF(existingRows) {
  if (!Array.isArray(existingRows) || existingRows.length === 0) return null;
  const sorted = [...existingRows].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
  return normalizeTF(sorted[0]?.tf);
}

function selectSmartContextRows(validRows, likelyTf) {
  if (!Array.isArray(validRows) || validRows.length === 0) return [];
  const tfSet = new Set();

  if (likelyTf && PARENT_TF_MAP[likelyTf]) {
    for (const p of PARENT_TF_MAP[likelyTf]) tfSet.add(normalizeTF(p));
  } else {
    // Fallback: prefer HTF chain for safe Top-Down calls
    ['1D', 'H4', 'H1', 'M15'].forEach(t => tfSet.add(t));
  }

  // Only include TFs that exist in validRows
  const selected = validRows.filter(r => tfSet.has(normalizeTF(r.tf)));

  // If nothing matched, return the most recent 3 rows (least noise, still helpful)
  if (selected.length === 0) {
    return [...validRows].sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0)).slice(0, 3);
  }

  // Keep a stable order by TF hierarchy (HTF -> LTF)
  selected.sort((a, b) => TF_ORDER.indexOf(normalizeTF(a.tf)) - TF_ORDER.indexOf(normalizeTF(b.tf)));
  return selected;
}

class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TimeoutError';
  }
}

function promiseWithTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError(`Timeout after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function safeParseJsonLoosely(rawText) {
  if (!rawText) throw new Error('Empty AI response text');
  // Remove markdown fences if present
  const cleaned = String(rawText).replace(/```json/gi, '```').replace(/```/g, '').trim();
  // Attempt to extract the first top-level JSON object
  const start = cleaned.indexOf('{');
  const end = cleaned.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('No JSON object found in AI response');
  }
  const candidate = cleaned.slice(start, end + 1).trim();
  return JSON.parse(candidate);
}

// --- QUICK REPLY HELPERS (Global Cancel) ---

const CANCEL_TEXT = 'CANCEL';
const MAIN_MENU_TEXT = 'MAIN_MENU';

function ensureQuickReplyLimit(quickReply) {
  if (!quickReply || !Array.isArray(quickReply.items)) return quickReply;
  // LINE Quick Reply supports up to 13 actions
  if (quickReply.items.length <= 13) return quickReply;
  quickReply.items = quickReply.items.slice(0, 13);
  return quickReply;
}

function addCancelQuickReply(quickReply, includeCancel) {
  if (!includeCancel) return quickReply;
  if (!quickReply || !Array.isArray(quickReply.items)) return quickReply;

  const cancelItem = {
    type: "action",
    action: { type: "message", label: "ยกเลิก", text: CANCEL_TEXT }
  };

  // Avoid duplicates
  const hasCancel = quickReply.items.some(it => it?.action?.text === CANCEL_TEXT);
  if (hasCancel) return quickReply;

  if (quickReply.items.length >= 13) {
    // Replace last item to respect LINE limit
    quickReply.items[12] = cancelItem;
  } else {
    quickReply.items.push(cancelItem);
  }
  return quickReply;
}

function normalizeQuickReply(quickReply) {
  // Cancel must appear in every menu EXCEPT mainMenu
  const includeCancel = Boolean(quickReply) && quickReply !== mainMenu;
  const q = quickReply ? JSON.parse(JSON.stringify(quickReply)) : null; // clone to avoid side effects
  const withCancel = addCancelQuickReply(q, includeCancel);
  return ensureQuickReplyLimit(withCancel);
}


// --- MENUS ---
const mainMenu = {
  items: [
    {
      type: "action",
      action: {
        type: "message",
        label: "📊 สถานะข้อมูล",
        text: "STATUS"
      }
    },
    {
      type: "action",
      action: {
        type: "message",
        label: "📌 สรุปผลวิเคราะห์",
        text: "SUMMARY"
      }
    },
    {
      type: "action",
      action: {
        type: "message",
        label: "⚡ เล่นสั้น/สวิง",
        text: "TRADE_STYLE"
      }
    },
    {
      type: "action",
      action: {
        type: "message",
        label: "🔧 แก้ไข/ลบ ข้อมูล",
        text: "MANAGE_DATA"
      }
    }
  ]
};



const tradeStyleMenu = {
  items: [
    {
      type: "action",
      action: { type: "message", label: "⚡ เล่นสั้น (Scalp)", text: "TRADE_STYLE:SCALP" }
    },
    {
      type: "action",
      action: { type: "message", label: "🌊 เล่นสวิง (Swing)", text: "TRADE_STYLE:SWING" }
    },
    {
      type: "action",
      action: { type: "message", label: "⬅️ กลับเมนูหลัก", text: MAIN_MENU_TEXT }
    }
  ]
};

// --- EVENT HANDLER ---
async function handleEvent(event, env, ctx, requestUrl) {
  if (event.type !== 'message') return;

  const replyToken = event.replyToken;
  const messageType = event.message.type;
  const messageId = event.message.id;
  const userId = event.source.userId;

  try {
    // 1. Text Message Handling
    if (messageType === 'text') {
      const userText = event.message.text.trim();

      // --- GLOBAL: CANCEL / MAIN MENU ---
      if (userText === CANCEL_TEXT) {
        await replyText(replyToken, "✅ ยกเลิกแล้วครับ", env, mainMenu);
        return;
      }
      if (userText === MAIN_MENU_TEXT) {
        await replyText(replyToken, "📌 เมนูหลัก", env, mainMenu);
        return;
      }

      // --- MENU: STATUS ---
      if (userText === 'STATUS') {
        await handleStatusRequest(userId, replyToken, env);
        return;
      }

      // --- MENU: SUMMARY ---
      if (userText === 'SUMMARY') {
        await handleSummaryMenuRequest(userId, replyToken, env);
        return;
      }
      // --- MENU: TRADE STYLE (SCALP / SWING) ---
      if (userText === 'TRADE_STYLE') {
        await handleTradeStyleMenuRequest(userId, replyToken, env);
        return;
      }

      // --- MENU: TRADE STYLE (RUN) ---
      if (userText.startsWith('TRADE_STYLE:')) {
        const mode = (userText.split(':')[1] || '').trim().toUpperCase();
        await handleTradeStyleAnalysisRequest(userId, mode, replyToken, env);
        return;
      }


      // --- MENU: SUMMARY (SELECT TF) ---
      if (userText.startsWith('SUMMARY_TF:')) {
        const targetTF = normalizeTF(userText.split(':')[1]);
        await handleSummaryTFRequest(userId, targetTF, replyToken, env);
        return;
      }

      // --- MENU: MANAGE_DATA ---
      if (userText === 'MANAGE_DATA') {
        await handleManageDataRequest(userId, replyToken, env);
        return;
      }

      // --- COMMAND: SELECT ITEM TO EDIT ---
      if (userText.startsWith('EDIT_SEL:')) {
        const targetTF = normalizeTF(userText.split(':')[1]);
        await handleEditSelection(userId, targetTF, replyToken, env);
        return;
      }

      // --- COMMAND: DELETE ITEM ---
      if (userText.startsWith('DEL_EXEC:')) {
        const targetTF = normalizeTF(userText.split(':')[1]);
        await deleteAnalysis(userId, targetTF, env);
        await replyText(replyToken, `🗑️ ลบข้อมูล TF: ${targetTF} เรียบร้อยแล้วครับ`, env, mainMenu);
        return;
      }

      // --- COMMAND: CHANGE TF ---
      if (userText.startsWith('CHANGE_TF:')) {
        const parts = userText.split(':');
        const oldTF = normalizeTF(parts[1]);
        const newTF = normalizeTF(parts[3]);
        await updateAnalysisTF(userId, oldTF, newTF, env);
        await replyText(replyToken, `✅ แก้ไข TF จาก ${oldTF} เป็น ${newTF} เรียบร้อยแล้วครับ`, env, mainMenu);
        return;
      }

      // Chat with Context (AI Chat) - DB First Logic
      const aiResponse = await chatWithGeminiText(userId, userText, env);
      await replyText(replyToken, aiResponse, env, mainMenu);
      return;
    }

    // 2. Image Handling (Analysis)
    if (messageType === 'image') {

      // Enqueue this image for FIFO processing (allows users to send multiple images continuously)
      const { jobId, createdAt } = await enqueueAnalysisJob(userId, messageId, env);

      // If there is already a queue/processing job, acknowledge immediately and let background worker process in order
      const qStats = await getUserQueueStats(userId, env);
      const canFastPath = (qStats.processing_count === 0 && qStats.queued_count === 1);

      if (!canFastPath) {
        const ackMsg = await buildQueueAckMessage(userId, jobId, createdAt, env);

        await replyText(replyToken, ackMsg, env, mainMenu);

        await triggerInternalAnalyze(userId, requestUrl, env);
        return;
      }

      // This is the only job in queue -> try FAST-PATH (use replyToken) while also marking job as processing
      await env.DB.prepare(`UPDATE analysis_jobs SET status='processing', started_at=? WHERE job_id=? AND status='queued'`)
        .bind(Date.now(), jobId).run();
      const { arrayBuffer: imageBinary, contentType } = await getContentFromLine(messageId, env);
      const base64Image = arrayBufferToBase64(imageBinary);

      // Load ALL previous analyses
      const existingRows = await getAllAnalyses(userId, env);

      // FAST-PATH: try to finish analysis within a short deadline (use replyToken as usual)
      const FAST_DEADLINE_MS = Number(env.FAST_ANALYSIS_DEADLINE_MS || 18000);
      const controller = new AbortController();

      let analysisResult = null;
      try {
        analysisResult = await promiseWithTimeout(
          analyzeChartStructured(userId, base64Image, existingRows, env, { mimeType: contentType, signal: controller.signal }),
          FAST_DEADLINE_MS
        );
      } catch (e) {
        // If we're running out of time, ACK first, then continue in background via internal endpoint
        controller.abort();

        // Re-queue current job so the internal FIFO processor can continue
        try { await requeueJob(jobId, env, 1, 'Foreground timeout -> background'); } catch (_) {}
        const ackMsg = await buildQueueAckMessage(userId, jobId, createdAt, env);

        await replyText(replyToken, ackMsg, env, mainMenu);

        // Free-plan friendly background continuation: self-invocation internal endpoint
        await triggerInternalAnalyze(userId, requestUrl, env);
        return;
      }

      // Handle "Stale Data" Request
      if (analysisResult?.request_update_for_tf && analysisResult.request_update_for_tf.length > 0) {
        const tfList = Array.isArray(analysisResult.request_update_for_tf)
          ? analysisResult.request_update_for_tf.join(', ')
          : analysisResult.request_update_for_tf;
        const warningMsg = `⚠️ **ข้อมูลไม่เพียงพอสำหรับ Top-Down Analysis**

ระบบต้องการภาพ TF: **${tfList}** เพิ่มเติมเพื่อยืนยันแนวโน้มหลัก (Big Picture)

📸 กรุณาส่งภาพกราฟ **${tfList}** เข้ามาครับ`;
        await replyText(replyToken, warningMsg, env, mainMenu);
        return;
      // Close job to avoid blocking the queue
      try { await markJobDone(jobId, env, 'REQUEST_UPDATE'); } catch (_) {}

      }

      const detectedTF = normalizeTF(analysisResult?.detected_tf || "Unknown_TF");
      const now = new Date();
      const readableTime = now.toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });

      // Build rich analysis JSON for DB (Requirement: store detailed info, not only setup)
      const detailed = analysisResult?.detailed_technical_data || {};
      const toStore = {
        detected_tf: detectedTF,
        tfs_used_for_confluence: analysisResult?.tfs_used_for_confluence || [],
        request_update_for_tf: analysisResult?.request_update_for_tf || null,
        trend_bias: detailed.trend_bias || detailed?.structure?.trend_bias || 'Unknown',
        trade_setup: detailed.trade_setup || detailed?.setup || {},
        reasoning_trace: analysisResult?.reasoning_trace || detailed?.reasoning_trace || [],
        structure: detailed.structure || detailed?.priority_1_structure || {},
        value: detailed.value || detailed?.priority_2_value || {},
        trigger: detailed.trigger || detailed?.priority_3_trigger || {},
        indicators: detailed.indicators || {},
        patterns: detailed.patterns || [],
        key_levels: detailed.key_levels || {},
        raw_extraction: detailed.raw_extraction || {},
        notes: detailed.notes || null
      };

      // Save Data (Requirement: D1/1D must be stored as 1D only)
      await saveAnalysis(userId, detectedTF, Date.now(), readableTime, toStore, env);

      await replyText(replyToken, analysisResult.user_response_text || "✅ บันทึกผลวิเคราะห์เรียบร้อยแล้วครับ", env, mainMenu);
    
      // If there are more jobs queued (user sent multiple images), continue background processing
      try {
        const st = await getUserQueueStats(userId, env);
        if (st.queued_count > 0) await triggerInternalAnalyze(userId, requestUrl, env);
      } catch (_) {}
}

  } catch (error) {
    console.error(safeError(error));
    await replyText(replyToken, `⚠️ System Error:
${error.message}`, env, mainMenu);
  }
}


// --- LOGIC: MANAGE DATA (Interactive Menu) ---

async function handleManageDataRequest(userId, replyToken, env) {
  const rows = await getAllAnalyses(userId, env);
  
  if (!rows || rows.length === 0) {
    await replyText(replyToken, "❌ ไม่พบข้อมูลกราฟที่บันทึกไว้", env, mainMenu);
    return;
  }

  // Sort by latest
  rows.sort((a, b) => b.timestamp - a.timestamp);

  let msg = "🔧 **จัดการข้อมูลที่บันทึกไว้**\nเลือกหมายเลขเพื่อแก้ไขหรือลบ:\n";
  const quickReplyItems = [];

  rows.forEach((row, index) => {
    const num = index + 1;
    const timeDiffMins = Math.floor((Date.now() - row.timestamp) / 60000);
    const ageText = timeDiffMins > 60 ? `${(timeDiffMins/60).toFixed(1)} ชม.` : `${timeDiffMins} นาที`;
    
    msg += `\n${num}. TF: **${row.tf}** (อัพเดท ${ageText} ที่แล้ว)`;
    
    quickReplyItems.push({
      type: "action",
      action: {
        type: "message",
        label: `เลือกรายการที่ ${num} (${row.tf})`,
        text: `EDIT_SEL:${row.tf}`
      }
    });
  });

  msg += "\n\n(กดปุ่มด้านล่างเพื่อเลือกรายการ)";
  await replyText(replyToken, msg, env, { items: quickReplyItems });
}

async function handleEditSelection(userId, targetTF, replyToken, env) {
  const msg = `⚙️ **กำลังจัดการข้อมูล TF: ${targetTF}**

ท่านต้องการทำรายการใด?`;
  const allTFs = [...new Set(Object.keys(TF_VALIDITY_MS)
    .map(normalizeTF)
    .filter(Boolean)
    .filter(tf => tf !== targetTF))];

  // Prefer a stable TF order for readability
  allTFs.sort((a, b) => TF_ORDER.indexOf(a) - TF_ORDER.indexOf(b));

  const quickReplyItems = [];

  // 1) Delete
  quickReplyItems.push({
    type: "action",
    action: {
      type: "message",
      label: "🗑️ ลบข้อมูลนี้",
      text: `DEL_EXEC:${targetTF}`
    }
  });

  // 2) Change TF (cap items to respect LINE Quick Reply limit; Cancel will be auto-added)
  const MAX_CHANGE_ITEMS = 11;
  allTFs.slice(0, MAX_CHANGE_ITEMS).forEach(tf => {
    quickReplyItems.push({
      type: "action",
      action: {
        type: "message",
        label: `เปลี่ยนเป็น ${tf}`,
        text: `CHANGE_TF:${targetTF}:TO:${tf}`
      }
    });
  });

  await replyText(replyToken, msg, env, { items: quickReplyItems });
}

// --- LOGIC: CORE ANALYSIS (UPDATED FOR TOP-DOWN) ---

async function analyzeChartStructured(userId, base64Image, existingRows, env, options = {}) {
  const modelId = getModelId(env);
  const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${modelId}:generateContent?key=${env.GEMINI_API_KEY}`;
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
    existingContextStr += `Context selection based on last-updated TF: ${likelyTf || 'Unknown'}
`;
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
      }
    `}]
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
    throw new Error(`AI API Error: ${response.status} ${errText ? ('- ' + errText.slice(0, 200)) : ''}`);
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

async function chatWithGeminiText(userId, userText, env) {
  const modelId = getModelId(env);
  const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${modelId}:generateContent?key=${env.GEMINI_API_KEY}`;

  // 1) Detect requested TF (if any)
  const tfRegex = /(M1|M5|M15|M30|H1|H4|1D|D1|1W|WEEK|DAY|HOUR)/i;
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
      return `❌ ไม่พบข้อมูล ${targetTF} ที่เป็นปัจจุบัน

📸 กรุณาส่งรูปกราฟ Timeframe **${targetTF}** เข้ามาก่อน เพื่อให้ผมวิเคราะห์ต่อได้ครับ`;
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
        trigger?.divergence ? `Div=${trigger.divergence}` : null,
        Array.isArray(trigger?.candlestick_patterns) && trigger.candlestick_patterns.length ? `Patterns=${trigger.candlestick_patterns.join(',')}` : null
      ].filter(Boolean).join(' | ') || '-';

      return [
        `- TF ${x.tf} (${freshness}, Age=${x.ageMins}m, Updated=${x.timestamp_readable || '-'})`,
        `  Trend=${d.trend_bias || structure?.parent_bias || 'Unknown'} | Action=${setup?.action || 'N/A'}`,
        `  Entry=${setup?.entry_zone || '-'} | TP=${setup?.target_price || '-'} | SL=${setup?.stop_loss || '-'}`,
        `  P1(Structure)=${structure?.market_structure || '-'} | P2(Value)=${keyLevels} | P3(Trigger)=${trig}`
      ].join('\n');
    }).join('\n');

    marketState = `=== CURRENT MARKET STATE (Database: ALL TFs) ===
${lines}
===============================================`;
  } else {
    marketState = `=== CURRENT MARKET STATE ===
No technical data available in database.
User must upload charts first.
================================`;
}

  // 6) LLM Response Generation (Hard Rules + DB-first)
  const payload = {
    contents: [{
      role: "user",
      parts: [{ text: `
        Role: Assistant Trader & Technical Analyst (Thai Language).

        ${marketState}

        User Question: "${userText}"

        Hard Rules:
        - Answer strictly based on the Database state above (no hallucinated prices/trends).
        - Respect Top-Down: do not recommend counter-trend against the highest available Parent TF bias, unless the DB explicitly shows price at a major HTF key level + clear reversal trigger.
        - If data is missing/stale for any critical TF to answer safely, ask the user to upload that TF.

        Output format:
        - Provide a short "🧠 ขั้นคิด (สรุปเป็นขั้น)" explaining how you used the DB (P1->P2->P3).
        - Then provide the final answer in Thai (concise, actionable).
      ` }]
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

// --- DATABASE FUNCTIONS (D1) ---

async function initDatabase(env) {
  if (!env.DB) return;
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS user_analysis_logs (
      user_id TEXT,
      tf TEXT,
      timestamp INTEGER,
      timestamp_readable TEXT,
      analysis_json TEXT,
      PRIMARY KEY (user_id, tf)
    )
  `).run();


// FIFO queue for sequential image analysis (per-user)
await env.DB.prepare(`
  CREATE TABLE IF NOT EXISTS analysis_jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    message_id TEXT,
    created_at INTEGER,
    status TEXT,
    attempt INTEGER,
    started_at INTEGER,
    finished_at INTEGER,
    result_tf TEXT,
    last_error TEXT
  )
`).run();

// Helpful index for fetching per-user queue quickly (SQLite/D1 supports CREATE INDEX IF NOT EXISTS)
await env.DB.prepare(`
  CREATE INDEX IF NOT EXISTS idx_analysis_jobs_user_status_created
  ON analysis_jobs(user_id, status, created_at)
`).run();

}


async function getAllAnalyses(userId, env) {
  if (!env.DB) throw new Error("No DB");
  const stmt = env.DB.prepare("SELECT * FROM user_analysis_logs WHERE user_id = ?");
  const { results } = await stmt.bind(userId).all();
  return results || [];
}

async function saveAnalysis(userId, tf, timestamp, timestampReadable, dataObj, env) {
  if (!env.DB) throw new Error("No DB");
  const jsonStr = JSON.stringify(dataObj);
  const stmt = env.DB.prepare(`
    INSERT INTO user_analysis_logs (user_id, tf, timestamp, timestamp_readable, analysis_json)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(user_id, tf) DO UPDATE SET
      timestamp = excluded.timestamp,
      timestamp_readable = excluded.timestamp_readable,
      analysis_json = excluded.analysis_json
  `);
  await stmt.bind(userId, tf, timestamp, timestampReadable, jsonStr).run();
}

async function deleteAnalysis(userId, tf, env) {
  if (!env.DB) throw new Error("No DB");
  await env.DB.prepare("DELETE FROM user_analysis_logs WHERE user_id = ? AND tf = ?")
    .bind(userId, tf).run();
}

async function updateAnalysisTF(userId, oldTF, newTF, env) {
  if (!env.DB) throw new Error("No DB");
  
  const stmtGet = env.DB.prepare("SELECT * FROM user_analysis_logs WHERE user_id = ? AND tf = ?");
  const oldRow = await stmtGet.bind(userId, oldTF).first();
  
  if (!oldRow) return;

  await saveAnalysis(userId, newTF, oldRow.timestamp, oldRow.timestamp_readable, JSON.parse(oldRow.analysis_json), env);
  await deleteAnalysis(userId, oldTF, env);
}

// --- JOB QUEUE (D1) : FIFO sequential image processing ---

function makeJobId(userId, messageId) {
  // Stable-enough unique id without crypto UUID
  return `${Date.now()}_${userId.slice(-6)}_${messageId}`;
}

async function enqueueAnalysisJob(userId, messageId, env) {
  if (!env.DB) throw new Error("No DB");
  const jobId = makeJobId(userId, messageId);
  const createdAt = Date.now();
  await env.DB.prepare(`
    INSERT OR IGNORE INTO analysis_jobs
      (job_id, user_id, message_id, created_at, status, attempt, started_at, finished_at, result_tf, last_error)
    VALUES (?, ?, ?, ?, 'queued', 0, NULL, NULL, NULL, NULL)
  `).bind(jobId, userId, messageId, createdAt).run();
  return { jobId, createdAt };
}

async function getUserQueueStats(userId, env) {
  if (!env.DB) throw new Error("No DB");
  const queued = await env.DB.prepare(`SELECT COUNT(*) AS c FROM analysis_jobs WHERE user_id = ? AND status = 'queued'`)
    .bind(userId).first();
  const processing = await env.DB.prepare(`SELECT COUNT(*) AS c FROM analysis_jobs WHERE user_id = ? AND status = 'processing'`)
    .bind(userId).first();
  return {
    queued_count: Number(queued?.c || 0),
    processing_count: Number(processing?.c || 0)
  };
}

function formatDurationTH(seconds) {
  const s = Math.max(0, Math.round(Number(seconds || 0)));
  if (s < 60) return `${s} วินาที`;
  const mins = Math.round(s / 60);
  if (mins < 60) return `${mins} นาที`;
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  return remMins ? `${hours} ชม. ${remMins} นาที` : `${hours} ชม.`;
}

function shortMessageId(messageId) {
  if (!messageId) return "??????";
  const s = String(messageId);
  return s.length <= 6 ? s : s.slice(-6);
}

async function estimateSecondsPerImage(userId, env) {
  const fallback = Math.max(10, Number(env.EST_SECONDS_PER_IMAGE || 45));
  if (!env.DB) return fallback;

  try {
    const { results } = await env.DB.prepare(`
      SELECT (finished_at - started_at) AS dur_ms
      FROM analysis_jobs
      WHERE user_id = ?
        AND status = 'done'
        AND started_at IS NOT NULL
        AND finished_at IS NOT NULL
        AND finished_at >= started_at
      ORDER BY finished_at DESC
      LIMIT 5
    `).bind(userId).all();

    const durations = (results || [])
      .map(r => Number(r?.dur_ms || 0))
      .filter(n => Number.isFinite(n) && n > 0);

    if (durations.length === 0) return fallback;

    const avgMs = durations.reduce((a, b) => a + b, 0) / durations.length;
    const avgSec = avgMs / 1000;

    // clamp for stability
    return Math.min(180, Math.max(15, avgSec));
  } catch (_) {
    return fallback;
  }
}

async function getQueueProgressForAck(userId, jobId, createdAt, env) {
  const stats = await getUserQueueStats(userId, env);

  const totalRow = await env.DB.prepare(`
    SELECT COUNT(*) AS c
    FROM analysis_jobs
    WHERE user_id = ? AND status IN ('queued','processing')
  `).bind(userId).first();

  const totalPending = Number(totalRow?.c || 0);

  const processing = await env.DB.prepare(`
    SELECT job_id, message_id, created_at, started_at
    FROM analysis_jobs
    WHERE user_id = ? AND status = 'processing'
    ORDER BY started_at ASC
    LIMIT 1
  `).bind(userId).first();

  let processingOrder = null;
  if (processing?.created_at != null) {
    const ord = await env.DB.prepare(`
      SELECT COUNT(*) AS c
      FROM analysis_jobs
      WHERE user_id = ?
        AND status IN ('queued','processing')
        AND created_at <= ?
    `).bind(userId, processing.created_at).first();
    processingOrder = Number(ord?.c || 1);
  }

  // your position in the combined pending list (processing + queued), ordered by created_at
  let yourPosition = null;
  try {
    let ca = createdAt;
    if (ca == null && jobId) {
      const r = await env.DB.prepare(`SELECT created_at FROM analysis_jobs WHERE job_id = ?`).bind(jobId).first();
      ca = r?.created_at ?? null;
    }
    if (ca != null) {
      const pos = await env.DB.prepare(`
        SELECT COUNT(*) AS c
        FROM analysis_jobs
        WHERE user_id = ?
          AND status IN ('queued','processing')
          AND created_at <= ?
      `).bind(userId, ca).first();
      yourPosition = Number(pos?.c || 0);
    }
  } catch (_) {
    yourPosition = null;
  }

  return {
    queued_count: stats.queued_count,
    processing_count: stats.processing_count,
    totalPending,
    processing,
    processingOrder,
    yourPosition
  };
}

async function buildQueueAckMessage(userId, jobId, createdAt, env) {
  const perImageSec = await estimateSecondsPerImage(userId, env);
  const q = await getQueueProgressForAck(userId, jobId, createdAt, env);

  const total = q.totalPending || (q.queued_count + q.processing_count) || 1;
  const yourPos = q.yourPosition || null;

  const lines = [];
  lines.push("✅ ได้รับรูปแล้วครับ");
  lines.push("");
  lines.push(`📥 คิว: รอ ${q.queued_count} รูป (รวมรูปนี้) | กำลังประมวลผล ${q.processing_count} รูป`);

  if (q.processing?.message_id) {
    lines.push(`⚙️ กำลังทำ: รูปที่ ${(q.processingOrder || 1)}/${total} (ID …${shortMessageId(q.processing.message_id)})`);
  } else {
    lines.push(`⚙️ กำลังทำ: ยังไม่มีงานที่กำลังประมวลผล (กำลังเริ่มคิว)`);
  }

  lines.push(`🧮 ประมาณการ: ~${formatDurationTH(perImageSec)}/รูป`);

  if (yourPos) {
    const etaStart = Math.max(0, (yourPos - 1) * perImageSec);
    const etaDone = Math.max(0, yourPos * perImageSec);
    lines.push(`📌 รูปนี้อยู่ลำดับที่ ${yourPos}/${Math.max(total, yourPos)}`);
    lines.push(`⏱️ คาดเริ่ม ~${formatDurationTH(etaStart)} | เสร็จ ~${formatDurationTH(etaDone)}`);
  }

  lines.push("");
  lines.push("📌 เข้าเมนู **สรุปผลวิเคราะห์** เพื่อดูผลลัพธ์ครับ");

  return lines.join("\n");
}

async function claimNextQueuedJob(userId, env) {
  if (!env.DB) throw new Error("No DB");

  // Enforce single processing job per-user
  const proc = await env.DB.prepare(`SELECT COUNT(*) AS c FROM analysis_jobs WHERE user_id = ? AND status = 'processing'`)
    .bind(userId).first();
  if (Number(proc?.c || 0) > 0) return null;

  const next = await env.DB.prepare(`
    SELECT job_id, message_id, created_at, attempt
    FROM analysis_jobs
    WHERE user_id = ? AND status = 'queued'
    ORDER BY created_at ASC
    LIMIT 1
  `).bind(userId).first();

  if (!next?.job_id) return null;

  const startedAt = Date.now();
  const res = await env.DB.prepare(`
    UPDATE analysis_jobs
    SET status = 'processing', started_at = ?, last_error = NULL
    WHERE job_id = ? AND status = 'queued'
  `).bind(startedAt, next.job_id).run();

  // If update did not apply (race), return null
  // D1 meta may not always include changes; be defensive:
  if (res?.meta?.changes === 0) return null;

  return { 
    job_id: next.job_id, 
    message_id: next.message_id, 
    created_at: next.created_at, 
    attempt: Number(next.attempt || 0),
    started_at: startedAt
  };
}

async function requeueJob(jobId, env, attempt, lastError) {
  if (!env.DB) throw new Error("No DB");
  await env.DB.prepare(`
    UPDATE analysis_jobs
    SET status = 'queued',
        attempt = ?,
        started_at = NULL,
        last_error = ?
    WHERE job_id = ?
  `).bind(attempt, lastError ? String(lastError).slice(0, 800) : null, jobId).run();
}

async function pruneDoneJobHistory(userId, env, keepCount = 5) {
  if (!env.DB) return;
  const keep = Math.max(1, Math.floor(Number(keepCount || 5)));

  // Keep only the most recent N completed jobs (status='done') per user.
  // This keeps DB small and stabilizes ETA estimation.
  try {
    const sql = `
      DELETE FROM analysis_jobs
      WHERE user_id = ?
        AND status = 'done'
        AND job_id NOT IN (
          SELECT job_id FROM analysis_jobs
          WHERE user_id = ?
            AND status = 'done'
          ORDER BY finished_at DESC
          LIMIT ${keep}
        )
    `;
    await env.DB.prepare(sql).bind(userId, userId).run();
  } catch (e) {
    console.warn('[pruneDoneJobHistory] ' + safeError(e));
  }
}

async function markJobDone(jobId, env, resultTF) {
  if (!env.DB) throw new Error("No DB");
  const finishedAt = Date.now();
  await env.DB.prepare(`
    UPDATE analysis_jobs
    SET status = 'done',
        finished_at = ?,
        result_tf = ?
    WHERE job_id = ?
  `).bind(finishedAt, resultTF || null, jobId).run();

  // Prune completed job history: keep only the latest 5 'done' jobs for ETA averaging.
  try {
    const row = await env.DB.prepare(`SELECT user_id FROM analysis_jobs WHERE job_id = ?`).bind(jobId).first();
    if (row?.user_id) {
      await pruneDoneJobHistory(row.user_id, env, 5);
    }
  } catch (e) {
    console.warn('[markJobDone prune] ' + safeError(e));
  }
}

async function markJobError(jobId, env, errMsg) {
  if (!env.DB) throw new Error("No DB");
  const finishedAt = Date.now();
  await env.DB.prepare(`
    UPDATE analysis_jobs
    SET status = 'error',
        finished_at = ?,
        last_error = ?
    WHERE job_id = ?
  `).bind(finishedAt, String(errMsg || '').slice(0, 800), jobId).run();
}

async function hasQueuedJobs(userId, env) {
  if (!env.DB) throw new Error("No DB");
  const row = await env.DB.prepare(`SELECT COUNT(*) AS c FROM analysis_jobs WHERE user_id = ? AND status = 'queued'`)
    .bind(userId).first();
  return Number(row?.c || 0) > 0;
}

// --- UTILS ---

async function handleStatusRequest(userId, replyToken, env) {
  const rows = await getAllAnalyses(userId, env);
  let msg = "✅ สถานะข้อมูลกราฟในระบบ (กรองตามอายุ):";
  
  if (rows && rows.length > 0) {
    rows.sort((a, b) => b.timestamp - a.timestamp);

    for (const row of rows) {
       const data = JSON.parse(row.analysis_json || '{}');
       const ageMs = Date.now() - row.timestamp;
       const limitMs = TF_VALIDITY_MS[normalizeTF(row.tf)];
       
       let statusIcon = "🟢"; 
       let statusText = "ใช้งานได้";
       
       if (limitMs && ageMs > limitMs) {
         statusIcon = "🔴";
         statusText = "หมดอายุ";
       }

       const ageMins = (ageMs / 60000).toFixed(0);
       msg += `\n\n${statusIcon} **TF: ${row.tf}**`;
       msg += `\n🕒 อายุ: ${ageMins} นาที (${statusText})`;
       if (statusIcon === "🟢") {
          msg += `\n📈 เทรนด์: ${data.trend_bias || '-'}`;
       }
    }
  } else {
    msg += "\n(ยังไม่มีข้อมูลกราฟในระบบ)";
  }
  await replyText(replyToken, msg, env, mainMenu);
}

async function getContentFromLine(messageId, env) {
  const url = `https://api-data.line.me/v2/bot/message/${messageId}/content`;
  const response = await fetch(url, {
    method: 'GET',
    headers: { 'Authorization': `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}` }
  });
  if (!response.ok) throw new Error(`LINE Error: ${response.status}`);

  const contentType = response.headers.get('content-type') || 'image/jpeg';
  const arrayBuffer = await response.arrayBuffer();
  return { arrayBuffer, contentType };
}

async function replyText(replyToken, text, env, quickReply = null) {
  const body = {
    replyToken: replyToken,
    messages: [{ type: 'text', text: text }]
  };

  const normalized = normalizeQuickReply(quickReply);
  if (normalized) body.messages[0].quickReply = normalized;

  await fetch('https://api.line.me/v2/bot/message/reply', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${env.LINE_CHANNEL_ACCESS_TOKEN}`
    },
    body: JSON.stringify(body)
  });
}

// --- INTERNAL BACKGROUND ANALYSIS (Free-plan friendly) ---

async function triggerInternalAnalyze(userId, requestUrl, env) {
  try {
    // Query-param mode ensures it hits the same Worker route (even if mounted on a sub-path).
    const u = new URL(requestUrl);
    u.search = '';
    u.searchParams.set('__internal', 'analyze');

    const headers = { 'Content-Type': 'application/json' };
    // Optional shared secret (recommended)
    if (env.INTERNAL_TASK_TOKEN) {
      headers['X-Internal-Task-Token'] = env.INTERNAL_TASK_TOKEN;
    }

    const res = await fetch(u.toString(), {
      method: 'POST',
      headers,
      body: JSON.stringify({ userId })
    });

    // Helpful for debugging: if internal call fails, you will see it in logs.
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      console.error(`Internal analyze trigger failed: HTTP ${res.status} ${t ? ('- ' + t.slice(0, 120)) : ''}`);
    }
  } catch (e) {
    console.error("Internal analyze trigger failed:", safeError(e));
  }
}

async function handleInternalAnalyze(request, env, ctx) {
  // Optional protection
  if (env.INTERNAL_TASK_TOKEN) {
    const token = request.headers.get('x-internal-task-token');
    if (!token || token !== env.INTERNAL_TASK_TOKEN) {
      return new Response('Unauthorized', { status: 401 });
    }
  }

  // Ensure DB schema exists for this request too
  await initDatabase(env);

  let body = null;
  try {
    body = await request.json();
  } catch (_) {
    return new Response('Bad Request', { status: 400 });
  }

  const userId = body?.userId;
  if (!userId) return new Response('Bad Request', { status: 400 });

  // Claim exactly 1 job per invocation (Free-plan friendly)
  const job = await claimNextQueuedJob(userId, env);

  if (!job) {
    // Nothing to do (or already processing)
    try {
      const idleAt = Date.now();
      const idleReadable = new Date(idleAt).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
      const stats = await getUserQueueStats(userId, env);
      await saveAnalysis(userId, '_JOB', idleAt, idleReadable, {
        status: (stats.processing_count > 0 ? 'busy' : 'idle'),
        queued: stats.queued_count,
        processing: stats.processing_count,
        idleAt,
        idleReadable
      }, env);
    } catch (_) {}
    return new Response('OK', { status: 200 });
  }

  const internalTimeoutMs = Math.max(8000, Number(env.INTERNAL_AI_TIMEOUT_MS || 24000));
  const maxAttempts = Math.max(1, Number(env.INTERNAL_MAX_RETRY || 3));
  const attempt = Number(job.attempt || 0);

  const startedReadable = new Date(job.started_at).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });

  // Write job marker for visibility (single row per-user)
  try {
    const stats = await getUserQueueStats(userId, env);
    await saveAnalysis(userId, '_JOB', job.started_at, startedReadable, {
      status: 'processing',
      jobId: job.job_id,
      messageId: job.message_id,
      attempt,
      maxAttempts,
      internalTimeoutMs,
      queued_after_claim: stats.queued_count,
      startedAt: job.started_at,
      startedReadable
    }, env);
  } catch (e) {
    console.error("Failed to write _JOB marker:", safeError(e));
  }

  ctx.waitUntil((async () => {
    try {
      const { arrayBuffer: imageBinary, contentType } = await getContentFromLine(job.message_id, env);
      const base64Image = arrayBufferToBase64(imageBinary);

      const existingRows = (await getAllAnalyses(userId, env)).filter(r => !String(r.tf || '').startsWith('_'));
      const controller = new AbortController();

      let analysisResult;
      try {
        analysisResult = await promiseWithTimeout(
          analyzeChartStructured(userId, base64Image, existingRows, env, { mimeType: contentType, signal: controller.signal }),
          internalTimeoutMs
        );
      } catch (err) {
        // retryable?
        const msg = String(err?.message || err);
        const retryable =
          (err && (err.name === 'TimeoutError' || err.name === 'AbortError')) ||
          msg.includes('429') || msg.includes('503') || msg.includes('500');

        const nextAttempt = attempt + 1;
        if (retryable && nextAttempt <= maxAttempts) {
          await requeueJob(job.job_id, env, nextAttempt, msg);

          const retryAt = Date.now();
          const retryReadable = new Date(retryAt).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
          await saveAnalysis(userId, '_JOB', retryAt, retryReadable, {
            status: 'retrying',
            jobId: job.job_id,
            messageId: job.message_id,
            attempt: nextAttempt,
            maxAttempts,
            lastError: msg,
            retryAt,
            retryReadable
          }, env);

          await triggerInternalAnalyze(userId, request.url, env);
          return;
        }

        // non-retryable / exceeded attempts
        await markJobError(job.job_id, env, msg);
        const errAt = Date.now();
        const errReadable = new Date(errAt).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
        await saveAnalysis(userId, '_JOB', errAt, errReadable, {
          status: 'error',
          jobId: job.job_id,
          messageId: job.message_id,
          attempt: nextAttempt,
          maxAttempts,
          error: msg,
          errAt,
          errReadable
        }, env);

        // continue to next queued job if any
        if (await hasQueuedJobs(userId, env)) {
          await triggerInternalAnalyze(userId, request.url, env);
        }
        return;
      } finally {
        try { controller.abort(); } catch (_) {}
      }

      // Normalize + store rich analysis (same as foreground path)
      const detectedTF = normalizeTF(analysisResult?.detected_tf || "Unknown_TF");
      const readableTime = new Date().toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });

      const detailed = analysisResult?.detailed_technical_data || {};
      const toStore = {
        detected_tf: detectedTF,
        tfs_used_for_confluence: analysisResult?.tfs_used_for_confluence || [],
        request_update_for_tf: analysisResult?.request_update_for_tf || null,
        trend_bias: detailed.trend_bias || detailed?.structure?.trend_bias || 'Unknown',
        trade_setup: detailed.trade_setup || detailed?.setup || {},
        reasoning_trace: analysisResult?.reasoning_trace || detailed?.reasoning_trace || [],
        structure: detailed.structure || detailed?.priority_1_structure || {},
        value: detailed.value || detailed?.priority_2_value || {},
        trigger: detailed.trigger || detailed?.priority_3_trigger || {},
        indicators: detailed.indicators || {},
        patterns: detailed.patterns || [],
        key_levels: detailed.key_levels || {},
        raw_extraction: detailed.raw_extraction || {},
        notes: detailed.notes || null
      };

      // If the model requests TF update, keep it as WAIT + include a clear trace (so user sees it via Summary)
      if (Array.isArray(analysisResult?.request_update_for_tf) && analysisResult.request_update_for_tf.length > 0) {
        toStore.trade_setup = { ...(toStore.trade_setup || {}), action: 'WAIT', confidence: 'Low' };
        const need = analysisResult.request_update_for_tf.join(', ');
        const rt = Array.isArray(toStore.reasoning_trace) ? toStore.reasoning_trace : [];
        rt.push(`Decision: WAIT (ต้องการภาพ TF เพิ่มเติม: ${need})`);
        toStore.reasoning_trace = rt;
      }

      await saveAnalysis(userId, detectedTF, Date.now(), readableTime, toStore, env);
      await markJobDone(job.job_id, env, detectedTF);

      // Update _JOB summary
      const doneAt = Date.now();
      const doneReadable = new Date(doneAt).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
      const stats = await getUserQueueStats(userId, env);
      await saveAnalysis(userId, '_JOB', doneAt, doneReadable, {
        status: 'done',
        jobId: job.job_id,
        messageId: job.message_id,
        resultTF: detectedTF,
        remainingQueued: stats.queued_count,
        doneAt,
        doneReadable
      }, env);

      // If more jobs queued, continue chain
      if (stats.queued_count > 0) {
        await triggerInternalAnalyze(userId, request.url, env);
      }
    } catch (e) {
      console.error("Internal analyze failed:", safeError(e));
      try {
        await markJobError(job.job_id, env, String(e?.message || e));
        const errAt = Date.now();
        const errReadable = new Date(errAt).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });
        await saveAnalysis(userId, '_JOB', errAt, errReadable, {
          status: 'error',
          jobId: job.job_id,
          messageId: job.message_id,
          error: String(e?.message || e),
          errAt,
          errReadable
        }, env);

        if (await hasQueuedJobs(userId, env)) {
          await triggerInternalAnalyze(userId, request.url, env);
        }
      } catch (e2) {
        console.error("Failed to update _JOB marker (error):", safeError(e2));
      }
    }
  })());

  return new Response('ACCEPTED', { status: 202 });
}

// --- MENU: SUMMARY (per TF) ---

async function handleSummaryMenuRequest(userId, replyToken, env) {
  const rows = await getAllAnalyses(userId, env);
  if (!rows || rows.length === 0) {
    await replyText(replyToken, "ยังไม่มีผลวิเคราะห์ในระบบครับ\n\n📸 กรุณาส่งรูปกราฟเข้ามาก่อน", env, mainMenu);
    return;
  }

  // Unique TF list, ordered by TF hierarchy
  const unique = new Map();
  rows.forEach(r => {
    const tf = normalizeTF(r.tf);
    if (!unique.has(tf)) unique.set(tf, r.timestamp);
  });

  const tfList = [...unique.keys()].sort((a, b) => TF_ORDER.indexOf(a) - TF_ORDER.indexOf(b));
  const quickReplyItems = tfList.map(tf => ({
    type: "action",
    action: { type: "message", label: `TF ${tf}`, text: `SUMMARY_TF:${tf}` }
  }));

  // Back to main menu
  quickReplyItems.push({
    type: "action",
    action: { type: "message", label: "⬅️ เมนูหลัก", text: MAIN_MENU_TEXT }
  });

  const msg = "📌 เลือก TF ที่ต้องการดูสรุปผลวิเคราะห์";
  await replyText(replyToken, msg, env, { items: quickReplyItems });
}

function buildTFSetupSummary(data) {
  const setup = data?.trade_setup || data?.detailed_technical_data?.trade_setup || {};
  const entry = setup?.entry_zone ?? '-';
  const tp = setup?.target_price ?? '-';
  const sl = setup?.stop_loss ?? '-';

  let summary = '-';
  if (typeof data?.summary_text === 'string' && data.summary_text.trim()) {
    summary = data.summary_text.trim();
  } else if (Array.isArray(data?.reasoning_trace) && data.reasoning_trace.length) {
    summary = data.reasoning_trace[data.reasoning_trace.length - 1];
  } else if (data?.notes) {
    summary = String(data.notes);
  }

  return { entry, tp, sl, summary };
}

async function handleSummaryTFRequest(userId, targetTF, replyToken, env) {
  if (!targetTF) {
    await replyText(replyToken, "⚠️ TF ไม่ถูกต้อง", env, mainMenu);
    return;
  }

  const rows = await getAllAnalyses(userId, env);
  const row = (rows || []).find(r => normalizeTF(r.tf) === targetTF);

  if (!row) {
    await replyText(replyToken, `ยังไม่มีข้อมูล TF: ${targetTF}`, env, mainMenu);
    return;
  }

  let data = {};
  try { data = JSON.parse(row.analysis_json || '{}'); } catch (_) { data = {}; }

  const { entry, tp, sl, summary } = buildTFSetupSummary(data);
  const ageMins = Math.floor((Date.now() - row.timestamp) / 60000);

  const msg =
    `📌 สรุปผลวิเคราะห์ TF: ${targetTF}\n\n` +
    `🎯 Setup:\n` +
    `- Entry: ${entry}\n` +
    `- TP: ${tp}\n` +
    `- SL: ${sl}\n\n` +
    `💡 สรุป: ${summary}\n\n` +
    `🕒 Updated: ${row.timestamp_readable || '-'} (Age ~${ageMins} นาที)`;

  const quickReply = {
    items: [
      { type: "action", action: { type: "message", label: "📌 เลือก TF อื่น", text: "SUMMARY" } },
      { type: "action", action: { type: "message", label: "⬅️ เมนูหลัก", text: MAIN_MENU_TEXT } }
    ]
  };

  await replyText(replyToken, msg, env, quickReply);
}

// --- MENU: TRADE STYLE (SCALP / SWING) ---

async function handleTradeStyleMenuRequest(userId, replyToken, env) {
  const msg =
`⚡ โหมดวิเคราะห์สำหรับ "เล่นสั้น/เล่นสวิง"

เลือกโหมดที่ต้องการได้เลยครับ (ระบบจะใช้ข้อมูลกราฟล่าสุดที่มีอยู่ในฐานข้อมูลมาประกอบการวิเคราะห์)
- เล่นสั้น: เน้นจังหวะเข้า/ออกเร็ว ใช้ LTF เป็น Trigger แต่ยังยึด HTF เป็นทิศทางหลัก
- เล่นสวิง: เน้นถือเป็นรอบ ใช้ H4/1D เป็นโครงสร้าง แล้วค่อยหา Trigger จาก TF รองลงมา`;

  await replyText(replyToken, msg, env, tradeStyleMenu);
}

function enrichRowsWithFreshness(rows) {
  const now = Date.now();
  return (rows || []).map(r => {
    const tf = normalizeTF(r.tf);
    const ts = Number(r.timestamp || 0);
    const ageMs = ts ? (now - ts) : Number.MAX_SAFE_INTEGER;
    const maxAge = TF_VALIDITY_MS[tf];
    const isFresh = !maxAge ? true : ageMs <= maxAge;

    let data = {};
    try { data = JSON.parse(r.analysis_json || '{}'); } catch (_) { data = {}; }

    return {
      tf,
      timestamp: ts,
      timestamp_readable: r.timestamp_readable,
      ageMins: Math.floor(ageMs / 60000),
      isFresh,
      data
    };
  });
}

function pickMostRecentRowByTF(rows, candidateTFs) {
  let best = null;
  for (const t of candidateTFs) {
    const tf = normalizeTF(t);
    const r = (rows || []).find(x => normalizeTF(x.tf) === tf);
    if (r && (!best || Number(r.timestamp || 0) > Number(best.timestamp || 0))) best = r;
  }
  return best;
}

function selectRowsForTradeStyle(validRows, mode) {
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

function buildTradeStyleContext(enrichedSelected, mode) {
  const header = `=== DB CONTEXT FOR ${mode} (Selected TFs Only / Smart Context) ===`;
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
      `[TF ${x.tf}] ${x.isFresh ? '🟢 Fresh' : '🔴 Stale'} | Age=${x.ageMins}m | Updated=${x.timestamp_readable || '-'}`,
      `- TrendBias: ${d.trend_bias || detailed.trend_bias || '-'}`,
      `- Structure: ${JSON.stringify(structure || {}).slice(0, 380)}`,
      `- Value: ${JSON.stringify(value || {}).slice(0, 380)}`,
      `- Trigger: ${JSON.stringify(trigger || {}).slice(0, 380)}`,
      `- Indicators: ${JSON.stringify(indicators || {}).slice(0, 380)}`,
      `- Setup: ${JSON.stringify(tradeSetup || {}).slice(0, 380)}`,
      '---'
    ].join('\n');
  });

  return [header, lines.join('\n')].join('\n');
}

async function analyzeTradeStyleWithGemini(mode, contextStr, env) {
  const modelId = getModelId(env);
  const apiUrl = `https://generativelanguage.googleapis.com/v1beta/models/${modelId}:generateContent?key=${env.GEMINI_API_KEY}`;

  const modeThai = (mode === 'SCALP') ? 'เล่นสั้น (Scalp)' : 'เล่นสวิง (Swing)';

  const instruction = {
    role: "user",
    parts: [{
      text: `
Role: Expert Technical Analyst (Thai).
Task: Create a trading plan for mode = "${modeThai}" using ONLY the DB context provided.
Methodology: Strict Top-Down (Structure -> Value -> Trigger) + Confluence.

${contextStr}

*** HARD RULES (MUST FOLLOW) ***
1) Strict Top-Down: Direction must follow Higher TF (1D/H4) first.
2) No counter-trend trades. Exception ONLY when price is at a clearly major Support/Resistance or key Fib zone; if exception applies, you MUST label "Counter-trend (Risky)" and reduce confidence.
3) If price is in No Man's Land (no clear value zone), output action = WAIT.
4) Indicators (RSI/MACD/Stoch/MA/Volume) are CONFIRMATION only, not direction setters.
5) Use only what exists in DB context. If missing critical TFs for safe call, set request_update_for_tf accordingly.

*** MODE-SPECIFIC GUIDANCE ***
- SCALP: prioritize precise trigger on LTF, tight invalidation, quick TP; still filter by HTF bias.
- SWING: prioritize HTF structure/value; TP wider; trigger can be from H1/M30.

*** CHAIN-OF-THOUGHT RULE ***
Think step-by-step internally, but do NOT reveal internal chain-of-thought. Output only the JSON below.
In reasoning_trace, provide SHORT bullet summary derived from PRIORITY 1/2/3 (not hidden reasoning).

*** OUTPUT FORMAT (JSON ONLY) ***
{
  "mode": "SCALP|SWING",
  "tfs_used_for_confluence": ["1D","H4","H1","M15"],
  "request_update_for_tf": ["1D"] | null,
  "reasoning_trace": [
    "P1 Structure: ...",
    "P2 Value: ...",
    "P3 Trigger: ..."
  ],
  "trade_plan": {
    "action": "BUY|SELL|WAIT|HOLD",
    "entry_zone": "...",
    "target_price": "...",
    "stop_loss": "...",
    "confidence": "High|Medium|Low",
    "probability_pct": 0
  },
  "risk_notes": [
    "..."
  ],
  "user_response_text": "Generate a concise Thai response:\\n\\n⚡ **โหมด:** ${modeThai}\\n📢 **สถานะ:** [ACTION] (Confidence/Probability)\\n📚 **TF ที่ใช้:** ...\\n\\n🔍 **Top-Down:**\\n1️⃣ Structure: ...\\n2️⃣ Value: ...\\n3️⃣ Trigger: ...\\n\\n🎯 **Setup:**\\n- **Entry:** ...\\n- **TP:** ...\\n- **SL:** ...\\n\\n💡 **สรุป:** ...\\n⚠️ **ความเสี่ยง:** ..."
}
      `.trim()
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
    throw new Error(`AI API Error: ${response.status} ${errText ? ('- ' + errText.slice(0, 200)) : ''}`);
  }

  const data = await response.json();
  const rawText = data?.candidates?.[0]?.content?.parts?.[0]?.text || "";
  return safeParseJsonLoosely(rawText);
}

async function handleTradeStyleAnalysisRequest(userId, mode, replyToken, env) {
  const m = String(mode || '').trim().toUpperCase();
  const finalMode = (m === 'SCALP' || m === 'SWING') ? m : null;

  if (!finalMode) {
    await handleTradeStyleMenuRequest(userId, replyToken, env);
    return;
  }

  const rows = await getAllAnalyses(userId, env);
  const usable = (rows || []).filter(r => normalizeTF(r.tf) && normalizeTF(r.tf) !== '_JOB');

  if (usable.length === 0) {
    await replyText(
      replyToken,
      "❌ ยังไม่มีข้อมูลกราฟในฐานข้อมูล\n\n📸 กรุณาส่งรูปกราฟเข้ามาก่อน แล้วค่อยเลือกโหมดเล่นสั้น/เล่นสวิงครับ",
      env,
      mainMenu
    );
    return;
  }

  // Filter stale data using TF_VALIDITY_MS (same safety rule as image analysis)
  const validRows = usable.filter(row => {
    const tf = normalizeTF(row.tf);
    const maxAge = TF_VALIDITY_MS[tf];
    if (!maxAge) return true;
    const age = Date.now() - Number(row.timestamp || 0);
    return age <= maxAge;
  });

  const selected = selectRowsForTradeStyle(validRows, finalMode);
  const enrichedSelected = enrichRowsWithFreshness(selected);

  // Critical TF requirement for safer calls (mode-specific)
  const critical = (finalMode === 'SCALP') ? ['H1'] : ['1D', 'H4'];
  const missingCritical = critical.filter(tf => !enrichedSelected.some(x => normalizeTF(x.tf) === tf));

  if (missingCritical.length > 0) {
    const modeThai = (finalMode === 'SCALP') ? 'เล่นสั้น (Scalp)' : 'เล่นสวิง (Swing)';
    const ask = missingCritical.join(', ');
    await replyText(
      replyToken,
      `❌ ขาดข้อมูลสำคัญสำหรับโหมด ${modeThai}: ${ask}\n\n📸 กรุณาส่งรูปกราฟ Timeframe **${ask}** เข้ามาอัปเดตก่อน เพื่อให้วิเคราะห์ได้ปลอดภัยและแม่นขึ้นครับ`,
      env,
      tradeStyleMenu
    );
    return;
  }

  const contextStr = buildTradeStyleContext(enrichedSelected, finalMode);

  try {
    const result = await analyzeTradeStyleWithGemini(finalMode, contextStr, env);
    const textOut = result?.user_response_text || "⚠️ วิเคราะห์ได้ แต่ไม่สามารถสร้างข้อความสรุปได้";
    await replyText(replyToken, textOut, env, tradeStyleMenu);
  } catch (e) {
    console.error("Trade style analysis error:", safeError(e));
    await replyText(replyToken, `❌ เกิดข้อผิดพลาดในการวิเคราะห์โหมด ${finalMode}: ${safeError(e)}`, env, tradeStyleMenu);
  }
}



function arrayBufferToBase64(buffer) {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
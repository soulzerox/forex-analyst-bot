import { normalizeTF, safeError, inferLikelyCurrentTF, arrayBufferToBase64, promiseWithTimeout, safeParseJsonLoosely } from './utils.js';
import { TF_VALIDITY_MS, TF_ORDER, CANCEL_TEXT, MAIN_MENU_TEXT } from './config.js';
import { mainMenu, tradeStyleMenu } from './menus.js';
import { replyText, getContentFromLine } from './line.js';
import { analyzeChartStructured, chatWithGeminiText, analyzeTradeStyleWithGemini, reanalyzeFromDB, createFallbackAnalysis, selectRowsForTradeStyle, buildTradeStyleContext } from './ai.js';
import { saveImageToKV, getImageFromKV, saveAnalysisStateToKV, getAnalysisStateFromKV, cleanupAnalysisFromKV } from './kv.js';
import { getAllAnalyses, saveAnalysis, deleteAnalysis, updateAnalysisTF } from './database.js';
import { enqueueAnalysisJob, buildQueueAckMessage, claimNextQueuedJob, requeueJob, markJobDone, markJobError, hasQueuedJobs, getUserQueueStats } from './queue.js';

// --- HELPER: Enrich rows with freshness info & age recommendation ---
function enrichRowsWithFreshness(rows) {
  return (rows || []).map(r => {
    const tf = normalizeTF(r.tf);
    const maxAge = TF_VALIDITY_MS[tf] || (24 * 60 * 60 * 1000);
    const ageMs = Date.now() - Number(r.timestamp || 0);
    const isFresh = ageMs <= maxAge;
    const freshnessPercent = Math.max(0, Math.min(100, Math.round((1 - ageMs / maxAge) * 100)));
    const ageMins = Math.floor(ageMs / 60000);
    
    // Determine if update should be requested (AI will decide based on this)
    // If freshness < 50%, recommend update
    const recommendUpdate = freshnessPercent < 50;
    
    let data = {};
    try { data = JSON.parse(r.analysis_json || '{}'); } catch (_) { data = {}; }
    return {
      ...r,
      tf,
      isFresh,
      ageMins,
      freshnessPercent,
      recommendUpdate,
      data
    };
  });
}

// --- EVENT HANDLER ---
export async function handleEvent(event, env, ctx, requestUrl) {
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

      // --- COMMAND: RE-ANALYZE (Background DB-first re-evaluation) ---
      if (userText === 'REANALYZE') {
        // Start background re-analysis using DB context only
        ctx.waitUntil(reanalyzeFromDB(userId, env, requestUrl));
        await replyText(replyToken, '🔄 เริ่มทำการ Re-analyze ข้อมูลล่าสุดในฐานข้อมูล (ทำงานพื้นหลัง)\nจะอัปเดตผลและแทนที่ผลเก่าเมื่อเสร็จเรียบร้อย', env, mainMenu);
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

      const ackMsg = await buildQueueAckMessage(userId, jobId, createdAt, env);

      await replyText(replyToken, ackMsg, env, mainMenu);

      // Trigger background analysis asynchronously (will cache image in KV from there)
      // Pass ctx so the fetch trigger gets properly awaited
      triggerAsyncJobProcessing(userId, requestUrl, env, ctx)
        .catch(e => console.error("Failed to trigger initial analysis:", safeError(e)));

      return;
    }

  } catch (error) {
    console.error(safeError(error));
    await replyText(replyToken, `⚠️ System Error:
${error.message}`, env, mainMenu);
  }
}

// --- LOGIC: MANAGE DATA (Interactive Menu) ---

export async function handleManageDataRequest(userId, replyToken, env) {
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

export async function handleEditSelection(userId, targetTF, replyToken, env) {
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

// --- LOGIC: STATUS ---

export async function handleStatusRequest(userId, replyToken, env) {
  const rows = await getAllAnalyses(userId, env);
  let msg = "✅ สถานะข้อมูลกราฟในระบบ (กรองตามอายุ):";
  
  if (rows && rows.length > 0) {
    // Filter out internal markers like _JOB
    const visibleRows = rows.filter(r => !String(r.tf || '').startsWith('_'));
    visibleRows.sort((a, b) => b.timestamp - a.timestamp);

    for (const row of visibleRows) {
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

// --- MENU: SUMMARY (per TF) ---

export async function handleSummaryMenuRequest(userId, replyToken, env) {
  const rows = await getAllAnalyses(userId, env);
  if (!rows || rows.length === 0) {
    await replyText(replyToken, "ยังไม่มีผลวิเคราะห์ในระบบครับ\n\n📸 กรุณาส่งรูปกราฟเข้ามาก่อน", env, mainMenu);
    return;
  }

  // Unique TF list, ordered by TF hierarchy, and filter out EXPIRED data
  const unique = new Map();
  const now = Date.now();
  
  rows.forEach(r => {
    const tf = normalizeTF(r.tf);
    // Hide internal markers (e.g., _JOB) from the visible summary menu
    if (String(tf || '').startsWith('_')) return;
    
    // Check if this TF data has expired
    const maxAge = TF_VALIDITY_MS[tf] || (24 * 60 * 60 * 1000); // Default to 24 hours
    const ageMs = now - Number(r.timestamp || 0);
    const isFresh = ageMs <= maxAge;
    
    // Only show fresh (non-expired) TF data
    if (isFresh && !unique.has(tf)) {
      unique.set(tf, r.timestamp);
    }
  });

  if (unique.size === 0) {
    await replyText(replyToken, "❌ ข้อมูลทั้งหมดหมดอายุแล้วครับ\n\n📸 กรุณาส่งรูปกราฟใหม่เข้ามา", env, mainMenu);
    return;
  }

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

  // Re-analyze (DB-first) - single-button background re-evaluation
  quickReplyItems.unshift({
    type: "action",
    action: { type: "message", label: "🔄 Re-analyze (DB)", text: "REANALYZE" }
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

function formatAnalysisSummary(data, row) {
  const tf = normalizeTF(row?.tf) || 'Unknown';
  const setup = data?.trade_setup || data?.detailed_technical_data?.trade_setup || {};
  const structure = data?.structure || data?.detailed_technical_data?.structure || {};
  const value = data?.value || data?.detailed_technical_data?.value || {};
  const trigger = data?.trigger || data?.detailed_technical_data?.trigger || {};
  const action = (setup?.action || 'WAIT').toUpperCase();
  const confidence = setup?.confidence || 'Medium';
  
  // Only show TFs that were actually used for confluence (not requested ones)
  const actualTFs = (data?.tfs_used_for_confluence || []).filter(t => t && !String(t).startsWith('_'));
  const tfs = actualTFs.length > 0 ? actualTFs.join(', ') : '-';
  
  const entry = setup?.entry_zone || '-';
  const tp = setup?.target_price || '-';
  const sl = setup?.stop_loss || '-';
  
  // Build summary in Thai format (same as image analysis)
  const structureText = structure?.market_structure || 'Unknown';
  const valueText = value?.key_levels_summary || 'ไม่ชัดเจน';
  const triggerText = (trigger?.candlestick_patterns || []).length > 0 ? trigger.candlestick_patterns.join(', ') : 'ยังไม่ชัดเจน';
  const summary = data?.user_response_text || '(ไม่มีข้อมูลสรุป)';
  
  // Use the same format as image analysis response
  let lines = [];
  lines.push(`📢 สถานะ: ${action} (${confidence})`);
  lines.push(`⏱️ TF ปัจจุบัน: ${tf}`);
  
  // Show confluence only if there are actual TFs used
  if (tfs !== '-') {
    lines.push(`📚 ข้อมูลประกอบ (Confluence): ${tfs}`);
  }
  
  lines.push('');
  lines.push(`🔍 Top-Down Analysis:`);
  lines.push(`1️⃣ Structure: ${structureText}`);
  lines.push(`2️⃣ Area of Value: ${valueText}`);
  lines.push(`3️⃣ Entry Trigger: ${triggerText}`);
  lines.push('');
  lines.push(`🎯 Setup:`);
  lines.push(`- Entry: ${entry}`);
  lines.push(`- TP: ${tp}`);
  lines.push(`- SL: ${sl}`);
  lines.push('');
  lines.push(`💡 สรุป: ${summary}`);

  return lines.join('\n');
}

export async function handleSummaryTFRequest(userId, targetTF, replyToken, env) {
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

  // Check if data has expired
  const maxAge = TF_VALIDITY_MS[targetTF] || (24 * 60 * 60 * 1000);
  const ageMs = Date.now() - Number(row.timestamp || 0);
  
  if (ageMs > maxAge) {
    await replyText(
      replyToken,
      `❌ ข้อมูล TF: ${targetTF} หมดอายุแล้วครับ\n\n📸 กรุณาส่งรูปกราฟใหม่เข้ามาเพื่ออัปเดต`,
      env,
      mainMenu
    );
    return;
  }

  let data = {};
  try { data = JSON.parse(row.analysis_json || '{}'); } catch (_) { data = {}; }

  const formatted = formatAnalysisSummary(data, row);
  const quickReply = {
    items: [
      { type: "action", action: { type: "message", label: "📌 เลือก TF อื่น", text: "SUMMARY" } },
      { type: "action", action: { type: "message", label: "🔄 Re-analyze (DB)", text: "REANALYZE" } },
      { type: "action", action: { type: "message", label: "⬅️ เมนูหลัก", text: MAIN_MENU_TEXT } }
    ]
  };

  await replyText(replyToken, formatted, env, quickReply);
}

// --- MENU: TRADE STYLE (SCALP / SWING) ---

export async function handleTradeStyleMenuRequest(userId, replyToken, env) {
  const msg =
`⚡ โหมดวิเคราะห์สำหรับ "เล่นสั้น/เล่นสวิง"

เลือกโหมดที่ต้องการได้เลยครับ (ระบบจะใช้ข้อมูลกราฟล่าสุดที่มีอยู่ในฐานข้อมูลมาประกอบการวิเคราะห์)
- เล่นสั้น: เน้นจังหวะเข้า/ออกเร็ว ใช้ LTF เป็น Trigger แต่ยังยึด HTF เป็นทิศทางหลัก
- เล่นสวิง: เน้นถือเป็นรอบ ใช้ H4/1D เป็นโครงสร้าง แล้วค่อยหา Trigger จาก TF รองลงมา`;

  await replyText(replyToken, msg, env, tradeStyleMenu);
}

export async function handleTradeStyleAnalysisRequest(userId, mode, replyToken, env) {
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
      "❌ ยังไม่มีข้อมูลกราฟในฐานข้อมูล\n\n📸 กรุณาส่งรูปกราฟเข้ามาอัปเดตก่อน แล้วค่อยเลือกโหมดเล่นสั้น/เล่นสวิงครับ",
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

// --- INTERNAL BACKGROUND ANALYSIS (Free-plan friendly) ---

export async function triggerInternalAnalyze(userId, requestUrl, env) {
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

// ✅ NEW: Async trigger that returns immediately without waiting
// This allows job processing to happen without blocking the main request
// IMPORTANT: We use ctx.waitUntil() ONLY for the fetch trigger itself (very fast)
// Not for the actual analysis, which happens in a separate worker invocation
async function triggerAsyncJobProcessing(userId, requestUrl, env, ctx) {
  try {
    const u = new URL(requestUrl);
    u.search = '';
    u.searchParams.set('__internal', 'analyze');

    const headers = { 'Content-Type': 'application/json' };
    if (env.INTERNAL_TASK_TOKEN) {
      headers['X-Internal-Task-Token'] = env.INTERNAL_TASK_TOKEN;
    }

    // Create the fetch promise
    const fetchPromise = fetch(u.toString(), {
      method: 'POST',
      headers,
      body: JSON.stringify({ userId })
    }).catch(e => console.error("Async job processing trigger failed:", safeError(e)));

    // If ctx is available, wrap in waitUntil to ensure fetch gets triggered
    // This is safe because the fetch itself is very fast (just HTTP request)
    if (ctx && ctx.waitUntil) {
      ctx.waitUntil(fetchPromise);
    } else {
      // Fallback if ctx not available
      console.warn("ctx not available in triggerAsyncJobProcessing, fetch may not complete");
    }
  } catch (e) {
    console.error("Failed to setup async job processing:", safeError(e));
  }
}


export async function handleInternalAnalyze(request, env, ctx) {
  // Optional protection
  if (env.INTERNAL_TASK_TOKEN) {
    const token = request.headers.get('x-internal-task-token');
    if (!token || token !== env.INTERNAL_TASK_TOKEN) {
      return new Response('Unauthorized', { status: 401 });
    }
  }

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

  const internalTimeoutMs = Math.max(8000, Number(env.INTERNAL_AI_TIMEOUT_MS || 15000));
  const maxAttempts = Math.max(1, Number(env.INTERNAL_MAX_RETRY || 3));
  const attempt = Number(job.attempt || 0);

  const startedReadable = new Date(job.started_at).toLocaleString('th-TH', { timeZone: 'Asia/Bangkok' });

  // Log timeout setting
  // NOTE: Cloudflare free tier has 30s total timeout per request
  // We use 15s for AI to leave 5-10s for image fetch + DB operations
  console.log(`[Job ${job.job_id}] Starting analysis with timeout: ${internalTimeoutMs}ms (Cloudflare max: 30000ms total)`);

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

  // ✅ CRITICAL: Return immediately WITHOUT ctx.waitUntil()
  // Why: ctx.waitUntil has a 30-second timeout on Cloudflare free tier, which isn't enough for AI analysis
  // Solution: Trigger async processing via separate fetch that returns immediately
  // This allows multiple jobs to be processed sequentially without timeout conflicts
  const requestUrl = request.url;
  triggerAsyncJobProcessing(userId, requestUrl, env)
    .catch(e => console.error("Async job trigger failed:", safeError(e)));

  return new Response('ACCEPTED', { status: 202 });
}

// ✅ NEW: Separate function for background analysis (not wrapped in ctx.waitUntil)
async function performAnalysisInBackground(userId, job, internalTimeoutMs, maxAttempts, env, requestUrl) {
  const attempt = Number(job.attempt || 0);

  try {
    // Fetch image from LINE - with fallback to cached version if fetch fails
    let imageBinary, contentType;
    try {
      const result = await getContentFromLine(job.message_id, env);
      imageBinary = result.arrayBuffer;
      contentType = result.contentType;
    } catch (lineErr) {
      console.warn(`[Job ${job.job_id}] Failed to fetch from LINE API:`, safeError(lineErr));
      // Try to recover from KV cache
      const cachedImg = await getImageFromKV(env.ANALYSIS_KV, userId, job.job_id);
      if (cachedImg && cachedImg.base64) {
        console.log(`[Job ${job.job_id}] Recovered from cached image in KV`);
        // Convert base64 back to binary
        const binaryString = atob(cachedImg.base64);
        imageBinary = new Uint8Array(binaryString.length);
        for (let i = 0; i < binaryString.length; i++) {
          imageBinary[i] = binaryString.charCodeAt(i);
        }
        contentType = cachedImg.contentType || 'image/jpeg';
      } else {
        throw new Error(`Cannot fetch image from LINE and no cached version available: ${lineErr.message}`);
      }
    }
    
    const base64Image = arrayBufferToBase64(imageBinary);

    // Cache image in KV for timeout recovery & retry capability
    try {
      if (env.ANALYSIS_KV) {
        await saveImageToKV(env.ANALYSIS_KV, userId, job.job_id, base64Image, contentType, 0);
      } else {
        console.warn(`[Job ${job.job_id}] ANALYSIS_KV not configured, skipping cache`);
      }
    } catch (kvErr) {
      console.warn(`[Job ${job.job_id}] Failed to cache image to KV:`, safeError(kvErr));
    }

    const existingRows = (await getAllAnalyses(userId, env)).filter(r => !String(r.tf || '').startsWith('_'));
    const controller = new AbortController();

    let analysisResult;
    let shouldContinueToStorage = true; // Flag to determine if we should proceed to storage
    
    try {
      analysisResult = await promiseWithTimeout(
        analyzeChartStructured(userId, base64Image, existingRows, env, { mimeType: contentType, signal: controller.signal }),
        internalTimeoutMs
      );
    } catch (err) {
        // Handle timeout gracefully with smart recovery
        const msg = String(err?.message || err);
        const isTimeout = err && (err.name === 'TimeoutError' || err.name === 'AbortError');
        
        // For TIMEOUT: Try to recover by analyzing from cached KV image + retry one more time
        if (isTimeout) {
          console.warn('AI analysis timeout, attempting recovery from KV cache:', msg);
          // Abort original controller to free resources immediately
          try { controller.abort(); } catch (_) {}
          
          const attempt_recovery = Number(job.attempt || 0) + 1;
          const maxRecoveryAttempts = 2;
          
          // If we haven't exceeded max recovery attempts, try one retry from KV
          if (attempt_recovery <= maxRecoveryAttempts) {
            console.log(`[Timeout Recovery] Attempt ${attempt_recovery}/${maxRecoveryAttempts} - Fetching from KV and retrying`);
            try {
              const cachedImg = await getImageFromKV(env.ANALYSIS_KV, userId, job.job_id);
              if (cachedImg && cachedImg.base64) {
                // Retry analysis with shorter timeout
                const recoveryTimeoutMs = Math.max(5000, internalTimeoutMs / 2);
                const recoveryController = new AbortController();
                try {
                  analysisResult = await promiseWithTimeout(
                    analyzeChartStructured(userId, cachedImg.base64, existingRows, env, { mimeType: cachedImg.contentType, signal: recoveryController.signal }),
                    recoveryTimeoutMs
                  );
                  console.log('[Timeout Recovery] Success! Analysis completed on recovery attempt');
                  analysisResult._recovery_attempt = attempt_recovery;
                } catch (recoveryErr) {
                  // Recovery also failed, fall back to previous analysis
                  console.warn('[Timeout Recovery] Recovery attempt also timed out, using cached analysis');
                  const recentAnalysis = existingRows?.[0];
                  const recentContext = recentAnalysis ? JSON.parse(recentAnalysis.analysis_json || '{}') : null;
                  analysisResult = createFallbackAnalysis(userId, 'Unknown', recentContext);
                  analysisResult._analysis_timeout = true;
                  analysisResult._recovery_failed = true;
                } finally {
                  try { recoveryController.abort(); } catch (_) {}
                }
              } else {
                // No KV cache available, use immediate fallback
                console.warn('[Timeout Recovery] No cached image in KV, using fallback analysis');
                const recentAnalysis = existingRows?.[0];
                const recentContext = recentAnalysis?.analysis_json ? JSON.parse(recentAnalysis.analysis_json || '{}') : null;
                analysisResult = createFallbackAnalysis(userId, 'Unknown', recentContext);
                analysisResult._analysis_timeout = true;
              }
            } catch (kvErr) {
              console.error('[Timeout Recovery] KV fetch failed:', safeError(kvErr));
              const recentAnalysis = existingRows?.[0];
              const recentContext = recentAnalysis?.analysis_json ? JSON.parse(recentAnalysis.analysis_json || '{}') : null;
              analysisResult = createFallbackAnalysis(userId, 'Unknown', recentContext);
              analysisResult._analysis_timeout = true;
            }
          } else {
            // Exceeded max recovery attempts, give up and use fallback
            console.log('[Timeout Recovery] Max recovery attempts exceeded, using fallback');
            const recentAnalysis = existingRows?.[0];
            const recentContext = recentAnalysis?.analysis_json ? JSON.parse(recentAnalysis.analysis_json || '{}') : null;
            analysisResult = createFallbackAnalysis(userId, 'Unknown', recentContext);
            analysisResult._analysis_timeout = true;
          }
        } else {
          // For other errors: abort controller and retry if retryable
          try { controller.abort(); } catch (_) {}
          
          const retryable = msg.includes('429') || msg.includes('503') || msg.includes('500');
          const nextAttempt = attempt + 1;
          
          if (retryable && nextAttempt <= maxAttempts) {
            console.log(`[Job ${job.job_id}] Retryable error (attempt ${nextAttempt}/${maxAttempts}): ${msg}`);
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

            triggerAsyncJobProcessing(userId, requestUrl, env)
              .catch(e => console.error("Failed to trigger retry job:", safeError(e)));
            shouldContinueToStorage = false;
          } else {
            // non-retryable / exceeded attempts
            console.log(`[Job ${job.job_id}] Non-retryable error or max attempts exceeded: ${msg}`);
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
              await triggerInternalAnalyze(userId, requestUrl, env);
            }
            shouldContinueToStorage = false;
          }
        }
    } finally {
        try { controller.abort(); } catch (_) {}
    }

    // Only proceed to storage if we have a valid analysis result
    if (!shouldContinueToStorage || !analysisResult) {
      return;
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
      console.log(`[Job ${job.job_id}] Analysis saved for TF: ${detectedTF}`);
      
      await markJobDone(job.job_id, env, detectedTF);
      console.log(`[Job ${job.job_id}] Job marked as done`);

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

      // Clean up cached image and state from KV (analysis is complete)
      await cleanupAnalysisFromKV(env.ANALYSIS_KV, userId, job.job_id);
      console.log(`[Job ${job.job_id}] Cleaned up KV cache and state`);

      // If more jobs queued, trigger next job asynchronously (non-blocking)
      if (stats.queued_count > 0) {
        triggerAsyncJobProcessing(userId, requestUrl, env)
          .catch(e => console.error("Failed to trigger next job:", safeError(e)));
      }
    } catch (e) {
      console.error("Background analysis failed:", safeError(e));
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
          triggerAsyncJobProcessing(userId, requestUrl, env)
            .catch(e => console.error("Failed to trigger next job after error:", safeError(e)));
        }
      } catch (e2) {
        console.error("Failed to update _JOB marker (error):", safeError(e2));
      }
    }
}

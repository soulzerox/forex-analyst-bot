// ============================================================================
// KV.JS - Workers KV Management for Image Cache & Analysis State
// ============================================================================
// Purpose: Store image data, analysis state, and retry context in KV
//          to enable recovery from timeout and efficient reanalysis
// 
// Key Operations:
// 1. Save image + metadata when job is enqueued
// 2. Retrieve image for analysis (fallback on timeout)
// 3. Store partial analysis results
// 4. Clean up old entries after successful analysis
// ============================================================================

const KV_EXPIRATION_SECONDS = 7 * 24 * 60 * 60; // 7 days
const IMAGE_CACHE_PREFIX = 'img:';
const ANALYSIS_STATE_PREFIX = 'state:';

/**
 * Save image and metadata to KV for future retrieval
 * Key: img:{userId}:{jobId}
 * Value: { base64: string, contentType: string, timestamp: number, attempt: number }
 */
export async function saveImageToKV(kv, userId, jobId, base64Image, contentType, attempt = 0) {
  const key = `${IMAGE_CACHE_PREFIX}${userId}:${jobId}`;
  const value = {
    base64: base64Image,
    contentType: contentType || 'image/jpeg',
    timestamp: Date.now(),
    attempt,
    userId,
    jobId
  };
  
  try {
    await kv.put(key, JSON.stringify(value), {
      expirationTtl: KV_EXPIRATION_SECONDS
    });
    console.log(`[KV] Saved image: ${key} (${base64Image.length} bytes)`);
    return true;
  } catch (err) {
    console.error(`[KV] Failed to save image: ${key}`, err);
    return false;
  }
}

/**
 * Retrieve image from KV by jobId
 * Returns: { base64, contentType, timestamp, attempt } or null
 */
export async function getImageFromKV(kv, userId, jobId) {
  const key = `${IMAGE_CACHE_PREFIX}${userId}:${jobId}`;
  
  try {
    const cached = await kv.get(key, 'json');
    if (!cached) {
      console.warn(`[KV] No cached image found: ${key}`);
      return null;
    }
    
    console.log(`[KV] Retrieved image: ${key} (age: ${Date.now() - cached.timestamp}ms)`);
    return cached;
  } catch (err) {
    console.error(`[KV] Failed to retrieve image: ${key}`, err);
    return null;
  }
}

/**
 * Save partial/full analysis state to KV for retry logic
 * Key: state:{userId}:{jobId}
 * Value: { analysis, timestamp, attempt, status: 'pending|success|timeout|partial' }
 */
export async function saveAnalysisStateToKV(kv, userId, jobId, analysis, status = 'pending', attempt = 0) {
  const key = `${ANALYSIS_STATE_PREFIX}${userId}:${jobId}`;
  const value = {
    analysis,
    status,
    timestamp: Date.now(),
    attempt,
    userId,
    jobId
  };
  
  try {
    await kv.put(key, JSON.stringify(value), {
      expirationTtl: KV_EXPIRATION_SECONDS
    });
    console.log(`[KV] Saved analysis state: ${key} (status: ${status})`);
    return true;
  } catch (err) {
    console.error(`[KV] Failed to save analysis state: ${key}`, err);
    return false;
  }
}

/**
 * Retrieve analysis state from KV
 * Returns: { analysis, status, timestamp, attempt } or null
 */
export async function getAnalysisStateFromKV(kv, userId, jobId) {
  const key = `${ANALYSIS_STATE_PREFIX}${userId}:${jobId}`;
  
  try {
    const cached = await kv.get(key, 'json');
    if (!cached) {
      console.warn(`[KV] No cached state found: ${key}`);
      return null;
    }
    
    console.log(`[KV] Retrieved analysis state: ${key} (status: ${cached.status})`);
    return cached;
  } catch (err) {
    console.error(`[KV] Failed to retrieve analysis state: ${key}`, err);
    return null;
  }
}

/**
 * Delete cached image and state after successful analysis
 */
export async function cleanupAnalysisFromKV(kv, userId, jobId) {
  const imgKey = `${IMAGE_CACHE_PREFIX}${userId}:${jobId}`;
  const stateKey = `${ANALYSIS_STATE_PREFIX}${userId}:${jobId}`;
  
  try {
    await Promise.all([
      kv.delete(imgKey),
      kv.delete(stateKey)
    ]);
    console.log(`[KV] Cleaned up analysis: ${userId}:${jobId}`);
    return true;
  } catch (err) {
    console.error(`[KV] Failed to cleanup analysis: ${userId}:${jobId}`, err);
    return false;
  }
}

/**
 * List all cached images for a user (for debugging/monitoring)
 */
export async function listUserAnalysesInKV(kv, userId) {
  try {
    const prefix = `${IMAGE_CACHE_PREFIX}${userId}:`;
    const list = await kv.list({ prefix });
    console.log(`[KV] Found ${list.keys.length} cached analyses for user: ${userId}`);
    return list.keys;
  } catch (err) {
    console.error(`[KV] Failed to list user analyses: ${userId}`, err);
    return [];
  }
}

/**
 * Get storage stats for a user (debugging)
 */
export async function getKVStatsForUser(kv, userId) {
  try {
    const imgPrefix = `${IMAGE_CACHE_PREFIX}${userId}:`;
    const statePrefix = `${ANALYSIS_STATE_PREFIX}${userId}:`;
    
    const imgList = await kv.list({ prefix: imgPrefix });
    const stateList = await kv.list({ prefix: statePrefix });
    
    return {
      user: userId,
      cachedImages: imgList.keys.length,
      cachedStates: stateList.keys.length,
      totalCached: imgList.keys.length + stateList.keys.length
    };
  } catch (err) {
    console.error(`[KV] Failed to get stats for user: ${userId}`, err);
    return { user: userId, error: String(err) };
  }
}

export default {
  saveImageToKV,
  getImageFromKV,
  saveAnalysisStateToKV,
  getAnalysisStateFromKV,
  cleanupAnalysisFromKV,
  listUserAnalysesInKV,
  getKVStatsForUser
};

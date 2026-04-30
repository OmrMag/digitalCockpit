/* ==========================================================================
   Digital Construction Cockpit — Utilities
   Hacker text animation + data caching
   ========================================================================== */

// ---------------------------------------------------------------------------
// Hacker Text Animation
// Scrambles characters left to right, first letter slowest, last fastest
// ---------------------------------------------------------------------------

const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789@#$%&*!?<>{}[]';

function hackerText(element, finalText, options = {}) {
    const {
        duration = 1500,      // total animation time in ms
        startDelay = 0,       // delay before starting
        scrambleSpeed = 30,   // ms between character changes
    } = options;

    if (!finalText || !element) return;

    const len = finalText.length;
    const resolved = new Array(len).fill(false);
    const current = new Array(len).fill('');

    // Calculate resolve time for each character
    // First char resolves last (slowest), last char resolves first (fastest)
    const resolveDelays = [];
    for (let i = 0; i < len; i++) {
        // Exponential curve: last chars resolve much faster
        const progress = i / Math.max(len - 1, 1);
        const delay = duration * (1 - Math.pow(progress, 0.5));
        resolveDelays.push(delay);
    }

    let startTime = null;
    let scrambleInterval = null;

    function scramble() {
        let display = '';
        for (let i = 0; i < len; i++) {
            if (resolved[i]) {
                display += finalText[i];
            } else if (finalText[i] === ' ') {
                display += ' ';
            } else {
                display += CHARS[Math.floor(Math.random() * CHARS.length)];
            }
        }
        element.textContent = display;
    }

    function resolve(timestamp) {
        if (!startTime) startTime = timestamp;
        const elapsed = timestamp - startTime;

        // Resolve characters from right to left (last char first)
        for (let i = len - 1; i >= 0; i--) {
            if (!resolved[i] && elapsed >= resolveDelays[i]) {
                resolved[i] = true;
            }
        }

        if (resolved.every(r => r)) {
            clearInterval(scrambleInterval);
            element.textContent = finalText;
            return;
        }

        requestAnimationFrame(resolve);
    }

    setTimeout(() => {
        scrambleInterval = setInterval(scramble, scrambleSpeed);
        requestAnimationFrame(resolve);
    }, startDelay);
}

// Batch animate multiple elements
function hackerTextAll(selector, options = {}) {
    const elements = document.querySelectorAll(selector);
    elements.forEach((el, index) => {
        const text = el.dataset.hackerText || el.textContent;
        el.dataset.hackerText = text;
        hackerText(el, text, {
            ...options,
            startDelay: (options.stagger || 100) * index,
        });
    });
}

// ---------------------------------------------------------------------------
// Data Cache
// Uses localStorage with timestamps, shows cached data while syncing fresh
// ---------------------------------------------------------------------------

const CACHE_PREFIX = 'dcc_cache_';
const CACHE_TTL = 15 * 60 * 1000; // 15 minutes default

function cacheGet(key) {
    try {
        const raw = localStorage.getItem(CACHE_PREFIX + key);
        if (!raw) return null;
        const cached = JSON.parse(raw);
        return {
            data: cached.data,
            timestamp: cached.timestamp,
            age: Date.now() - cached.timestamp,
            fresh: (Date.now() - cached.timestamp) < CACHE_TTL,
        };
    } catch { return null; }
}

function cacheSet(key, data) {
    try {
        localStorage.setItem(CACHE_PREFIX + key, JSON.stringify({
            data: data,
            timestamp: Date.now(),
        }));
    } catch { /* storage full, ignore */ }
}

function cacheClear(key) {
    if (key) {
        localStorage.removeItem(CACHE_PREFIX + key);
    } else {
        Object.keys(localStorage)
            .filter(k => k.startsWith(CACHE_PREFIX))
            .forEach(k => localStorage.removeItem(k));
    }
}

// ---------------------------------------------------------------------------
// Smart Fetch — show cached data instantly, sync in background
// ---------------------------------------------------------------------------

async function smartFetch(url, cacheKey, options = {}) {
    const {
        onCached = null,      // callback with cached data
        onFresh = null,       // callback with fresh data
        onSyncing = null,     // callback when sync starts
        onError = null,       // callback on error
        forceRefresh = false, // skip cache
        fetchOptions = {},    // pass to fetch()
    } = options;

    // Step 1: Show cached data immediately
    if (!forceRefresh) {
        const cached = cacheGet(cacheKey);
        if (cached && cached.data) {
            if (onCached) onCached(cached.data, cached.age);
            if (cached.fresh) return cached.data; // Still fresh, no need to re-fetch
        }
    }

    // Step 2: Fetch fresh data in background
    if (onSyncing) onSyncing();

    try {
        const response = await fetch(url, fetchOptions);
        const data = await response.json();
        cacheSet(cacheKey, data);
        if (onFresh) onFresh(data);
        return data;
    } catch (err) {
        if (onError) onError(err);
        // Return cached data as fallback
        const cached = cacheGet(cacheKey);
        return cached ? cached.data : null;
    }
}

// ---------------------------------------------------------------------------
// Sync Indicator
// Shows a subtle "syncing" badge in the page header
// ---------------------------------------------------------------------------

function showSyncIndicator(message = 'Syncing') {
    let indicator = document.getElementById('syncIndicator');
    if (!indicator) {
        indicator = document.createElement('div');
        indicator.id = 'syncIndicator';
        indicator.style.cssText = `
            position: fixed; top: 12px; right: 20px; z-index: 300;
            background: var(--navy); color: var(--text-light);
            padding: 5px 12px; border-radius: 4px;
            font-family: var(--font); font-size: 10px;
            font-weight: 500; letter-spacing: 0.5px;
            text-transform: uppercase;
            display: flex; align-items: center; gap: 6px;
            opacity: 0; transition: opacity 0.3s ease;
        `;
        document.body.appendChild(indicator);
    }
    indicator.innerHTML = `<span class="spinner" style="width:10px;height:10px;border-width:1.5px;"></span> ${message}`;
    indicator.style.opacity = '1';
    return indicator;
}

function hideSyncIndicator() {
    const indicator = document.getElementById('syncIndicator');
    if (indicator) {
        indicator.style.opacity = '0';
        setTimeout(() => indicator.remove(), 300);
    }
}

// ---------------------------------------------------------------------------
// Time Ago helper
// ---------------------------------------------------------------------------

function timeAgo(ms) {
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return 'just now';
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    const days = Math.floor(hours / 24);
    return `${days}d ago`;
}
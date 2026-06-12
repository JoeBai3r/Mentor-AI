// background service worker for MV3
let ws = null;
let wsUrl = 'ws://127.0.0.1:3030/ws';
let lastSent = { tabId: null, url: null, ts: 0 };

function ensureWs() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  try {
    ws = new WebSocket(wsUrl);
    ws.onopen = () => console.log('Activity extension websocket open');
    ws.onclose = () => console.log('Activity extension websocket closed');
    ws.onerror = (e) => console.error('ws error', e);
  } catch (e) {
    console.error('ws create error', e);
    ws = null;
  }
}

function extractSite(url) {
  try {
    let u = new URL(url);
    return u.hostname.replace('www.', '');
  } catch (e) { return '' }
}

// Classify a hostname into a coarse intent bucket. The category matters more
// to downstream inference than the raw URL.
const CATEGORY_RULES = [
  { category: 'documentation', hosts: ['docs.rs', 'developer.mozilla.org', 'devdocs.io', 'readthedocs.io', 'rust-lang.org', 'doc.rust-lang.org', 'docs.python.org', 'docs.github.com'] },
  { category: 'error_lookup', hosts: ['stackoverflow.com', 'stackexchange.com', 'serverfault.com', 'superuser.com'] },
  { category: 'code_hosting', hosts: ['github.com', 'gitlab.com', 'bitbucket.org', 'crates.io', 'npmjs.com', 'pypi.org'] },
  { category: 'ai_assistant', hosts: ['chatgpt.com', 'chat.openai.com', 'claude.ai', 'gemini.google.com', 'perplexity.ai'] },
  { category: 'communication', hosts: ['mail.google.com', 'outlook.office.com', 'slack.com', 'discord.com', 'teams.microsoft.com', 'zoom.us'] },
  { category: 'project_management', hosts: ['linear.app', 'atlassian.net', 'jira.com', 'trello.com', 'notion.so', 'asana.com'] },
  { category: 'search', hosts: ['google.com', 'bing.com', 'duckduckgo.com', 'kagi.com'] },
  { category: 'entertainment', hosts: ['youtube.com', 'vimeo.com', 'reddit.com', 'netflix.com', 'twitch.tv'] },
  { category: 'social', hosts: ['linkedin.com', 'facebook.com', 'instagram.com', 'tiktok.com', 'pinterest.com', 'twitter.com', 'x.com'] },
];

function categorizeSite(hostname) {
  if (!hostname) return 'other';
  for (const rule of CATEGORY_RULES) {
    if (rule.hosts.some(h => hostname === h || hostname.endsWith('.' + h))) return rule.category;
  }
  return 'other';
}

// Search queries are direct intent signals - extract the query param from
// known search engines / site search boxes.
const SEARCH_QUERY_PARAMS = {
  'google.com': 'q',
  'bing.com': 'q',
  'duckduckgo.com': 'q',
  'kagi.com': 'q',
  'youtube.com': 'search_query',
  'stackoverflow.com': 'q',
  'github.com': 'q',
  'amazon.com': 'k',
};

function extractSearchQuery(url, hostname) {
  try {
    const u = new URL(url);
    for (const [host, param] of Object.entries(SEARCH_QUERY_PARAMS)) {
      if (hostname === host || hostname.endsWith('.' + host)) {
        const q = u.searchParams.get(param);
        if (q) return q;
      }
    }
  } catch (e) { /* ignore */ }
  return null;
}

// Execute a small script in the page to gather metadata (title, metas, h1, selection)
async function gatherPageMetadata(tabId) {
  try {
    const results = await chrome.scripting.executeScript({
      target: { tabId },
      func: () => {
        try {
          const h1 = (document.querySelector('h1') && document.querySelector('h1').innerText) || null;
          const selection = window.getSelection ? window.getSelection().toString().slice(0,500) : null;
          return {
            title: document.title || null,
            h1, selection,
            url: location.href,
          };
        } catch (e) { return { error: String(e) }; }
      }
    });
    if (results && results.length && results[0].result) return results[0].result;
  } catch (e) {
    console.error('gatherPageMetadata error', e);
  }
  return null;
}

async function sendTabInfo(tab) {
  if (!tab || !tab.id) return;
  // basic dedupe: avoid sending same url repeatedly within 5s
  const now = Date.now();
  if (lastSent.tabId === tab.id && lastSent.url === tab.url && (now - lastSent.ts) < 5000) return;

  ensureWs();
  const meta = await gatherPageMetadata(tab.id).catch(()=>null);
  const site = extractSite(tab.url || '');
  const url = tab.url || null;
  const category = categorizeSite(site);
  const search_query = url ? extractSearchQuery(url, site) : null;

  const payload = {
    source: 'browser',
    site,
    url,
    title: (meta && meta.title) || tab.title || null,
    h1: meta && meta.h1 || null,
    selection: meta && meta.selection || null,
    category,
    search_query,
    timestamp: Math.floor(Date.now()/1000)
  };

  const txt = JSON.stringify(payload);
  console.log('tab switched ->', txt);
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(txt);
    lastSent = { tabId: tab.id, url: tab.url, ts: now };
  } else {
    ensureWs();
    setTimeout(()=>{ if (ws && ws.readyState === WebSocket.OPEN) { ws.send(txt); lastSent = { tabId: tab.id, url: tab.url, ts: now }; } }, 200);
  }
}

async function onActivated(activeInfo) {
  try {
    const tab = await chrome.tabs.get(activeInfo.tabId);
    await sendTabInfo(tab);
  } catch (e) { console.error('onActivated error', e); }
}

chrome.tabs.onActivated.addListener(onActivated);

// Also handle tab updates (url/title changes)
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab)=>{
  if (changeInfo.status === 'complete' || changeInfo.title) {
    // if the updated tab is active, treat as activation
    chrome.tabs.query({active:true, currentWindow:true}, (tabs)=>{
      if (tabs && tabs.length && tabs[0].id === tabId) {
        sendTabInfo(tabs[0]);
      }
    });
  }
});

// open ws eagerly
ensureWs();

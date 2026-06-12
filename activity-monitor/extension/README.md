Activity Monitor Browser Extension (development)

Install (developer mode):
1. Open chrome://extensions in Chrome/Chromium
2. Enable Developer mode
3. Click "Load unpacked" and select this extension/ directory

After making changes to background.js, click the reload icon on the
extension's card in chrome://extensions to pick them up (the service
worker is not hot-reloaded).

What it does:
- Listens for tab activation and tab updates
- For each tab activation, builds a JSON payload and sends it over a
  WebSocket to ws://127.0.0.1:3030/ws:
  {
    source: "browser",
    site: "docs.rs",
    url: "https://docs.rs/tokio/latest/tokio/",
    title: "tokio - Rust",
    h1: "Crate tokio",
    selection: null,
    category: "documentation",
    search_query: null,
    timestamp: 1718000000
  }

Field notes:
- site / url / title / h1 / selection: page identity and content gathered
  from the active tab.
- category: a coarse intent bucket inferred from the domain (e.g.
  "documentation", "error_lookup", "code_hosting", "ai_assistant",
  "communication", "project_management", "search", "entertainment",
  "other").
- search_query: the search query extracted from the URL when the page is a
  known search engine (Google, Bing, DuckDuckGo, YouTube, Stack Overflow,
  GitHub, Amazon, etc), otherwise null.

Notes:
- The daemon must be running on localhost for messages to be received.

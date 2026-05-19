(function () {
  // ============================================================================
  // Remove Mintlify "Powered by" link (kept from original custom.js)
  // ============================================================================
  function removePoweredBy() {
    const links = document.querySelectorAll('a[href*="mintlify.com"]');
    links.forEach((link) => {
      if (link.textContent && link.textContent.includes("Powered by")) {
        link.remove();
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", removePoweredBy);
  } else {
    removePoweredBy();
  }

  // ============================================================================
  // Rift Feedback Chat Widget
  // ----------------------------------------------------------------------------
  // Vanilla DOM port of the dapp's FeedbackChat component. Talks to the same
  // Supabase Edge Function used by the dapp + admin dashboard.
  // ============================================================================

  const SUPABASE_URL =
    "https://ookzpviwfhzfarouusah.supabase.co/functions/v1/chats";
  const SUPABASE_ANON_KEY =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9va3pwdml3Zmh6ZmFyb3V1c2FoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI2OTYxMDEsImV4cCI6MjA3ODI3MjEwMX0._FALhXPYD7nVGGg-Jw1EZ8DfjaPPWd7VBcMl86ckRZI";

  const VISITOR_ID_KEY = "rift_docs_visitor_id";
  const POLL_INTERVAL_MS = 10000;

  const ACCENT = "#fe8f21";
  const STARTER_SUGGESTIONS = [
    "what feature do you wish we had?",
    "what's most confusing about rift?",
    "what chain should we launch on next?",
  ];

  // --------------------------------------------------------------------------
  // Visitor identity (persistent random 0x... 40-hex address-shaped ID).
  // --------------------------------------------------------------------------
  function getVisitorId() {
    try {
      const cached = window.localStorage.getItem(VISITOR_ID_KEY);
      if (cached && /^0x[0-9a-f]{40}$/.test(cached)) return cached;
    } catch (_) {}

    const bytes = new Uint8Array(20);
    (window.crypto || window.msCrypto).getRandomValues(bytes);
    const hex = Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    const id = "0x" + hex;
    try {
      window.localStorage.setItem(VISITOR_ID_KEY, id);
    } catch (_) {}
    return id;
  }

  // --------------------------------------------------------------------------
  // Supabase Edge Function client (mirrors frontend/src/utils/chatClient.ts).
  // --------------------------------------------------------------------------
  function chatHeaders() {
    return {
      "content-type": "application/json",
      apikey: SUPABASE_ANON_KEY,
      authorization: "Bearer " + SUPABASE_ANON_KEY,
    };
  }

  async function apiCreateChat(address, meta) {
    const r = await fetch(SUPABASE_URL + "/create", {
      method: "POST",
      headers: chatHeaders(),
      body: JSON.stringify({ address: address, meta: meta || {} }),
    });
    if (!r.ok) throw new Error(await r.text());
    const data = await r.json();
    return data.id;
  }

  async function apiListChats(address) {
    const r = await fetch(
      SUPABASE_URL + "/list?address=" + encodeURIComponent(address),
      { headers: chatHeaders() },
    );
    if (!r.ok) throw new Error(await r.text());
    return await r.json();
  }

  async function apiGetThread(chatId, address) {
    const r = await fetch(
      SUPABASE_URL +
        "/thread/" +
        encodeURIComponent(chatId) +
        "?address=" +
        encodeURIComponent(address),
      { headers: chatHeaders() },
    );
    if (!r.ok) throw new Error(await r.text());
    return await r.json();
  }

  async function apiAppendMessage(chatId, address, role, message) {
    const r = await fetch(SUPABASE_URL + "/append", {
      method: "POST",
      headers: chatHeaders(),
      body: JSON.stringify({
        chat_id: chatId,
        address: address,
        role: role,
        message: message,
      }),
    });
    if (!r.ok) throw new Error(await r.text());
    return await r.json();
  }

  async function apiMarkRead(chatId, address) {
    const r = await fetch(SUPABASE_URL + "/mark-read", {
      method: "POST",
      headers: chatHeaders(),
      body: JSON.stringify({ chat_id: chatId, address: address }),
    });
    if (!r.ok) throw new Error(await r.text());
    return await r.json();
  }

  // --------------------------------------------------------------------------
  // Stylesheet (injected once).
  // --------------------------------------------------------------------------
  function injectStyles() {
    if (document.getElementById("rift-chat-styles")) return;
    const style = document.createElement("style");
    style.id = "rift-chat-styles";
    style.textContent = `
      .rift-chat-root, .rift-chat-root * { box-sizing: border-box; }
      .rift-chat-root {
        position: fixed;
        bottom: 30px;
        right: 30px;
        z-index: 2147483000;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
        color: #f5f5f5;
      }
      .rift-chat-button {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        background: linear-gradient(180deg, rgba(254,143,33,0.95) 0%, rgba(229,116,18,0.95) 100%);
        color: #1a0d00;
        border: 1px solid ${ACCENT};
        border-radius: 999px;
        padding: 10px 18px;
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 0.5px;
        text-transform: uppercase;
        cursor: pointer;
        box-shadow: 0 6px 20px rgba(254,143,33,0.35), 0 2px 6px rgba(0,0,0,0.3);
        transition: transform 0.15s ease, box-shadow 0.15s ease, opacity 0.15s ease;
        position: relative;
        user-select: none;
      }
      .rift-chat-button:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 28px rgba(254,143,33,0.45), 0 3px 8px rgba(0,0,0,0.35);
      }
      .rift-chat-button:active { transform: translateY(0); }
      .rift-chat-button svg { display: block; }
      .rift-chat-badge {
        position: absolute;
        top: -8px;
        right: -8px;
        background: #dc2626;
        color: #fff;
        border-radius: 50%;
        min-width: 22px;
        height: 22px;
        padding: 0 6px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        font-weight: 700;
        border: 2px solid #0f0f14;
        line-height: 1;
      }
      .rift-chat-badge[data-count="0"] { display: none; }

      .rift-chat-panel {
        position: absolute;
        bottom: 56px;
        right: 0;
        width: min(460px, calc(100vw - 24px));
        height: min(580px, calc(100vh - 120px));
        background: rgba(15, 15, 20, 0.96);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid rgba(254,143,33,0.45);
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 12px 40px rgba(0,0,0,0.6);
        display: none;
        flex-direction: column;
        transform-origin: bottom right;
        animation: rift-chat-pop 0.16s ease-out;
      }
      @keyframes rift-chat-pop {
        from { opacity: 0; transform: scale(0.92) translateY(8px); }
        to   { opacity: 1; transform: scale(1) translateY(0); }
      }
      .rift-chat-panel[data-open="true"] { display: flex; }

      .rift-chat-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 14px 16px;
        border-bottom: 1px solid rgba(255,255,255,0.08);
        background: rgba(0,0,0,0.35);
      }
      .rift-chat-header-title {
        font-size: 13px;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
        color: #f5f5f5;
      }
      .rift-chat-close {
        background: transparent;
        border: none;
        color: #f5f5f5;
        cursor: pointer;
        padding: 4px 6px;
        border-radius: 6px;
        font-size: 18px;
        line-height: 1;
        transition: background 0.15s ease;
      }
      .rift-chat-close:hover { background: rgba(255,255,255,0.08); }

      .rift-chat-messages {
        flex: 1;
        overflow-y: auto;
        padding: 14px;
        display: flex;
        flex-direction: column;
        gap: 10px;
      }
      .rift-chat-messages::-webkit-scrollbar { width: 8px; }
      .rift-chat-messages::-webkit-scrollbar-track { background: transparent; }
      .rift-chat-messages::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
      .rift-chat-messages::-webkit-scrollbar-thumb:hover { background: #444; }

      .rift-chat-msg {
        max-width: 82%;
        padding: 10px 12px;
        border-radius: 12px;
        font-size: 13px;
        line-height: 1.45;
        white-space: pre-wrap;
        word-wrap: break-word;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
      }
      .rift-chat-msg-row { display: flex; }
      .rift-chat-msg-row[data-role="user"] { justify-content: flex-end; }
      .rift-chat-msg-row[data-role="admin"] { justify-content: flex-start; }
      .rift-chat-msg[data-role="user"] {
        background: rgba(254,143,33,0.18);
        border: 1px solid rgba(254,143,33,0.45);
        color: #fff;
      }
      .rift-chat-msg[data-role="admin"] {
        background: rgba(255,255,255,0.06);
        border: 1px solid rgba(255,255,255,0.12);
        color: #f5f5f5;
      }
      .rift-chat-msg-ts {
        font-size: 10px;
        opacity: 0.55;
        margin-top: 4px;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
      }

      .rift-chat-empty {
        flex: 1;
        display: flex;
        flex-direction: column;
        gap: 12px;
        justify-content: center;
        align-items: stretch;
        padding: 16px 8px;
      }
      .rift-chat-empty-title {
        text-align: center;
        font-size: 13px;
        color: rgba(255,255,255,0.65);
        margin-bottom: 4px;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
      }
      .rift-chat-suggestion {
        text-align: left;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.12);
        color: rgba(255,255,255,0.75);
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
        font-size: 12px;
        padding: 10px 12px;
        border-radius: 10px;
        cursor: pointer;
        transition: background 0.15s ease, border-color 0.15s ease, color 0.15s ease;
      }
      .rift-chat-suggestion:hover {
        background: rgba(254,143,33,0.08);
        border-color: rgba(254,143,33,0.55);
        color: #fff;
      }

      .rift-chat-input-row {
        display: flex;
        gap: 8px;
        padding: 12px;
        border-top: 1px solid rgba(255,255,255,0.08);
        background: rgba(0,0,0,0.35);
      }
      .rift-chat-input {
        flex: 1;
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.12);
        border-radius: 10px;
        padding: 10px 12px;
        color: #fff;
        font-size: 13px;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
        outline: none;
        transition: border-color 0.15s ease;
      }
      .rift-chat-input::placeholder { color: rgba(255,255,255,0.4); }
      .rift-chat-input:focus { border-color: ${ACCENT}; }

      .rift-chat-send {
        background: ${ACCENT};
        color: #1a0d00;
        border: none;
        border-radius: 10px;
        padding: 0 16px;
        font-weight: 700;
        cursor: pointer;
        display: flex;
        align-items: center;
        justify-content: center;
        transition: opacity 0.15s ease;
      }
      .rift-chat-send:disabled { opacity: 0.45; cursor: not-allowed; }
      .rift-chat-send:not(:disabled):hover { opacity: 0.88; }

      .rift-chat-status {
        text-align: center;
        font-size: 12px;
        color: rgba(255,255,255,0.5);
        padding: 12px;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
      }

      .rift-chat-awaiting {
        align-self: center;
        max-width: 90%;
        margin-top: 4px;
        padding: 10px 14px;
        background: rgba(255,255,255,0.04);
        border: 1px dashed rgba(255,255,255,0.18);
        border-radius: 10px;
        font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace;
        font-size: 11.5px;
        line-height: 1.5;
        color: rgba(255,255,255,0.6);
        text-align: center;
      }

      @media (max-width: 480px) {
        .rift-chat-root { bottom: 16px; right: 16px; }
        .rift-chat-panel { width: calc(100vw - 32px); right: 0; }
      }
    `;
    document.head.appendChild(style);
  }

  // --------------------------------------------------------------------------
  // Widget instance (single global).
  // --------------------------------------------------------------------------
  const widget = {
    rootEl: null,
    buttonEl: null,
    badgeEl: null,
    panelEl: null,
    messagesEl: null,
    inputEl: null,
    sendBtnEl: null,
    isOpen: false,
    isSending: false,
    currentChatId: null,
    currentThread: null,
    unreadCount: 0,
    pollHandle: null,
    visitorId: null,
  };

  function mountWidget() {
    if (document.getElementById("rift-chat-root")) {
      widget.rootEl = document.getElementById("rift-chat-root");
      return;
    }

    injectStyles();
    widget.visitorId = getVisitorId();

    const root = document.createElement("div");
    root.id = "rift-chat-root";
    root.className = "rift-chat-root";

    // Button
    const button = document.createElement("button");
    button.type = "button";
    button.className = "rift-chat-button";
    button.setAttribute("aria-label", "Open feedback chat");
    button.innerHTML = `
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
        <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z"/>
      </svg>
      <span>Chat with team</span>
      <span class="rift-chat-badge" data-count="0">0</span>
    `;
    button.addEventListener("click", openPanel);

    // Panel
    const panel = document.createElement("div");
    panel.className = "rift-chat-panel";
    panel.setAttribute("data-open", "false");
    panel.innerHTML = `
      <div class="rift-chat-header">
        <div class="rift-chat-header-title">Feedback Chat</div>
        <button type="button" class="rift-chat-close" aria-label="Close">×</button>
      </div>
      <div class="rift-chat-messages"></div>
      <div class="rift-chat-input-row">
        <input class="rift-chat-input" type="text" placeholder="Type your feedback..." autocomplete="off" />
        <button type="button" class="rift-chat-send" aria-label="Send">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
            <line x1="22" y1="2" x2="11" y2="13"></line>
            <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
          </svg>
        </button>
      </div>
    `;

    root.appendChild(panel);
    root.appendChild(button);
    document.body.appendChild(root);

    widget.rootEl = root;
    widget.buttonEl = button;
    widget.badgeEl = button.querySelector(".rift-chat-badge");
    widget.panelEl = panel;
    widget.messagesEl = panel.querySelector(".rift-chat-messages");
    widget.inputEl = panel.querySelector(".rift-chat-input");
    widget.sendBtnEl = panel.querySelector(".rift-chat-send");

    panel
      .querySelector(".rift-chat-close")
      .addEventListener("click", closePanel);
    widget.inputEl.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
      }
    });
    widget.sendBtnEl.addEventListener("click", sendMessage);

    renderEmptyState();
    startPolling();
  }

  // --------------------------------------------------------------------------
  // Render helpers
  // --------------------------------------------------------------------------
  function setBadge(n) {
    widget.unreadCount = n;
    if (!widget.badgeEl) return;
    widget.badgeEl.setAttribute("data-count", String(n));
    widget.badgeEl.textContent = n > 9 ? "9+" : String(n);
  }

  function renderStatus(text) {
    widget.messagesEl.innerHTML = "";
    const div = document.createElement("div");
    div.className = "rift-chat-status";
    div.textContent = text;
    widget.messagesEl.appendChild(div);
  }

  function renderEmptyState() {
    widget.messagesEl.innerHTML = "";
    const wrap = document.createElement("div");
    wrap.className = "rift-chat-empty";

    const title = document.createElement("div");
    title.className = "rift-chat-empty-title";
    title.textContent = "Start a conversation with Rift";
    wrap.appendChild(title);

    STARTER_SUGGESTIONS.forEach((s) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "rift-chat-suggestion";
      btn.textContent = s;
      btn.addEventListener("click", () => {
        widget.inputEl.value = s;
        widget.inputEl.focus();
      });
      wrap.appendChild(btn);
    });

    widget.messagesEl.appendChild(wrap);
  }

  function renderThread(thread) {
    widget.currentThread = thread;
    const msgs = (thread && thread.messages) || [];
    if (msgs.length === 0) {
      renderEmptyState();
      return;
    }

    widget.messagesEl.innerHTML = "";
    msgs.forEach((msg) => {
      const row = document.createElement("div");
      row.className = "rift-chat-msg-row";
      row.setAttribute("data-role", msg.role);

      const bubble = document.createElement("div");
      bubble.className = "rift-chat-msg";
      bubble.setAttribute("data-role", msg.role);

      const text = document.createElement("div");
      text.textContent = msg.message;
      bubble.appendChild(text);

      const ts = document.createElement("div");
      ts.className = "rift-chat-msg-ts";
      try {
        ts.textContent = new Date(msg.ts).toLocaleTimeString();
      } catch (_) {
        ts.textContent = "";
      }
      bubble.appendChild(ts);

      row.appendChild(bubble);
      widget.messagesEl.appendChild(row);
    });

    const hasUserMsg = msgs.some((m) => m.role === "user");
    const hasAdminMsg = msgs.some((m) => m.role === "admin");
    if (hasUserMsg && !hasAdminMsg) {
      const note = document.createElement("div");
      note.className = "rift-chat-awaiting";
      note.textContent =
        "The Rift team will get back to you shortly — check back soon.";
      widget.messagesEl.appendChild(note);
    }

    widget.messagesEl.scrollTop = widget.messagesEl.scrollHeight;
  }

  // --------------------------------------------------------------------------
  // Chat actions
  // --------------------------------------------------------------------------
  async function ensureChatId() {
    if (widget.currentChatId) return widget.currentChatId;
    const list = await apiListChats(widget.visitorId).catch(() => []);
    if (Array.isArray(list) && list.length > 0) {
      widget.currentChatId = list[0].id;
      return widget.currentChatId;
    }
    const id = await apiCreateChat(widget.visitorId, {
      source: "docs_feedback_button",
      url: typeof location !== "undefined" ? location.href : "",
      ua: typeof navigator !== "undefined" ? navigator.userAgent : "",
    });
    widget.currentChatId = id;
    return id;
  }

  async function loadAndRender() {
    try {
      renderStatus("Loading…");
      const list = await apiListChats(widget.visitorId);
      if (!Array.isArray(list) || list.length === 0) {
        widget.currentChatId = null;
        renderEmptyState();
        return;
      }
      widget.currentChatId = list[0].id;
      const thread = await apiGetThread(widget.currentChatId, widget.visitorId);
      renderThread(thread);

      // Mark read after viewing.
      try {
        await apiMarkRead(widget.currentChatId, widget.visitorId);
      } catch (_) {}
      refreshUnread().catch(() => {});
    } catch (err) {
      console.error("[rift-chat] loadAndRender failed:", err);
      renderStatus("Couldn't load chat. Try again later.");
    }
  }

  async function sendMessage() {
    const text = (widget.inputEl.value || "").trim();
    if (!text || widget.isSending) return;
    widget.isSending = true;
    widget.sendBtnEl.disabled = true;
    try {
      const chatId = await ensureChatId();
      await apiAppendMessage(chatId, widget.visitorId, "user", text);
      widget.inputEl.value = "";
      const thread = await apiGetThread(chatId, widget.visitorId);
      renderThread(thread);
    } catch (err) {
      console.error("[rift-chat] sendMessage failed:", err);
      alert("Failed to send message. Please try again.");
    } finally {
      widget.isSending = false;
      widget.sendBtnEl.disabled = false;
      widget.inputEl.focus();
    }
  }

  async function refreshUnread() {
    try {
      const list = await apiListChats(widget.visitorId);
      if (!Array.isArray(list)) return;
      const total = list.reduce(
        (sum, c) => sum + (c.user_unread_count || 0),
        0,
      );
      setBadge(total);

      if (widget.isOpen && widget.currentChatId) {
        const thread = await apiGetThread(
          widget.currentChatId,
          widget.visitorId,
        );
        renderThread(thread);
      }
    } catch (err) {
      // Silent in poll loop.
    }
  }

  function startPolling() {
    if (widget.pollHandle) return;
    refreshUnread();
    widget.pollHandle = setInterval(refreshUnread, POLL_INTERVAL_MS);
  }

  // --------------------------------------------------------------------------
  // Open / close
  // --------------------------------------------------------------------------
  function openPanel() {
    if (!widget.panelEl) return;
    widget.isOpen = true;
    widget.panelEl.setAttribute("data-open", "true");
    loadAndRender();
    setTimeout(() => widget.inputEl && widget.inputEl.focus(), 50);
  }

  async function closePanel() {
    if (!widget.panelEl) return;
    widget.isOpen = false;
    widget.panelEl.setAttribute("data-open", "false");
    if (widget.currentChatId) {
      try {
        await apiMarkRead(widget.currentChatId, widget.visitorId);
      } catch (_) {}
      refreshUnread().catch(() => {});
    }
  }

  // Expose for navbar button + other entry points.
  window.RiftChat = {
    open: openPanel,
    close: closePanel,
    toggle: function () {
      if (widget.isOpen) closePanel();
      else openPanel();
    },
    visitorId: function () {
      return widget.visitorId;
    },
  };

  // --------------------------------------------------------------------------
  // Navbar "Chat with team" hijack
  // ----------------------------------------------------------------------------
  // Mintlify's docs.json validates navbar.primary.href as a real URL/path, so
  // it points at "/?chat=open" (see docs.json). We intercept clicks via
  // delegation so SPA navigations don't lose the binding, AND we open the
  // panel on initial load if the URL carries the same flag.
  // --------------------------------------------------------------------------
  function isChatOpenHref(href) {
    if (!href) return false;
    try {
      const u = new URL(href, window.location.href);
      return u.searchParams.get("chat") === "open";
    } catch (_) {
      return href.indexOf("chat=open") !== -1;
    }
  }

  document.addEventListener(
    "click",
    function (e) {
      const target = e.target;
      if (!target || !(target instanceof Element)) return;
      const anchor = target.closest("a[href]");
      if (!anchor) return;
      if (!isChatOpenHref(anchor.getAttribute("href"))) return;
      e.preventDefault();
      e.stopPropagation();
      openPanel();
    },
    true,
  );

  function maybeOpenFromUrl() {
    try {
      const params = new URLSearchParams(window.location.search);
      if (params.get("chat") === "open") {
        openPanel();
        params.delete("chat");
        const newQs = params.toString();
        const newUrl =
          window.location.pathname +
          (newQs ? "?" + newQs : "") +
          window.location.hash;
        window.history.replaceState({}, "", newUrl);
      }
    } catch (_) {}
  }

  // --------------------------------------------------------------------------
  // Mount + keep mounted across SPA route changes
  // --------------------------------------------------------------------------
  function ensureMounted() {
    if (!document.body) return;
    if (!document.getElementById("rift-chat-root")) {
      mountWidget();
      maybeOpenFromUrl();
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", ensureMounted);
  } else {
    ensureMounted();
  }

  const observer = new MutationObserver(() => {
    removePoweredBy();
    ensureMounted();
  });
  if (document.body) {
    observer.observe(document.body, { childList: true, subtree: true });
  } else {
    document.addEventListener("DOMContentLoaded", () => {
      observer.observe(document.body, { childList: true, subtree: true });
    });
  }
})();

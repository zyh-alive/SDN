/**
 * api.js — REST/WS 数据层封装 (防崩溃版)
 *
 * 功能:
 *   - 自动从 sessionStorage 读取 token 并附加到请求头
 *   - 封装 fetch() 为统一的 get() / post() 方法 (内置 try-catch)
 *   - WebSocket 连接管理 (自动重连, Socket.IO 缺失时 fallback)
 *   - 发出自定义事件通知其他模块
 */
(function () {
    'use strict';

    // 如果 Token 不存在 (未登录) → 直接跳转，不加载后续模块
    if (!sessionStorage.getItem('sdn_token')) {
        window.location.href = '/login.html';
        throw new Error('No auth token — redirect to login');
    }

    const BASE = '';
    let socket = null;
    let reconnectTimer = null;
    let reconnectAttempts = 0;
    const MAX_RECONNECT_DELAY = 30000;
    let wsAvailable = false;

    // ── Token ──────────────────────────────────────────
    function getToken() {
        return sessionStorage.getItem('sdn_token') || '';
    }

    function getAuthHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        const token = getToken();
        if (token) {
            headers['Authorization'] = 'Bearer ' + token;
        }
        return headers;
    }

    // ── REST (自带容错) ────────────────────────────────
    async function get(url) {
        try {
            const resp = await fetch(BASE + url, {
                method: 'GET',
                headers: getAuthHeaders(),
            });
            // 401 → token 过期，回登录页
            if (resp.status === 401) {
                console.warn('[API] 401 Unauthorized — redirect to login');
                sessionStorage.clear();
                window.location.href = '/login.html';
                return { ok: false, error: 'Unauthorized' };
            }
            return await resp.json();
        } catch (e) {
            console.error('[API] GET', url, 'failed:', e.message);
            return { ok: false, error: e.message };
        }
    }

    async function post(url, body) {
        try {
            const resp = await fetch(BASE + url, {
                method: 'POST',
                headers: getAuthHeaders(),
                body: JSON.stringify(body),
            });
            if (resp.status === 401) {
                console.warn('[API] 401 Unauthorized — redirect to login');
                sessionStorage.clear();
                window.location.href = '/login.html';
                return { ok: false, error: 'Unauthorized' };
            }
            return await resp.json();
        } catch (e) {
            console.error('[API] POST', url, 'failed:', e.message);
            return { ok: false, error: e.message };
        }
    }

    // ── WebSocket (带 Socket.IO 缺失 fallback) ────────
    function connectWS() {
        if (socket && socket.connected) return;

        // Socket.IO CDN 是否存在?
        if (typeof io === 'undefined') {
            console.warn('[WS] Socket.IO not loaded (CDN unavailable) — WebSocket disabled');
            wsAvailable = false;
            window.dispatchEvent(new CustomEvent('ws:unavailable'));
            return;
        }

        try {
            socket = io({
                path: '/socket.io',
                transports: ['websocket', 'polling'],
                reconnection: false,
            });

            socket.on('connect', function () {
                console.log('[WS] Connected');
                wsAvailable = true;
                reconnectAttempts = 0;
                window.dispatchEvent(new CustomEvent('ws:connected'));
            });

            socket.on('disconnect', function (reason) {
                console.log('[WS] Disconnected:', reason);
                window.dispatchEvent(new CustomEvent('ws:disconnected'));
                scheduleReconnect();
            });

            socket.on('connect_error', function (err) {
                console.log('[WS] Connect error:', err.message);
                scheduleReconnect();
            });

            socket.on('topology_change', function (data) {
                window.dispatchEvent(new CustomEvent('ws:topology_change', { detail: data }));
            });

            socket.on('performance_update', function (data) {
                window.dispatchEvent(new CustomEvent('ws:perf_update', { detail: data }));
            });

            socket.on('classification_update', function (data) {
                window.dispatchEvent(new CustomEvent('ws:class_update', { detail: data }));
            });
        } catch (e) {
            console.error('[WS] Init failed:', e.message);
            wsAvailable = false;
            window.dispatchEvent(new CustomEvent('ws:unavailable'));
        }
    }

    function scheduleReconnect() {
        if (reconnectTimer) clearTimeout(reconnectTimer);
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), MAX_RECONNECT_DELAY);
        reconnectAttempts++;
        console.log('[WS] Reconnecting in', delay, 'ms (attempt', reconnectAttempts, ')');
        reconnectTimer = setTimeout(connectWS, delay);
    }

    function disconnectWS() {
        if (reconnectTimer) clearTimeout(reconnectTimer);
        if (socket) {
            try { socket.disconnect(); } catch (e) { /* ignore */ }
            socket = null;
        }
    }

    function isWsAvailable() {
        return wsAvailable;
    }

    // ── Global ─────────────────────────────────────────
    window.API = {
        get: get,
        post: post,
        getToken: getToken,
        connectWS: connectWS,
        disconnectWS: disconnectWS,
        isWsAvailable: isWsAvailable,
    };
})();

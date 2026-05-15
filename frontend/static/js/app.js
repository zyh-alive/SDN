/**
 * app.js — SPA 路由 + 全局状态 + WebSocket + 登出 [防崩溃版]
 *
 * 依赖: window.API (必须最先加载)
 */
(function () {
    'use strict';

    // ═══ Token 验证 ═══
    try {
        var token = sessionStorage.getItem('sdn_token');
        if (!token) {
            window.location.href = '/login.html';
            return;
        }
    } catch (e) {
        window.location.href = '/login.html';
        return;
    }

    // 显示用户名
    try {
        var usernameEl = document.getElementById('username-display');
        if (usernameEl) usernameEl.textContent = sessionStorage.getItem('sdn_username') || 'zyh';
    } catch (e) { /* ignore */ }

    // ═══ Tab 路由 ═══
    var currentTab = 'dashboard';

    function switchTab(tabName) {
        try {
            currentTab = tabName;
            document.querySelectorAll('.tab-btn').forEach(function (btn) {
                btn.classList.toggle('active', btn.dataset.tab === tabName);
            });
            document.querySelectorAll('.tab-panel').forEach(function (panel) {
                panel.classList.toggle('active', panel.id === 'tab-' + tabName);
            });

            // 按需加载
            if (tabName === 'performance' && window.Performance) window.Performance.load();
            if (tabName === 'history' && window.History) window.History.load();
            if (tabName === 'flows' && window.Flows) window.Flows.load();
            if (tabName === 'classification' && window.Classification) window.Classification.load();
            if (tabName === 'dashboard') {
                if (window.Topology) window.Topology.load();
                if (window.Classification) window.Classification.load();
            }

            setTimeout(function () { window.dispatchEvent(new Event('resize')); }, 150);
        } catch (e) { /* ignore */ }
    }

    try {
        document.querySelectorAll('.tab-btn').forEach(function (btn) {
            btn.addEventListener('click', function () { switchTab(btn.dataset.tab); });
        });
    } catch (e) { /* ignore */ }

    // ═══ 系统状态 ═══
    function updateSystemStatus() {
        if (!window.API) return;
        window.API.get('/api/status').then(function (resp) {
            try {
                var redisEl = document.getElementById('footer-redis');
                var mysqlEl = document.getElementById('footer-mysql');
                var sysStatus = document.getElementById('sys-status');
                var sysDot = sysStatus ? sysStatus.querySelector('.status-dot') : null;

                if (resp && resp.ok && resp.data) {
                    if (redisEl) { redisEl.textContent = resp.data.redis ? '✓' : '✗'; redisEl.style.color = resp.data.redis ? '#00ff41' : '#ff5252'; }
                    if (mysqlEl) { mysqlEl.textContent = resp.data.mysql ? '✓' : '✗'; mysqlEl.style.color = resp.data.mysql ? '#00ff41' : '#ff5252'; }
                    var allOk = resp.data.redis && resp.data.mysql;
                    if (sysStatus && sysStatus.childNodes[1]) sysStatus.childNodes[1].textContent = allOk ? ' 系统正常' : ' 部分异常';
                    if (sysDot) sysDot.className = 'status-dot ' + (allOk ? '' : 'warning');
                } else {
                    if (redisEl) { redisEl.textContent = '✗'; redisEl.style.color = '#ff5252'; }
                    if (mysqlEl) { mysqlEl.textContent = '✗'; mysqlEl.style.color = '#ff5252'; }
                    if (sysDot) sysDot.className = 'status-dot offline';
                }
            } catch (e) { /* ignore */ }
        }).catch(function () {
            try {
                var sysDot2 = document.querySelector('#sys-status .status-dot');
                if (sysDot2) sysDot2.className = 'status-dot offline';
            } catch (e2) { /* ignore */ }
        });
    }

    function updateClock() {
        try {
            var clockEl = document.getElementById('footer-clock');
            if (clockEl) clockEl.textContent = new Date().toLocaleTimeString('zh-CN');
        } catch (e) { /* ignore */ }
    }

    function onWSConnected() {
        try {
            var wsEl = document.getElementById('footer-ws');
            if (wsEl) { wsEl.textContent = '已连接'; wsEl.style.color = '#00ff41'; }
        } catch (e) { /* ignore */ }
    }

    function onWSDisconnected() {
        try {
            var wsEl = document.getElementById('footer-ws');
            if (wsEl) { wsEl.textContent = '断开'; wsEl.style.color = '#ff5252'; }
        } catch (e) { /* ignore */ }
    }

    function onWSUnavailable() {
        try {
            var wsEl = document.getElementById('footer-ws');
            if (wsEl) { wsEl.textContent = '不可用'; wsEl.style.color = '#6a7a8a'; }
        } catch (e) { /* ignore */ }
    }

    try {
        window.addEventListener('ws:connected', onWSConnected);
        window.addEventListener('ws:disconnected', onWSDisconnected);
        window.addEventListener('ws:unavailable', onWSUnavailable);
    } catch (e) { /* ignore */ }

    // ═══ 登出 ═══
    try {
        var btnLogout = document.getElementById('btn-logout');
        if (btnLogout) {
            btnLogout.addEventListener('click', function () {
                try {
                    if (window.API) window.API.disconnectWS();
                    sessionStorage.clear();
                } catch (e) { /* ignore */ }
                window.location.href = '/login.html';
            });
        }
    } catch (e) { /* ignore */ }

    // ═══ 报警 ═══
    function updateAlertCount(event) {
        try {
            var data = event && event.detail;
            if (!data || !data.data) return;
            var alertCount = 0;
            data.data.forEach(function (item) { if ((item.congestion_level || 0) >= 2) alertCount++; });
            var alertEl = document.getElementById('card-alerts');
            if (alertEl) {
                alertEl.textContent = alertCount > 0 ? '⚠ ' + alertCount + ' 条链路' : '0';
                alertEl.className = 'card-value' + (alertCount > 0 ? ' alert' : '');
            }
        } catch (e) { /* ignore */ }
    }

    try { window.addEventListener('ws:perf_update', updateAlertCount); } catch (e) { /* ignore */ }

    // ═══ 初始化 ═══
    function init() {
        try {
            // WS 连接
            if (window.API) window.API.connectWS();

            // 首页看板
            if (window.Topology) window.Topology.load();
            if (window.Classification) window.Classification.load();

            updateSystemStatus();
            updateClock();
            setInterval(updateSystemStatus, 30000);
            setInterval(updateClock, 1000);
        } catch (e) {
            console.error('[App] init failed:', e.message);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();

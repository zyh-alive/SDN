/**
 * flows.js — 流表管理页面 [防崩溃版]
 */
(function () {
    'use strict';

    function load() {
        var tbody = document.getElementById('flows-tbody');
        var statusEl = document.getElementById('flows-status');
        if (!tbody) return;

        if (!window.API) {
            if (statusEl) statusEl.innerHTML = '<span class="status-dot offline"></span> API 模块未加载';
            return;
        }

        window.API.get('/api/topology').then(function (resp) {
            try {
                if (!resp || !resp.ok || !resp.data || !resp.data.nodes) {
                    if (statusEl) statusEl.innerHTML = '<span class="status-dot warning"></span> 无法获取交换机列表 — 确认控制器已在运行';
                    return;
                }
                var switches = resp.data.nodes.filter(function (n) { return n.type === 'switch'; });
                if (switches.length === 0) {
                    if (statusEl) statusEl.innerHTML = '<span class="status-dot offline"></span> 未发现交换机';
                    return;
                }
                if (statusEl) {
                    statusEl.innerHTML = '<span class="status-dot" style="background:#00e5ff;box-shadow:0 0 6px #00e5ff;"></span> ' +
                        '检测到 ' + switches.length + ' 台交换机 — 流表请通过命令行查看: <code style="color:#00ff41">sudo ovs-ofctl dump-flows s1</code>';
                }
                tbody.innerHTML = switches.map(function (sw) {
                    return '<tr>' +
                        '<td>' + (sw.label || sw.id) + '</td>' +
                        '<td>-</td><td>-</td>' +
                        '<td colspan="3" style="color:#6a7a8a;">通过 OVS CLI 查看: ovs-ofctl dump-flows ' + (sw.label || sw.id) + '</td>' +
                        '</tr>';
                }).join('') || '<tr><td colspan="6" class="empty-row">暂无数据</td></tr>';
            } catch (e) { /* ignore */ }
        }).catch(function () {
            if (statusEl) statusEl.innerHTML = '<span class="status-dot offline"></span> API 请求失败';
        });
    }

    window.Flows = { load: load };
})();

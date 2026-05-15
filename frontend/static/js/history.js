/**
 * history.js — 历史记录页面 [防崩溃版]
 *
 * 依赖: ECharts 5, window.API
 */
(function () {
    'use strict';

    var perfHistoryChart = null;

    function isEChartsReady() {
        return typeof echarts !== 'undefined';
    }

    function escapeHtml(str) {
        return String(str || '').replace(/&/g, '&').replace(/</g, '<').replace(/>/g, '>');
    }

    function loadTopoHistory() {
        var container = document.getElementById('topo-timeline');
        if (!container || !window.API) return;
        window.API.get('/api/topology/history?limit=50').then(function (resp) {
            try {
                if (!resp || !resp.ok || !resp.data || resp.data.length === 0) {
                    container.innerHTML = '<div class="timeline-empty">暂无拓扑变更记录 (等待控制器...)</div>';
                    return;
                }
                var html = resp.data.map(function (item) {
                    var opClass = item.operation || 'ADD';
                    var opText = opClass === 'ADD' ? '添加' : opClass === 'DELETE' ? '删除' : '变更';
                    var timeStr = item.timestamp ? new Date(item.timestamp).toLocaleString('zh-CN') : '-';
                    var desc = (item.src_device || '?') + ':' + (item.src_port || '?') + ' ↔ ' + (item.dst_device || '?') + ':' + (item.dst_port || '?');
                    return '<div class="timeline-item">' +
                        '<div class="timeline-dot ' + opClass + '"></div>' +
                        '<div class="timeline-body">' +
                            '<div class="timeline-op ' + opClass + '">' + opText + '</div>' +
                            '<div class="timeline-desc">' + escapeHtml(desc) + '</div>' +
                            '<div class="timeline-time">' + timeStr + ' · v' + (item.topology_version || '?') + '</div>' +
                        '</div>' +
                        '</div>';
                }).join('');
                container.innerHTML = html;
            } catch (e) { /* ignore */ }
        }).catch(function () { /* ignore */ });
    }

    function initPerfHistoryChart() {
        if (!isEChartsReady()) { console.warn('[History] ECharts not loaded'); return; }
        try {
            var el = document.getElementById('chart-perf-history');
            if (!el) return;
            perfHistoryChart = echarts.init(el, null, { renderer: 'canvas' });
            perfHistoryChart.setOption({
                backgroundColor: 'transparent',
                title: { text: '请选择链路', left: 'center', top: 'center', textStyle: { color: '#6a7a8a', fontSize: 13 } },
                grid: { left: 70, right: 40, top: 20, bottom: 30 },
                legend: { bottom: 4, textStyle: { color: '#6a7a8a', fontSize: 10 } },
                xAxis: { type: 'time', axisLabel: { color: '#6a7a8a', fontSize: 10 }, splitLine: { show: false } },
                yAxis: [
                    { type: 'value', name: 'bps / ms', nameTextStyle: { color: '#6a7a8a' }, axisLabel: { color: '#6a7a8a' }, splitLine: { lineStyle: { color: 'rgba(26,42,60,0.4)' } } },
                    { type: 'value', name: '%', nameTextStyle: { color: '#6a7a8a' }, axisLabel: { color: '#6a7a8a' }, splitLine: { show: false } },
                ],
                series: [
                    { name: '吞吐量', type: 'line', data: [], smooth: true, symbol: 'none', lineStyle: { color: '#00e5ff', width: 1.5 } },
                    { name: '时延', type: 'line', data: [], smooth: true, symbol: 'none', lineStyle: { color: '#ff9800', width: 1.5 } },
                    { name: '抖动', type: 'line', data: [], smooth: true, symbol: 'none', lineStyle: { color: '#ffc107', width: 1.5 } },
                    { name: '丢包率', type: 'line', yAxisIndex: 1, data: [], smooth: true, symbol: 'none', lineStyle: { color: '#ff5252', width: 1.5 } },
                ],
            });
        } catch (e) {
            console.error('[History] initPerfHistoryChart failed:', e.message);
        }
    }

    function loadPerfHistory(linkId, mins) {
        if (!perfHistoryChart || !linkId || !window.API) return;
        window.API.get('/api/performance/' + encodeURIComponent(linkId) + '?mins=' + mins).then(function (resp) {
            try {
                if (!resp || !resp.ok || !resp.data || resp.data.length === 0) {
                    perfHistoryChart.setOption({
                        title: { text: '无历史数据', left: 'center', top: 'center', textStyle: { color: '#6a7a8a', fontSize: 13 } },
                        series: [
                            { name: '吞吐量', data: [] }, { name: '时延', data: [] },
                            { name: '抖动', data: [] }, { name: '丢包率', data: [] },
                        ],
                    });
                    return;
                }
                var throughput = [], delay = [], jitter = [], loss = [];
                resp.data.forEach(function (p) {
                    var ts = p.timestamp ? new Date(p.timestamp).getTime() : Date.now();
                    throughput.push([ts, p.throughput || 0]);
                    delay.push([ts, p.delay || 0]);
                    jitter.push([ts, p.jitter || 0]);
                    loss.push([ts, (p.packet_loss || 0) * 100]);
                });
                perfHistoryChart.setOption({
                    title: { text: '' },
                    series: [
                        { name: '吞吐量', data: throughput }, { name: '时延', data: delay },
                        { name: '抖动', data: jitter }, { name: '丢包率', data: loss },
                    ],
                });
            } catch (e) { /* ignore */ }
        }).catch(function () { /* ignore */ });
    }

    function setupSelectors() {
        try {
            var linkSelect = document.getElementById('hist-link-select');
            var minsSelect = document.getElementById('hist-mins-select');

            if (window.API) {
                window.API.get('/api/performance').then(function (resp) {
                    if (!resp || !resp.ok || !resp.data) return;
                    if (linkSelect) {
                        linkSelect.innerHTML = '<option value="">选择链路...</option>' +
                            resp.data.map(function (l) {
                                return '<option value="' + l.link_id + '">' + l.link_id + ' (Lv.' + (l.congestion_level || 0) + ')</option>';
                            }).join('');
                    }
                }).catch(function () { /* ignore */ });
            }

            if (linkSelect && minsSelect) {
                function refresh() {
                    var linkId = linkSelect.value;
                    var mins = parseInt(minsSelect.value) || 60;
                    if (linkId) loadPerfHistory(linkId, mins);
                }
                linkSelect.addEventListener('change', refresh);
                minsSelect.addEventListener('change', refresh);
            }
        } catch (e) { /* ignore */ }
    }

    function loadClassHistoryTable() {
        var tbody = document.getElementById('hist-class-tbody');
        if (!tbody || !window.API) return;
        window.API.get('/api/classification/history?limit=30').then(function (resp) {
            try {
                if (!resp || !resp.ok || !resp.data || resp.data.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" class="empty-row">暂无分类记录 (等待控制器...)</td></tr>';
                    return;
                }
                tbody.innerHTML = resp.data.slice(0, 30).map(function (r) {
                    var classLabel = r.predicted_class || 'other';
                    return '<tr>' +
                        '<td style="font-size:10px">' + escapeHtml(r.flow_key || '-') + '</td>' +
                        '<td>' + escapeHtml(r.src_ip || '') + ':' + (r.src_port || '') + ' → ' + escapeHtml(r.dst_ip || '') + ':' + (r.dst_port || '') + '</td>' +
                        '<td><span class="class-badge class-' + classLabel + '">' + classLabel + '</span></td>' +
                        '<td>' + (r.confidence != null ? (r.confidence * 100).toFixed(1) + '%' : '-') + '</td>' +
                        '<td>' + (r.is_pseudo_label ? '是' : '否') + '</td>' +
                        '<td style="font-size:10px">' + (r.timestamp ? new Date(r.timestamp).toLocaleTimeString('zh-CN') : '-') + '</td>' +
                        '</tr>';
                }).join('');
            } catch (e) { /* ignore */ }
        }).catch(function () { /* ignore */ });
    }

    function load() {
        loadTopoHistory();
        loadClassHistoryTable();
    }

    initPerfHistoryChart();
    setupSelectors();
    window.addEventListener('resize', function () {
        try { if (perfHistoryChart) perfHistoryChart.resize(); } catch (e) { /* ignore */ }
    });

    window.History = { load: load };
})();

/**
 * performance.js — 性能监测页面（四指标实时折线图）[防崩溃版]
 *
 * 依赖: ECharts 5, window.API
 */
(function () {
    'use strict';

    var charts = {};
    var buffers = {};
    var MAX_POINTS = 120;
    var selectedLink = null;
    var ready = false;

    function isEChartsReady() {
        return typeof echarts !== 'undefined';
    }

    function initCharts() {
        if (!isEChartsReady()) { console.warn('[Perf] ECharts not loaded'); return; }
        try {
            var ids = ['chart-perf-throughput', 'chart-perf-delay', 'chart-perf-jitter', 'chart-perf-loss'];
            ids.forEach(function (id) {
                var el = document.getElementById(id);
                if (el) charts[id] = echarts.init(el, null, { renderer: 'canvas' });
            });
            initEmptyChart('chart-perf-throughput', '吞吐量 (bps)');
            initEmptyChart('chart-perf-delay', '时延 (ms)');
            initEmptyChart('chart-perf-jitter', '抖动 (ms)');
            initEmptyChart('chart-perf-loss', '丢包率 (%)');
            ready = true;
        } catch (e) {
            console.error('[Perf] initCharts failed:', e.message);
        }
    }

    function initEmptyChart(id, name) {
        var ch = charts[id];
        if (!ch) return;
        try {
            ch.setOption({
                backgroundColor: 'transparent',
                grid: { left: 60, right: 20, top: 20, bottom: 30 },
                xAxis: {
                    type: 'time',
                    axisLine: { lineStyle: { color: '#1a2a3c' } },
                    axisLabel: { color: '#6a7a8a', fontSize: 10 },
                    splitLine: { show: false },
                },
                yAxis: {
                    type: 'value', name: name,
                    nameTextStyle: { color: '#6a7a8a', fontSize: 10 },
                    axisLabel: { color: '#6a7a8a', fontSize: 10 },
                    splitLine: { lineStyle: { color: 'rgba(26, 42, 60, 0.4)' } },
                },
                series: [{
                    type: 'line', data: [], smooth: true, symbol: 'none',
                    lineStyle: { color: '#00e5ff', width: 1.5 },
                    areaStyle: { color: 'rgba(0, 229, 255, 0.05)' },
                }],
            });
        } catch (e) { /* ignore */ }
    }

    function updateChart(id, timestamps, values, yName, color) {
        var ch = charts[id];
        if (!ch || !timestamps || !values) return;
        try {
            ch.setOption({
                xAxis: {
                    type: 'time',
                    axisLine: { lineStyle: { color: '#1a2a3c' } },
                    axisLabel: { color: '#6a7a8a', fontSize: 10 },
                    splitLine: { show: false },
                },
                yAxis: {
                    type: 'value', name: yName,
                    nameTextStyle: { color: '#6a7a8a', fontSize: 10 },
                    axisLabel: { color: '#6a7a8a', fontSize: 10 },
                    splitLine: { lineStyle: { color: 'rgba(26, 42, 60, 0.4)' } },
                },
                series: [{
                    type: 'line',
                    data: timestamps.map(function (t, i) { return [t, values[i] || 0]; }),
                    smooth: true, symbol: 'none',
                    lineStyle: { color: color || '#00e5ff', width: 1.5 },
                }],
            });
        } catch (e) { /* ignore */ }
    }

    function buildLinkList(perfData) {
        var container = document.getElementById('link-list');
        if (!container) return;

        try {
            if (!perfData || perfData.length === 0) {
                container.innerHTML = '<div class="link-list-empty">暂无链路数据 (等待控制器...)</div>';
                return;
            }

            container.innerHTML = '';
            perfData.forEach(function (item) {
                var div = document.createElement('div');
                div.className = 'link-item';
                div.dataset.linkId = item.link_id;
                var dot = document.createElement('span');
                dot.className = 'link-level-dot level-' + (item.congestion_level || 0);
                div.appendChild(dot);
                div.appendChild(document.createTextNode(item.link_id));
                div.addEventListener('click', function () { selectLink(item.link_id); });
                container.appendChild(div);
            });

            if (perfData.length > 0) selectLink(perfData[0].link_id);
        } catch (e) {
            console.error('[Perf] buildLinkList failed:', e.message);
        }
    }

    function selectLink(linkId) {
        selectedLink = linkId;
        try {
            document.querySelectorAll('.link-item').forEach(function (el) {
                el.classList.toggle('active', el.dataset.linkId === linkId);
            });
        } catch (e) { /* ignore */ }
        loadLinkHistory(linkId);
        if (buffers[linkId]) displayBuffer(linkId);
    }

    function displayBuffer(linkId) {
        var buf = buffers[linkId];
        if (!buf || !buf.timestamps || buf.timestamps.length === 0) return;
        updateChart('chart-perf-throughput', buf.timestamps, buf.throughput, 'bps', '#00e5ff');
        updateChart('chart-perf-delay', buf.timestamps, buf.delay, 'ms', '#00e5ff');
        updateChart('chart-perf-jitter', buf.timestamps, buf.jitter, 'ms', '#00e5ff');
        updateChart('chart-perf-loss', buf.timestamps, buf.loss, '%', '#00e5ff');
    }

    function loadLinkHistory(linkId) {
        if (!window.API) return;
        window.API.get('/api/performance/' + encodeURIComponent(linkId) + '?mins=60').then(function (resp) {
            if (!resp || !resp.ok || !resp.data) return;
            try {
                var timestamps = [], thr = [], del = [], jit = [], los = [];
                resp.data.forEach(function (point) {
                    var ts = point.timestamp ? new Date(point.timestamp).getTime() : Date.now();
                    timestamps.push(ts);
                    thr.push(point.throughput || 0);
                    del.push(point.delay || 0);
                    jit.push(point.jitter || 0);
                    los.push((point.packet_loss || 0) * 100);
                });
                buffers[linkId] = { timestamps: timestamps, throughput: thr, delay: del, jitter: jit, loss: los };
                displayBuffer(linkId);
            } catch (e) { /* ignore */ }
        }).catch(function () { /* ignore */ });
    }

    function onPerfUpdate(event) {
        try {
            var data = event && event.detail;
            if (!data || !data.data) return;
            buildLinkList(data.data);

            var now = Date.now();
            data.data.forEach(function (item) {
                var lid = item.link_id;
                if (!buffers[lid]) buffers[lid] = { timestamps: [], throughput: [], delay: [], jitter: [], loss: [] };
                var buf = buffers[lid];
                buf.timestamps.push(now);
                buf.throughput.push(item.throughput || 0);
                buf.delay.push(item.delay || 0);
                buf.jitter.push(item.jitter || 0);
                buf.loss.push((item.packet_loss || 0) * 100);
                if (buf.timestamps.length > MAX_POINTS) {
                    buf.timestamps = buf.timestamps.slice(-MAX_POINTS);
                    buf.throughput = buf.throughput.slice(-MAX_POINTS);
                    buf.delay = buf.delay.slice(-MAX_POINTS);
                    buf.jitter = buf.jitter.slice(-MAX_POINTS);
                    buf.loss = buf.loss.slice(-MAX_POINTS);
                }
            });
            if (selectedLink && buffers[selectedLink]) displayBuffer(selectedLink);
        } catch (e) { /* ignore */ }
    }

    function load() {
        if (!window.API) return;
        window.API.get('/api/performance').then(function (resp) {
            if (resp && resp.ok) buildLinkList(resp.data);
        }).catch(function () { /* ignore */ });
    }

    initCharts();
    try { window.addEventListener('ws:perf_update', onPerfUpdate); } catch (e) { /* ignore */ }
    window.addEventListener('resize', function () {
        try { Object.values(charts).forEach(function (c) { c.resize(); }); } catch (e) { /* ignore */ }
    });

    window.Performance = { load: load, onPerfUpdate: onPerfUpdate };
})();

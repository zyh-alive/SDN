/**
 * classification.js — 流量分类页面（饼图 + 统计 + 表格）[防崩溃版]
 *
 * 依赖: ECharts 5, window.API
 */
(function () {
    'use strict';

    var pieChart = null;
    var distChart = null;

    function isEChartsReady() {
        return typeof echarts !== 'undefined';
    }

    function initCharts() {
        if (!isEChartsReady()) { console.warn('[Class] ECharts not loaded'); return; }
        try {
            var el1 = document.getElementById('chart-class-pie');
            if (el1) pieChart = echarts.init(el1, null, { renderer: 'canvas' });
            var el2 = document.getElementById('chart-class-dist');
            if (el2) distChart = echarts.init(el2, null, { renderer: 'canvas' });
        } catch (e) {
            console.error('[Class] initCharts failed:', e.message);
        }
    }

    function renderPieChart(byClass, total) {
        if (!pieChart) return;
        try {
            if (!byClass || total === 0) {
                pieChart.setOption({
                    backgroundColor: 'transparent',
                    title: { text: '暂无分类数据', left: 'center', top: 'center', textStyle: { color: '#6a7a8a', fontSize: 13 } },
                });
                return;
            }
            var classColors = { realtime: '#ff5252', interactive: '#00ff41', streaming: '#00e5ff', bulk: '#ff9800', other: '#3a4a5a' };
            var data = Object.keys(byClass).map(function (k) {
                return { name: k, value: byClass[k], itemStyle: { color: classColors[k] || '#6a7a8a' } };
            });
            pieChart.setOption({
                backgroundColor: 'transparent',
                tooltip: { trigger: 'item', backgroundColor: 'rgba(15,22,30,0.95)', borderColor: '#1a2a3c', textStyle: { color: '#c8d6e5', fontSize: 12 } },
                legend: { bottom: 8, textStyle: { color: '#6a7a8a', fontSize: 11 } },
                series: [{ type: 'pie', radius: ['45%', '75%'], center: ['50%', '47%'], data: data, label: { color: '#6a7a8a', fontSize: 10 }, emphasis: { label: { fontSize: 16, fontWeight: 'bold' } } }],
            });
        } catch (e) { /* ignore */ }
    }

    function renderDistChart(stats) {
        if (!distChart) return;
        try {
            if (!stats || !stats.by_class || stats.total === 0) {
                distChart.setOption({
                    backgroundColor: 'transparent',
                    title: { text: '暂无分类数据', left: 'center', top: 'center', textStyle: { color: '#6a7a8a', fontSize: 13 } },
                });
                return;
            }
            var classColors = { realtime: '#ff5252', interactive: '#00ff41', streaming: '#00e5ff', bulk: '#ff9800', other: '#3a4a5a' };
            var categories = Object.keys(stats.by_class);
            var values = categories.map(function (k) { return stats.by_class[k]; });
            var colors = categories.map(function (k) { return classColors[k] || '#6a7a8a'; });
            distChart.setOption({
                backgroundColor: 'transparent',
                tooltip: { trigger: 'axis', backgroundColor: 'rgba(15,22,30,0.95)', borderColor: '#1a2a3c', textStyle: { color: '#c8d6e5' } },
                grid: { left: 60, right: 20, top: 20, bottom: 30 },
                xAxis: { type: 'category', data: categories, axisLabel: { color: '#6a7a8a', fontSize: 10, rotate: 30 }, axisLine: { lineStyle: { color: '#1a2a3c' } } },
                yAxis: { type: 'value', name: '流数', nameTextStyle: { color: '#6a7a8a' }, axisLabel: { color: '#6a7a8a' }, splitLine: { lineStyle: { color: 'rgba(26,42,60,0.4)' } } },
                series: [{ type: 'bar', data: values.map(function (v, i) { return { value: v, itemStyle: { color: colors[i] } }; }), barWidth: '50%' }],
            });
        } catch (e) { /* ignore */ }
    }

    function updateStats(stats) {
        try {
            var totalEl = document.getElementById('sum-total');
            var pseudoEl = document.getElementById('sum-pseudo');
            var byClassEl = document.getElementById('sum-by-class');

            if (totalEl) totalEl.textContent = (stats && stats.total) || 0;
            if (pseudoEl) pseudoEl.textContent = (((stats && stats.pseudo_ratio) || 0) * 100).toFixed(1) + '%';
            if (byClassEl && stats && stats.by_class) {
                var classColors = { realtime: '#ff5252', interactive: '#00ff41', streaming: '#00e5ff', bulk: '#ff9800', other: '#3a4a5a' };
                var html = '';
                Object.keys(stats.by_class).forEach(function (k) {
                    html += '<div class="summary-class-row"><span style="color:' + (classColors[k] || '#6a7a8a') + '">◆ ' + k + '</span><span>' + stats.by_class[k] + '</span></div>';
                });
                byClassEl.innerHTML = html;
            }
            if (stats) {
                renderPieChart(stats.by_class, stats.total);
                renderDistChart(stats);
            }
        } catch (e) { /* ignore */ }
    }

    function escapeHtml(str) {
        if (!str) return '';
        return String(str).replace(/&/g, '&').replace(/</g, '<').replace(/>/g, '>').replace(/"/g, '"');
    }

    function buildClassTable(data, filter) {
        var tbody = document.getElementById('class-tbody');
        if (!tbody) return;
        try {
            filter = filter || 'all';
            var filtered = filter === 'all' ? (data || []) : (data || []).filter(function (r) { return r.predicted_class === filter; });
            if (filtered.length === 0) {
                tbody.innerHTML = '<tr><td colspan="8" class="empty-row">暂无分类数据 (等待控制器...)</td></tr>';
                return;
            }
            tbody.innerHTML = filtered.slice(0, 50).map(function (r) {
                var classLabel = r.predicted_class || 'other';
                return '<tr>' +
                    '<td style="font-size:10px">' + escapeHtml(r.flow_key || '-') + '</td>' +
                    '<td>' + escapeHtml(r.src_ip || '') + ':' + (r.src_port || '') + ' → ' + escapeHtml(r.dst_ip || '') + ':' + (r.dst_port || '') + '</td>' +
                    '<td>' + (r.protocol === 6 ? 'TCP' : r.protocol === 17 ? 'UDP' : (r.protocol || '-')) + '</td>' +
                    '<td>' + (r.src_port || '') + '→' + (r.dst_port || '') + '</td>' +
                    '<td><span class="class-badge class-' + classLabel + '">' + classLabel + '</span></td>' +
                    '<td>' + (r.confidence != null ? (r.confidence * 100).toFixed(1) + '%' : '-') + '</td>' +
                    '<td>' + (r.is_pseudo_label ? '是' : '否') + '</td>' +
                    '<td style="font-size:10px">' + (r.timestamp ? new Date(r.timestamp).toLocaleTimeString('zh-CN') : '-') + '</td>' +
                    '</tr>';
            }).join('');
        } catch (e) { /* ignore */ }
    }

    function setupFilter() {
        try {
            var select = document.getElementById('class-filter');
            if (!select) return;
            select.addEventListener('change', function () {
                loadClassHistory(this.value);
            });
        } catch (e) { /* ignore */ }
    }

    function loadClassHistory(filter) {
        if (!window.API) return;
        window.API.get('/api/classification/history?limit=100').then(function (resp) {
            if (resp && resp.ok) buildClassTable(resp.data, filter);
        }).catch(function () { /* ignore */ });
    }

    function onClassUpdate(event) {
        try {
            var data = event && event.detail;
            if (!data) return;
            updateStats({ total: data.total_flows || 0, by_class: data.by_class || {}, pseudo_ratio: 0 });
            var flowsEl = document.getElementById('card-flows');
            if (flowsEl) flowsEl.textContent = data.total_flows || 0;
        } catch (e) { /* ignore */ }
    }

    function load() {
        if (!window.API) return;
        window.API.get('/api/classification/stats').then(function (resp) {
            if (resp && resp.ok) updateStats(resp.data);
        }).catch(function () { /* ignore */ });
        loadClassHistory('all');
    }

    initCharts();
    setupFilter();
    try { window.addEventListener('ws:class_update', onClassUpdate); } catch (e) { /* ignore */ }
    window.addEventListener('resize', function () {
        try { if (pieChart) pieChart.resize(); if (distChart) distChart.resize(); } catch (e) { /* ignore */ }
    });

    window.Classification = { load: load };
})();

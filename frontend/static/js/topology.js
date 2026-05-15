/**
 * topology.js — 拓扑图渲染 (ECharts 力导向图) [防崩溃版]
 *
 * 依赖: ECharts 5 CDN, window.API
 */
(function () {
    'use strict';

    let chart = null;
    let domEl = null;
    let ready = false;

    function isEChartsReady() {
        return typeof echarts !== 'undefined';
    }

    function init() {
        try {
            domEl = document.getElementById('chart-topology');
            if (!domEl) { console.warn('[Topology] DOM #chart-topology not found'); return; }
            if (!isEChartsReady()) { console.warn('[Topology] ECharts not loaded'); return; }

            chart = echarts.init(domEl, null, { renderer: 'canvas' });
            ready = true;

            window.addEventListener('resize', function () {
                if (chart) { try { chart.resize(); } catch (e) { /* ignore */ } }
            });

            document.querySelectorAll('.tab-btn').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    setTimeout(function () {
                        if (chart) { try { chart.resize(); } catch (e) { /* ignore */ } }
                    }, 150);
                });
            });
        } catch (e) {
            console.error('[Topology] init failed:', e.message);
        }
    }

    function render(graph) {
        if (!ready || !chart || !graph) return;

        try {
            var nodes = (graph.nodes || []).map(function (n) {
                var isSwitch = n.type === 'switch';
                return {
                    id: n.id,
                    name: n.label || n.id,
                    symbolSize: isSwitch ? 36 : 18,
                    itemStyle: {
                        color: isSwitch ? '#0f161e' : '#0a0e13',
                        borderColor: isSwitch ? '#00e5ff' : '#3a4a5a',
                        borderWidth: isSwitch ? 2 : 1,
                        shadowBlur: isSwitch ? 12 : 0,
                        shadowColor: 'rgba(0, 229, 255, 0.3)',
                    },
                    label: {
                        show: true,
                        color: '#c8d6e5',
                        fontSize: isSwitch ? 11 : 10,
                        fontFamily: 'Courier New, monospace',
                    },
                    category: isSwitch ? 0 : 1,
                };
            });

            var links = (graph.links || []).map(function (l) {
                var level = l.congestion_level || 0;
                var colors = ['#00ff41', '#ffc107', '#ff9800', '#ff5252'];
                var widths = [1.5, 2.5, 3.5, 5];
                return {
                    source: l.source,
                    target: l.target,
                    lineStyle: {
                        color: colors[level] || colors[0],
                        width: widths[level] || widths[0],
                        opacity: 0.7,
                        curveness: 0.15,
                    },
                    label: {
                        show: true,
                        formatter: (l.src_port || '?') + ' ↔ ' + (l.dst_port || '?'),
                        color: '#6a7a8a',
                        fontSize: 9,
                        fontFamily: 'Courier New, monospace',
                    },
                };
            });

            chart.setOption({
                backgroundColor: 'transparent',
                tooltip: {
                    trigger: 'item',
                    backgroundColor: 'rgba(15, 22, 30, 0.95)',
                    borderColor: '#1a2a3c',
                    textStyle: { color: '#c8d6e5', fontFamily: 'Courier New, monospace', fontSize: 12 },
                    formatter: function (params) {
                        if (params.dataType === 'edge') return params.data.label.formatter;
                        return params.name;
                    },
                },
                legend: {
                    show: true,
                    bottom: 8,
                    textStyle: { color: '#6a7a8a', fontSize: 11 },
                    data: ['交换机', '主机'],
                },
                series: [{
                    type: 'graph',
                    layout: 'force',
                    roam: true,
                    draggable: true,
                    force: { repulsion: 400, edgeLength: [180, 280], gravity: 0.15 },
                    categories: [{ name: '交换机' }, { name: '主机' }],
                    data: nodes,
                    links: links,
                    emphasis: { focus: 'adjacency', lineStyle: { width: 4 } },
                }],
            }, true);
        } catch (e) {
            console.error('[Topology] render failed:', e.message);
        }
    }

    function load() {
        if (!window.API) { console.warn('[Topology] API not ready'); return; }
        window.API.get('/api/topology').then(function (resp) {
            try {
                if (resp && resp.ok) {
                    render(resp.data);
                    var verEl = document.getElementById('topo-graph-version');
                    if (verEl && resp.data && resp.data.version) verEl.textContent = 'v' + resp.data.version;
                    var topoVerEl = document.getElementById('topo-version');
                    if (topoVerEl && resp.data && resp.data.version) topoVerEl.textContent = '拓扑 v' + resp.data.version;
                    updateOverviewCards(resp.data);
                }
            } catch (e) { /* ignore */ }
        }).catch(function () { /* ignore */ });
    }

    function updateOverviewCards(graph) {
        try {
            var switches = 0, links = 0;
            if (graph && graph.nodes) {
                graph.nodes.forEach(function (n) { if (n.type === 'switch') switches++; });
            }
            if (graph && graph.links) links = graph.links.length;
            var swEl = document.getElementById('card-switches');
            var lkEl = document.getElementById('card-links');
            if (swEl) swEl.textContent = switches;
            if (lkEl) lkEl.textContent = links;
        } catch (e) { /* ignore */ }
    }

    init();
    window.Topology = { load: load, render: render };
})();

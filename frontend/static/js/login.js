/**
 * login.js — 登录页面逻辑
 *
 * 功能:
 *   1. Canvas 矩阵雨动效（Matrix Rain）
 *   2. Canvas 粒子网格背景（Particle Grid）
 *   3. 终端光标闪烁（CSS 已处理）
 *   4. 按钮呼吸光（CSS 已处理）
 *   5. 表单提交 → POST /api/login → token 存储 → 跳转
 *   6. 错误震动 + 红色提示
 */

(function () {
    'use strict';

    // ════════════════════════════════════════════════════
    //  矩阵雨 (Matrix Rain) — Canvas
    // ════════════════════════════════════════════════════
    const matrixCanvas = document.getElementById('matrix-rain');
    const matrixCtx = matrixCanvas.getContext('2d');

    const MATRIX_CHARS = 'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン0123456789ABCDEF<>/{}[]|&^%$#@!';
    const FONT_SIZE = 14;
    let matrixColumns = [];
    let matrixDrops = [];

    function initMatrixRain() {
        matrixCanvas.width = window.innerWidth;
        matrixCanvas.height = window.innerHeight;
        matrixColumns = Math.floor(matrixCanvas.width / FONT_SIZE);
        matrixDrops = new Array(matrixColumns).fill(0);
    }

    function drawMatrixRain() {
        matrixCtx.fillStyle = 'rgba(10, 12, 15, 0.05)';
        matrixCtx.fillRect(0, 0, matrixCanvas.width, matrixCanvas.height);

        matrixCtx.fillStyle = '#00ff41';
        matrixCtx.font = FONT_SIZE + 'px "Courier New", monospace';

        for (let i = 0; i < matrixDrops.length; i++) {
            const char = MATRIX_CHARS[Math.floor(Math.random() * MATRIX_CHARS.length)];
            const x = i * FONT_SIZE;
            const y = matrixDrops[i] * FONT_SIZE;

            // 头部字符更亮
            matrixCtx.fillStyle = '#00ff41';
            matrixCtx.fillText(char, x, y);

            // 尾部渐隐
            if (matrixDrops[i] > 1 && Math.random() > 0.95) {
                matrixCtx.fillStyle = 'rgba(0, 255, 65, 0.3)';
                matrixCtx.fillText(
                    MATRIX_CHARS[Math.floor(Math.random() * MATRIX_CHARS.length)],
                    x,
                    y - FONT_SIZE
                );
            }

            if (y > matrixCanvas.height && Math.random() > 0.975) {
                matrixDrops[i] = 0;
            }
            matrixDrops[i]++;
        }
    }

    // ════════════════════════════════════════════════════
    //  粒子网格背景 (Particle Grid) — Canvas
    // ════════════════════════════════════════════════════
    const particleCanvas = document.getElementById('particle-grid');
    const particleCtx = particleCanvas.getContext('2d');

    const PARTICLE_COUNT = 60;
    const CONNECT_DIST = 120;
    let particles = [];

    function initParticles() {
        particleCanvas.width = window.innerWidth;
        particleCanvas.height = window.innerHeight;
        particles = [];

        for (let i = 0; i < PARTICLE_COUNT; i++) {
            particles.push({
                x: Math.random() * particleCanvas.width,
                y: Math.random() * particleCanvas.height,
                vx: (Math.random() - 0.5) * 0.4,
                vy: (Math.random() - 0.5) * 0.4,
                radius: Math.random() * 1.5 + 0.5,
            });
        }
    }

    function drawParticles() {
        particleCtx.clearRect(0, 0, particleCanvas.width, particleCanvas.height);

        // 更新位置
        for (const p of particles) {
            p.x += p.vx;
            p.y += p.vy;

            if (p.x < 0) p.x = particleCanvas.width;
            if (p.x > particleCanvas.width) p.x = 0;
            if (p.y < 0) p.y = particleCanvas.height;
            if (p.y > particleCanvas.height) p.y = 0;
        }

        // 绘制连线
        for (let i = 0; i < particles.length; i++) {
            for (let j = i + 1; j < particles.length; j++) {
                const dx = particles[i].x - particles[j].x;
                const dy = particles[i].y - particles[j].y;
                const dist = Math.sqrt(dx * dx + dy * dy);

                if (dist < CONNECT_DIST) {
                    const alpha = (1 - dist / CONNECT_DIST) * 0.15;
                    particleCtx.strokeStyle = `rgba(0, 229, 255, ${alpha})`;
                    particleCtx.lineWidth = 0.5;
                    particleCtx.beginPath();
                    particleCtx.moveTo(particles[i].x, particles[i].y);
                    particleCtx.lineTo(particles[j].x, particles[j].y);
                    particleCtx.stroke();
                }
            }
        }

        // 绘制粒子
        for (const p of particles) {
            particleCtx.fillStyle = 'rgba(0, 229, 255, 0.5)';
            particleCtx.beginPath();
            particleCtx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
            particleCtx.fill();
        }
    }

    // ════════════════════════════════════════════════════
    //  动画循环
    // ════════════════════════════════════════════════════
    function animate() {
        drawMatrixRain();
        drawParticles();
        requestAnimationFrame(animate);
    }

    // ════════════════════════════════════════════════════
    //  登录逻辑
    // ════════════════════════════════════════════════════
    const loginForm = document.getElementById('login-form');
    const errorMsg = document.getElementById('error-msg');
    const errorText = document.getElementById('error-text');
    const netStatus = document.getElementById('net-status');
    const btnConnect = loginForm.querySelector('.btn-connect');

    function showError(msg) {
        errorText.textContent = msg;
        errorMsg.classList.remove('hidden');
        // 重新触发 shake 动画
        errorMsg.style.animation = 'none';
        void errorMsg.offsetWidth; // 强制回流
        errorMsg.style.animation = 'shake 0.5s ease-in-out';
    }

    function hideError() {
        errorMsg.classList.add('hidden');
    }

    loginForm.addEventListener('submit', async function (e) {
        e.preventDefault();
        hideError();

        const username = document.getElementById('username').value.trim();
        const password = document.getElementById('password').value;

        if (!username || !password) {
            showError('请输入用户名和密码');
            return;
        }

        // 按钮 loading 状态
        btnConnect.classList.add('loading');
        btnConnect.disabled = true;

        try {
            const resp = await fetch('/api/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, password }),
            });

            const data = await resp.json();

            if (data.ok) {
                // 存储 token
                sessionStorage.setItem('sdn_token', data.token);
                sessionStorage.setItem('sdn_username', data.username);

                // 更新状态栏
                netStatus.textContent = '已连接';
                netStatus.classList.add('connected');

                // 延迟跳转，展示成功状态
                setTimeout(() => {
                    window.location.href = '/index.html';
                }, 600);
            } else {
                showError(data.error || '身份验证失败，拒绝访问');
                btnConnect.classList.remove('loading');
                btnConnect.disabled = false;
            }
        } catch (err) {
            showError('网络连接失败，无法访问认证服务');
            btnConnect.classList.remove('loading');
            btnConnect.disabled = false;
        }
    });

    // ════════════════════════════════════════════════════
    //  初始化
    // ════════════════════════════════════════════════════
    function init() {
        initMatrixRain();
        initParticles();
        animate();
    }

    // 窗口大小变化时重新初始化 Canvas
    window.addEventListener('resize', function () {
        initMatrixRain();
        initParticles();
    });

    // 检查是否已登录（已登录直接跳转）
    if (sessionStorage.getItem('sdn_token')) {
        window.location.href = '/index.html';
        return;
    }

    init();
})();

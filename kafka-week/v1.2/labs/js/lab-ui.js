/* ============================================
   Lab UI — Shared components
   ============================================ */

const LabUI = (() => {

  // --- Theme ---
  function initTheme() {
    const saved = localStorage.getItem('kafka-lab-theme') || 'light';
    document.documentElement.setAttribute('data-theme', saved);
    updateThemeIcon(saved);
  }

  function toggleTheme() {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('kafka-lab-theme', next);
    updateThemeIcon(next);
  }

  function updateThemeIcon(theme) {
    const btn = document.querySelector('.theme-toggle');
    if (btn) btn.textContent = theme === 'dark' ? '☀️' : '🌙';
  }

  // --- Sidebar scroll spy ---
  function initScrollSpy() {
    const sections = document.querySelectorAll('.lab-section[id]');
    const navItems = document.querySelectorAll('.lab-sidebar__item[data-section]');
    if (!sections.length || !navItems.length) return;

    const observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          navItems.forEach(item => item.classList.remove('active'));
          const active = document.querySelector(`.lab-sidebar__item[data-section="${entry.target.id}"]`);
          if (active) active.classList.add('active');
        }
      });
    }, { rootMargin: '-20% 0px -70% 0px' });

    sections.forEach(s => observer.observe(s));

    navItems.forEach(item => {
      item.addEventListener('click', e => {
        e.preventDefault();
        const target = document.getElementById(item.dataset.section);
        if (target) target.scrollIntoView({ behavior: 'smooth' });
        closeMobileSidebar();
      });
    });
  }

  // --- Mobile sidebar ---
  function initMobileSidebar() {
    const toggle = document.querySelector('.menu-toggle');
    const sidebar = document.querySelector('.lab-sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    if (!toggle || !sidebar) return;

    toggle.addEventListener('click', () => {
      sidebar.classList.toggle('open');
      if (overlay) overlay.classList.toggle('open');
    });

    if (overlay) {
      overlay.addEventListener('click', closeMobileSidebar);
    }
  }

  function closeMobileSidebar() {
    const sidebar = document.querySelector('.lab-sidebar');
    const overlay = document.querySelector('.sidebar-overlay');
    if (sidebar) sidebar.classList.remove('open');
    if (overlay) overlay.classList.remove('open');
  }

  // --- Quiz engine ---
  function initQuiz(quizContainerId) {
    const container = document.getElementById(quizContainerId);
    if (!container) return;

    container.addEventListener('click', e => {
      const option = e.target.closest('.quiz__option');
      if (!option) return;

      const question = option.closest('.quiz__question');
      if (question.classList.contains('answered')) return;

      const allOptions = question.querySelectorAll('.quiz__option');
      allOptions.forEach(o => o.classList.remove('selected'));
      option.classList.add('selected');

      const isCorrect = option.dataset.correct === 'true';
      question.classList.add('answered');

      if (isCorrect) {
        option.classList.add('correct');
        showFeedback(question, true);
      } else {
        option.classList.add('incorrect');
        allOptions.forEach(o => {
          if (o.dataset.correct === 'true') o.classList.add('correct');
        });
        showFeedback(question, false);
      }
    });
  }

  function showFeedback(question, correct) {
    const feedbacks = question.querySelectorAll('.quiz__feedback');
    feedbacks.forEach(f => f.style.display = 'none');

    const selector = correct ? '.quiz__feedback--correct' : '.quiz__feedback--incorrect';
    const fb = question.querySelector(selector);
    if (fb) fb.style.display = 'block';
  }

  // --- Tabs ---
  function initTabs(containerSelector) {
    const containers = document.querySelectorAll(containerSelector || '.tabs-container');
    containers.forEach(container => {
      const tabs = container.querySelectorAll('.tab');
      const contents = container.querySelectorAll('.tab-content');

      tabs.forEach(tab => {
        tab.addEventListener('click', () => {
          tabs.forEach(t => t.classList.remove('active'));
          contents.forEach(c => c.classList.remove('active'));
          tab.classList.add('active');
          const target = container.querySelector(`#${tab.dataset.tab}`);
          if (target) target.classList.add('active');
        });
      });
    });
  }

  // --- Log console ---
  function createLogger(containerId) {
    const container = document.getElementById(containerId);
    if (!container) return { log() {}, clear() {} };

    return {
      log(message, type = '') {
        const line = document.createElement('div');
        line.className = `log-line${type ? ' log-line--' + type : ''}`;
        line.textContent = message;
        container.appendChild(line);
        container.scrollTop = container.scrollHeight;
      },
      clear() {
        container.innerHTML = '';
      }
    };
  }

  // --- Partition color helper ---
  function getPartitionColor(idx) {
    const colors = ['#6366f1', '#8b5cf6', '#ec4899', '#f59e0b', '#14b8a6'];
    return colors[idx % colors.length];
  }

  // --- Estado badge HTML ---
  function estadoBadge(estado) {
    const cls = estado ? estado.toLowerCase() : 'ninguno';
    const text = estado || 'NINGUNO';
    return `<span class="estado-badge estado-badge--${cls}">${text}</span>`;
  }

  // --- Init all ---
  function init() {
    initTheme();
    initScrollSpy();
    initMobileSidebar();
    initTabs();
  }

  return {
    init,
    initTheme,
    toggleTheme,
    initScrollSpy,
    initMobileSidebar,
    initQuiz,
    initTabs,
    createLogger,
    getPartitionColor,
    estadoBadge
  };
})();

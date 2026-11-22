(() => {
  const tabs = [...document.querySelectorAll('[data-module]')];
  function activate(id, focus = false) {
    if (!tabs.some(t => t.dataset.module === id)) id = 'offline';
    tabs.forEach(tab => {
      const active = tab.dataset.module === id;
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
      document.getElementById(tab.dataset.module).hidden = !active;
      if (active && focus) tab.focus();
    });
    requestAnimationFrame(() => {
      // Charts created while their panel was hidden need their visible dimensions.
      document.getElementById(id).querySelectorAll('[_echarts_instance_]').forEach(el => {
        window.echarts?.getInstanceByDom(el)?.resize();
      });
      window.dispatchEvent(new Event('resize'));
    });
  }
  function select(id, focus = false) {
    history.replaceState(null, '', '#' + id);
    activate(id, focus);
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => select(tab.dataset.module));
    tab.addEventListener('keydown', event => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = tabs.length - 1;
      if (next !== undefined) { event.preventDefault(); select(tabs[next].dataset.module, true); }
    });
  });
  window.addEventListener('hashchange', () => activate(location.hash.slice(1)));
  activate(location.hash.slice(1));
})();

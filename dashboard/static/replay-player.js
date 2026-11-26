(() => {
  let batches = [], shown = 0, playing = true;
  let internal = false, timer, lastKey = "";
  const panel = () => document.getElementById("realtime");
  const visible = () => panel() && !panel().hidden;
  const el = id => document.getElementById(id);

  function label() {
    if (!el("replay-progress")) return;
    el("replay-progress").textContent =
      batches.length
        ? `演示回放 · 已展示 ${shown} / ${batches.length} 批`
        : "等待加载评分批次";
    el("replay-pause").textContent = playing ? "暂停" : "继续";
  }

  function render() {
    if (!shown || !batches.length) return;
    const rows = batches.slice(0, shown);
    const last = rows[rows.length - 1];
    internal = true;
    try {
      window.dispatchEvent(new CustomEvent(
        "movielens-live-batches", {detail: rows}
      ));
    } finally {
      internal = false;
    }
    if (el("live-users")) {
      el("live-users").textContent = last.active_users;
    }
    if (el("live-detail")) {
      el("live-detail").textContent =
        `当前播放批次 ${last.batch_id} · 本批评分 ${last.rating_count} 条 · ` +
        new Date(last.committed_at).toLocaleString("zh-CN", {
          timeZone: "Asia/Shanghai", hour12: false
        });
    }
    if (el("realtime-status")) {
      el("realtime-status").textContent =
        `评分实时回放 · 已展示 ${shown} / ${batches.length} 批 · 北京时间`;
    }
    label();
  }

  function schedule() {
    clearTimeout(timer);
    label();
    if (!visible() || !playing || !batches.length) return;
    if (shown >= batches.length) {
      playing = false;
      label();
      return;
    }
    const wait = Number(el("replay-speed")?.value || 2000);
    timer = setTimeout(() => {
      if (!visible() || !playing) return;
      shown++;
      render();
      schedule();
    }, wait);
  }

  // 在原图表监听器之前接收完整数据，
  // 图表只接收当前播放进度对应的数据。
  window.addEventListener("movielens-live-batches", event => {
    if (internal) return;
    event.stopImmediatePropagation();
    if (!Array.isArray(event.detail)) return;
    const next = [...event.detail].sort((a,b) => a.batch_id - b.batch_id);
    const key = JSON.stringify(next);
    const changed = key !== lastKey;
    const differentRun = batches.length && next.length &&
      batches[0].run_id !== next[0].run_id;

    batches = next;
    lastKey = key;
    if (differentRun) {
      shown = 0;
      playing = true;
    }
    shown = Math.min(shown, batches.length);
    if (!shown && batches.length && visible()) shown = 1;
    render();
    label();
    if (changed) schedule();
  });

  document.addEventListener("DOMContentLoaded", () => {
    const target = panel();
    if (!target) return;

    const controls = document.createElement("div");
    controls.className = "panel";
    controls.style.cssText =
      "padding:14px 20px;margin-bottom:14px;display:flex;gap:14px;align-items:center;flex-wrap:wrap";
    controls.innerHTML = `
      <strong id="replay-progress">等待加载评分批次</strong>
      <button type="button" id="replay-pause">暂停</button>
      <button type="button" id="replay-restart">重新播放</button>
      <label>播放速度
        <select id="replay-speed">
          <option value="4000">慢速：4秒/批</option>
          <option value="2000" selected>正常：2秒/批</option>
          <option value="1000">快速：1秒/批</option>
        </select>
      </label>`;
    target.prepend(controls);

    el("replay-pause").onclick = () => {
      playing = !playing;
      schedule();
    };
    el("replay-restart").onclick = () => {
      shown = batches.length ? 1 : 0;
      playing = true;
      // 清除上一轮的缩放范围，重新从第一个批次展开。
      ["realtime-chart", "average-chart"].forEach(id => {
        const element = el(id);
        if (element) echarts.getInstanceByDom(element)?.clear();
      });
      render();
      schedule();
    };
    el("replay-speed").onchange = schedule;

    new MutationObserver(() => {
      if (visible() && !shown && batches.length) {
        shown = 1;
        render();
      }
      schedule();
    }).observe(target, {attributes:true, attributeFilter:["hidden"]});

    if (visible() && batches.length && !shown) shown = 1;
    render();
    schedule();
  });
})();
(() => {
  let busy = false;
  let state = null;
  let entered = false;
  let polling = false;
  let message = "";
  let selectedRun = null;

  const el = id => document.getElementById(id);
  const active = () => {
    const panel = el("realtime");
    return panel && !panel.hidden;
  };

  async function api(action) {
    const options = {
      cache: "no-store",
      signal: AbortSignal.timeout(8000)
    };
    if (action !== "status") {
      options.method = "POST";
      options.headers = {"X-Replay-Control": "1"};
    }
    const response = await fetch("/api/live-control/" + action, options);
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "控制请求失败");
    return data;
  }

  function paint() {
    if (!el("real-control-state")) return;
    const names = {
      PAUSED: "已暂停发送",
      RUNNING: "正在发送",
      COMPLETED: "本轮发送完成",
      STOPPED: "本轮已结束",
      ERROR: "发送器异常",
      SOURCE_EXHAUSTED: "评分文件已读完"
    };
    const label = state
      ? (names[state.state] || state.state) : "控制服务不可用";
    el("real-control-state").textContent = state
      ? `${label} · 已确认发送 ${state.sent} / ${state.limit} 条`
      : label;
    el("real-control-note").textContent = message ||
      (state && state.state === "PAUSED"
        ? "已停止新增评分输入；已发送的数据仍可能继续产生计算结果。"
        : "图表由实际计算结果更新，不进行定时动画回放。");
    el("real-resume").disabled =
      busy || !state || state.state !== "PAUSED";
    el("real-pause").disabled =
      busy || !state || state.state !== "RUNNING";
  }

  async function refresh() {
    if (polling) return;
    polling = true;
    try {
      state = await api("status");
      selectedRun = state.run_id;
      if (Date.now() - Number(state.updated_at_ms) > 45000) {
        message = "发送器状态超过45秒未更新，请检查虚拟机中的发送器。";
        state = null;
      }
    } catch (error) {
      state = null;
      message = error.message;
    } finally {
      polling = false;
      paint();
    }
  }

  async function command(action) {
    if (busy) return;
    busy = true;
    message = action === "pause"
      ? "正在请求暂停，等待发送器确认…"
      : "正在请求继续发送，等待发送器确认…";
    paint();
    try {
      await api(action);
      const expected = action === "pause" ? "PAUSED" : "RUNNING";
      for (let attempt = 0; attempt < 12; attempt++) {
        await new Promise(resolve => setTimeout(resolve, 500));
        const current = await api("status");
        state = current;
        selectedRun = current.run_id;
        if (current.state === expected ||
            ["COMPLETED","STOPPED","ERROR","SOURCE_EXHAUSTED"]
              .includes(current.state)) {
          message = "";
          return;
        }
      }
      message = "指令已接受，但尚未确认执行完成；请查看发送器状态。";
    } catch (error) {
      message = error.message;
    } finally {
      busy = false;
      paint();
    }
  }

  async function onEnter() {
    if (!active() || entered) return;
    entered = true;
    await refresh();
    if (active() && state && state.state === "PAUSED") {
      await command("resume");
    }
  }

  // 不再拦截正常批次用于动画播放；
  // 仅拒绝与控制端运行编号不一致的结果。
  window.addEventListener("movielens-live-batches", event => {
    const rows = event.detail;
    if (!Array.isArray(rows)) return;
    if (selectedRun && rows.some(row => row.run_id !== selectedRun)) {
      event.stopImmediatePropagation();
      message = "推送结果与发送器运行编号不一致，请检查8088服务配置。";
      paint();
    }
  });

  document.addEventListener("DOMContentLoaded", () => {
    const panel = el("realtime");
    if (!panel) return;

    const box = document.createElement("article");
    box.className = "panel";
    box.style.cssText = "padding:16px 20px;margin-bottom:14px";
    box.innerHTML = `
      <div style="display:flex;gap:12px;align-items:center;flex-wrap:wrap">
        <strong>评分实时分析</strong>
        <span id="real-control-state">正在检查发送器…</span>
        <button type="button" id="real-pause" disabled>暂停发送</button>
        <button type="button" id="real-resume" disabled>继续发送</button>
      </div>
      <p id="real-control-note" style="margin:10px 0 0;font-size:12px;color:#66768c"></p>`;
    panel.prepend(box);

    el("real-pause").onclick = () => command("pause");
    el("real-resume").onclick = () => command("resume");

    new MutationObserver(onEnter).observe(panel, {
      attributes: true, attributeFilter: ["hidden"]
    });

    refresh();
    onEnter();
    setInterval(() => { if (!busy) refresh(); }, 2000);
  });
})();
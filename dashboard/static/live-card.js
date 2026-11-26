(() => {
  const el = id => document.getElementById(id);
  const address = el("live-address");
  if (!address) return;

  address.value = `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.hostname}:8088/ws/live`;

  let socket, timer;
  let generation = 0, delay = 1000;
  let latest = null, connected = false, kafka = false;
  let invalidAddress = false;

  function status() {
    if (invalidAddress) return;
    const age = latest
      ? Math.max(0, Math.floor((Date.now() - Date.parse(latest.committed_at)) / 1000))
      : null;
    el("live-status").textContent =
      `${connected ? "推送已连接" : "推送未连接，自动重试中"} · ` +
      `${connected ? (kafka ? "Kafka 可访问" : "Kafka 暂不可用") : "请检查推送服务"}` +
      (age === null ? " · 等待评分批次"
        : ` · 距最后批次 ${age} 秒${age > 20 ? "（暂无新数据）" : ""}`);

    const badge = el("connection");
    if (badge) {
      badge.dataset.state = connected && kafka ? "online" : "stale";
      el("connection-text").textContent =
        connected && kafka ? "评分推送已连接" : "评分推送连接异常";
    }
  }

  function connect() {
    const token = ++generation;
    clearTimeout(timer);
    if (socket) socket.close();
    connected = false;
    invalidAddress = false;

    let current;
    try {
      const url = new URL(address.value);
      if (!["ws:", "wss:"].includes(url.protocol)) throw Error("protocol");
      current = new WebSocket(url);
      socket = current;
    } catch (_) {
      invalidAddress = true;
      el("live-status").textContent = "地址无效，请填写 ws:// 或 wss:// 地址";
      return;
    }

    current.onopen = () => {
      if (token !== generation) return;
      connected = true;
      delay = 1000;
      status();
    };

    current.onmessage = event => {
      if (token !== generation) return;
      let data;
      try { data = JSON.parse(event.data); } catch (_) { return; }
      if (!Array.isArray(data.batches)) return;

      kafka = data.kafka_connected === true;
      const rows = [...data.batches].sort((a, b) => a.batch_id - b.batch_id);
      latest = rows.at(-1) || null;
      el("live-users").textContent = latest
        ? Number(latest.active_users).toLocaleString() : "—";

      if (latest) {
        const time = new Date(latest.committed_at).toLocaleString("zh-CN", {
          timeZone: "Asia/Shanghai", hour12: false
        });
        el("live-detail").textContent =
          `共 ${rows.length} 批 · 最新批次 ${latest.batch_id} · 更新于 ${time} · 运行 ${latest.run_id}`;
      } else {
        el("live-detail").textContent = "等待评分批次";
      }

      window.dispatchEvent(new CustomEvent(
        "movielens-live-batches", { detail: rows }
      ));
      status();
    };

    current.onclose = () => {
      if (token !== generation) return;
      connected = false;
      status();
      timer = setTimeout(connect, delay);
      delay = Math.min(delay * 2, 15000);
    };
    current.onerror = () => {
      if (token === generation) current.close();
    };
    status();
  }

  el("live-connect").addEventListener("click", () => {
    delay = 1000;
    connect();
  });

  // 兼容之前收藏的第七个入口地址
  if (location.hash === "#live") location.hash = "#realtime";

  setInterval(status, 5000);
  connect();
})();
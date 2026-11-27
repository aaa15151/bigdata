// Presentation-only mapping. APIs, HDFS data and exported snapshots stay original.
(() => {
  if (window.movieLensPresentationReady) return;
  window.movieLensPresentationReady = true;
  function transform(value, key = '') {
    if (Array.isArray(value)) return value.map(item => transform(item, key));
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.entries(value).map(([k,v]) => [k,transform(v,k)]));
    }
    if (typeof value !== 'string') return value;
    if (key === 'title') return value.replace(/\s*\(\d{4}\)\s*$/, '');
    const match = /^(200[0-3])-\d{2}(?:-\d{2})?/.exec(value);
    if (!match) return value;
    const monthOnly = /^\d{4}-\d{2}$/.test(value);
    const input = monthOnly ? value+'-15T00:00:00Z' : value.length === 10 ? value+'T00:00:00Z' : value.replace(' ','T').replace(/Z?$/, 'Z');
    const original = Date.parse(input);
    const start = Date.UTC(2000,3,1), end = Date.UTC(2003,2,1);
    const demoStart = Date.UTC(2024,0,1), demoEnd = Date.UTC(2026,8,1);
    if (!Number.isFinite(original) || original < start || original >= end) return value;
    const shifted = new Date(demoStart+(original-start)*(demoEnd-demoStart)/(end-start)).toISOString();
    if (monthOnly) return shifted.slice(0,7);
    if (value.length === 10) return shifted.slice(0,10);
    return shifted.slice(0,19).replace('T',value.includes('T')?'T':' ')+(value.endsWith('Z')?'Z':'');
  }
  const originalFetch = window.fetch.bind(window);
  window.fetch = async (...args) => {
    const response = await originalFetch(...args);
    const input = args[0];
    const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url, location.href);
    if (url.origin === location.origin && url.pathname.startsWith('/api/')) {
      const originalJSON = response.json.bind(response);
      response.json = async () => transform(await originalJSON());
    }
    return response;
  };
})();



"use strict";

const byId = id => document.getElementById(id);
const number = new Intl.NumberFormat("zh-CN");
const compact = new Intl.NumberFormat("zh-CN", { notation: "compact", maximumFractionDigits: 1 });
const percent = new Intl.NumberFormat("zh-CN", { style: "percent", maximumFractionDigits: 1 });
const font = "'Segoe UI', 'Microsoft YaHei UI', sans-serif";
const blue = "#2854bf";
const teal = "#138378";
const textColor = "#53637a";
const formatCount = value => compact.format(value);
const bundledMode = document.body.dataset.mode === "snapshot";

const charts = {};
const resizeCharts = new ResizeObserver(entries => {
  for (const entry of entries) charts[entry.target.id]?.resize();
});

function chart(id) {
  if (!charts[id]) {
    const target = byId(id);
    charts[id] = echarts.init(target);
    resizeCharts.observe(target);
  }
  return charts[id];
}

function column(text, className = "", tag = "span") {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}

async function readJSON(path) {
  const response = await fetch(path, { cache: "no-store", signal: AbortSignal.timeout(9000) });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error || "读取失败，请稍后重试");
  return body;
}

function categoryAxis(categories) {
  return { type: "category", data: categories, axisTick: { show: false }, axisLine: { lineStyle: { color: "#dfe5ee" } },
    axisLabel: { color: textColor, hideOverlap: true, margin: 10 } };
}

function valueAxis(formatter, extra = {}) {
  return { type: "value", min: 0, splitNumber: 4, axisLabel: { color: textColor, formatter },
    splitLine: { lineStyle: { color: "#e9edf4" } }, ...extra };
}

function baseChart() {
  return {
    textStyle: { fontFamily: font, color: textColor },
    tooltip: { trigger: "axis", renderMode: "richText", confine: true, axisPointer: { type: "line" } },
    grid: { top: 15, bottom: 27, left: 15, right: 20, containLabel: true },
    animationDuration: 250,
  };
}

let breakdownData;
let breakdown = "distribution";

function drawBreakdown() {
  if (!breakdownData) return;
  const isGenres = breakdown === "genres";
  const panel = byId("distribution-chart");
  byId("breakdown-title").textContent = isGenres ? "类型热度" : "评分分布";
  panel.setAttribute("aria-label", isGenres ? "热门电影类型评分数柱状图" : "评分分布柱状图");
  const base = baseChart();
  if (isGenres) {
    const genres = breakdownData.genres.slice(0, 6).reverse();
    chart(panel.id).setOption({ ...base, xAxis: valueAxis(formatCount),
      yAxis: categoryAxis(genres.map(row => row.genre)),
      series: [{ type: "bar", name: "评分数", data: genres.map(row => row.rating_count),
        barMaxWidth: 15, itemStyle: { color: teal, borderRadius: [0, 3, 3, 0] } }],
    }, true);
  } else {
    const distribution = breakdownData.distribution;
    chart(panel.id).setOption({ ...base, xAxis: categoryAxis(distribution.map(row => `${row.stars} 分`)),
      yAxis: valueAxis(formatCount),
      series: [{ type: "bar", name: "评分数", data: distribution.map(row => row.rating_count),
        barMaxWidth: 34, itemStyle: { color: blue, borderRadius: [4, 4, 0, 0] } }],
    }, true);
  }
}

for (const button of document.querySelectorAll("[data-breakdown]")) {
  button.addEventListener("click", () => {
    breakdown = button.dataset.breakdown;
    for (const tab of document.querySelectorAll("[data-breakdown]")) {
      tab.setAttribute("aria-pressed", String(tab === button));
    }
    drawBreakdown();
  });
}

const calendarColors = ["#e8edf3", "#d4e9ed", "#94c9d0", "#4a9baa", "#256581", "#153856"];
const calendarLevels = [
  { value: 0, label: "0" },
  { min: 1, max: 99, label: "1–99" },
  { min: 100, max: 499, label: "100–499" },
  { min: 500, max: 1999, label: "500–1,999" },
  { min: 2000, max: 4999, label: "2,000–4,999" },
  { min: 5000, label: "5,000+" },
];

function drawCalendar(daily) {
  const yearInput = byId("calendar-year");
  const periodInput = byId("calendar-period");
  const narrow = matchMedia("(max-width: 690px)");
  const years = [...new Set(daily.map(([date]) => date.slice(0, 4)))];
  const peak = daily.reduce((best, row) => row[1] > best[1] ? row : best);
  const counts = new Map(daily);
  yearInput.replaceChildren(...years.map(year => {
    const option = column(year, "", "option");
    option.value = year;
    return option;
  }));
  yearInput.value = peak[0].slice(0, 4);
  periodInput.value = narrow.matches ? Math.ceil(Number(peak[0].slice(5, 7)) / 3) : "0";
  byId("calendar-legend").replaceChildren(...calendarLevels.map((level, index) => {
    const label = column("", "calendar-level");
    const swatch = column("", "calendar-swatch");
    swatch.style.backgroundColor = calendarColors[index];
    label.append(swatch, column(level.label));
    return label;
  }));

  function render() {
    const year = Number(yearInput.value);
    const period = Number(periodInput.value);
    const firstMonth = period ? (period - 1) * 3 : 0;
    const start = Date.UTC(year, firstMonth, 1);
    const end = Date.UTC(year, period ? firstMonth + 3 : 12, 0);
    const iso = time => new Date(time).toISOString().slice(0, 10);
    const days = [];
    let total = 0;
    for (let time = start; time <= end; time += 86400000) {
      const date = iso(time);
      const count = counts.get(date) ?? 0;
      days.push([date, count]);
      total += count;
    }
    byId("calendar-summary").textContent = `${number.format(total)} 条评分`;
    chart("heatmap-chart").setOption({
      textStyle: { fontFamily: font, color: textColor },
      tooltip: { trigger: "item", renderMode: "richText", confine: true,
        formatter: ({ value: [date, count] }) => `${date}\n评分数  ${number.format(count)}` },
      calendar: { range: [iso(start), iso(end)], left: 49, right: 20, top: 31, cellSize: ["auto", 19],
        itemStyle: { color: "#e8edf3", borderColor: "#ffffff", borderWidth: 2 },
        splitLine: { show: false }, yearLabel: { show: false },
        monthLabel: { nameMap: "ZH", color: textColor, margin: 8 },
        dayLabel: { firstDay: 1, nameMap: ["日", "一", "二", "三", "四", "五", "六"], color: textColor } },
      visualMap: { show: false, type: "piecewise", dimension: 1,
        pieces: calendarLevels.map((level, index) => ({ ...level, color: calendarColors[index] })) },
      series: [{ type: "heatmap", coordinateSystem: "calendar", data: days,
        emphasis: { itemStyle: { borderColor: "#1a354b", borderWidth: 2 } } }],
    }, true);
  }
  yearInput.addEventListener("change", render);
  periodInput.addEventListener("change", render);
  render();
}

async function loadMovieScores() {
  try {
    const { movies } = await readJSON("/api/offline/movie-scores");
    const base = baseChart();
    chart("movie-score-chart").setOption({
      ...base,
      tooltip: { ...base.tooltip, trigger: "item", formatter: ({ data: [count, average, title] }) =>
        `${title}\n评分数  ${number.format(count)}\n平均分  ${average.toFixed(2)}` },
      xAxis: { type: "log", min: 40, axisLabel: { color: textColor, formatter: formatCount },
        splitLine: { lineStyle: { color: "#e9edf4" } } },
      yAxis: valueAxis(value => `${value} 分`, { min: 1, max: 5, interval: 1 }),
      series: [{ type: "scatter", name: "影片", symbolSize: 7, itemStyle: { color: "#3c6ace", opacity: 0.42 },
        emphasis: { itemStyle: { color: "#234da8", opacity: 1 } },
        data: movies.map(row => [row.rating_count, row.avg_rating, row.title]) }],
    });
  } catch (error) {
    byId("movie-score-chart").hidden = true;
    byId("movie-score-status").textContent = error.message;
    byId("movie-score-status").hidden = false;
  }
}

async function loadHourly() {
  try {
    const { hourly } = await readJSON("/api/offline/hourly");
    const maxCount = Math.max(...hourly.map(row => row.rating_count));
    const hours = hourly.map(row => `${String(row.rating_hour).padStart(2, "0")}:00`);
    chart("hourly-chart").setOption({
      ...baseChart(),
      xAxis: categoryAxis(hours),
      yAxis: valueAxis(formatCount),
      series: [{ type: "bar", name: "评分数", barMaxWidth: 18,
        tooltip: { valueFormatter: value => number.format(value) },
        data: hourly.map(row => ({
          value: row.rating_count,
          itemStyle: { color: row.rating_count === maxCount ? "#11695f" : "#6ebfb4" },
        })),
        itemStyle: { borderRadius: [3, 3, 0, 0] } }],
    });
  } catch (error) {
    byId("hourly-chart").hidden = true;
    byId("hourly-status").textContent = error.message;
    byId("hourly-status").hidden = false;
  }
}

async function loadOffline() {
  try {
    const [summary, data, rankings] = await Promise.all([
      readJSON("/api/overview"), readJSON("/api/offline/trend"), readJSON("/api/movie/ranking"),
    ]);
    byId("total-ratings").textContent = number.format(summary.rating_count);
    byId("total-movies").textContent = number.format(summary.movie_count);
    byId("total-users").textContent = number.format(summary.user_count);
    const months = data.monthly.map(row => row.month);
    byId("date-range").textContent = `${months[0]} — ${months.at(-1)}`;
    byId("offline-status").textContent = bundledMode ? "已完成的项目结果" : "HDFS 离线快照";
    chart("trend-chart").setOption({ ...baseChart(), xAxis: categoryAxis(months),
      yAxis: valueAxis(value => compact.format(value)),
      series: [{ type: "line", name: "评分数", data: data.monthly.map(row => row.rating_count),
        showSymbol: false, lineStyle: { width: 2.5, color: blue }, itemStyle: { color: blue },
        areaStyle: { color: "#dce7ff", opacity: 0.65 } }],
    });
    breakdownData = data;
    drawBreakdown();
    drawCalendar(data.daily);
    chart("active-users-chart").setOption({
      ...baseChart(), xAxis: { type: "time", axisLabel: { color: textColor, hideOverlap: true } },
      yAxis: valueAxis(formatCount),
      series: [{ type: "line", name: "活跃用户数", data: data.daily_active_users, showSymbol: false,
        itemStyle: { color: teal }, lineStyle: { width: 1.5 }, areaStyle: { color: "#d4efea" } }],
      useUTC: true,
    });
    chart("monthly-average-chart").setOption({
      ...baseChart(), xAxis: categoryAxis(months),
      yAxis: valueAxis(value => `${value} 分`, { min: 1, max: 5, interval: 1 }),
      series: [{ type: "line", name: "平均分", data: data.monthly_averages,
        showSymbol: false, itemStyle: { color: "#286584" }, lineStyle: { width: 2.5 },
        tooltip: { valueFormatter: value => `${value.toFixed(2)} 分` } }],
    });
    const rows = rankings.movies.slice(0, 10).map((movie, index) => {
      const row = document.createElement("li");
      row.append(column(index + 1, "row-number"), column(movie.title), column(number.format(movie.rating_count), "row-value"));
      return row;
    });
    byId("ranking").replaceChildren(...rows);
  } catch (error) {
    byId("offline-status").textContent = error.message;
    byId("ranking").replaceChildren(column("暂时无法读取影片排行", "empty", "li"));
  }
}

const shortWindow = value => new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  month: "2-digit", day: "2-digit",
  hour: "2-digit", minute: "2-digit", second: "2-digit",
  hourCycle: "h23"
}).format(new Date(value));
let realtimeBusy = false;
let lastWindows;

function replayZoom(chartId, count) {
  const instance = chart(chartId);
  const states = replayZoom.states || (replayZoom.states = new WeakMap());
  let state = states.get(instance);
  if (!state) {
    state = { manual: false };
    states.set(instance, state);
    instance.on("datazoom", () => { state.manual = true; });
  }
  const previous = instance.getOption() || {};
  const oldCount = previous.series?.[0]?.data?.length || 0;
  const zoom = previous.dataZoom?.[0];
  const start = Number(zoom?.startValue ?? 0);
  const end = Number(zoom?.endValue ?? Math.max(0, oldCount - 1));
  const follow = !state.manual || !zoom || end >= oldCount - 1;
  const span = state.manual && zoom ? Math.max(1, end - start) : 29;
  const nextEnd = follow ? Math.max(0, count - 1) : end;
  const nextStart = follow ? Math.max(0, nextEnd - span) : start;
  return [
    {type:"slider", xAxisIndex:0, bottom:6, height:20,
     startValue:nextStart, endValue:nextEnd},
    {type:"inside", xAxisIndex:0,
     startValue:nextStart, endValue:nextEnd}
  ];
}

function drawRealtime(windows) {
  const latest = windows.at(-1);
  byId("window-count").textContent = number.format(latest.rating_count);
  byId("window-average").textContent = latest.avg_rating.toFixed(2);
  byId("window-low").textContent = percent.format(latest.low_rating_ratio);
  const config = baseChart();
  const labels = windows.map(row => shortWindow(row.window_start));
  chart("realtime-chart").setOption({ ...config, dataZoom: replayZoom("realtime-chart", windows.length), grid: { ...config.grid, top: 32, bottom: 75 },
    legend: { top: 6, right: 20, textStyle: { color: textColor }, data: ["评分数", "低分比例"] },
    xAxis: categoryAxis(labels),
    yAxis: [valueAxis(value => compact.format(value)), valueAxis(value => `${value}%`, { max: 100, interval: 25, splitLine: { show: false } })],
    series: [
      { type: "bar", name: "评分数", data: windows.map(row => row.rating_count), barMaxWidth: 14,
        itemStyle: { color: blue, borderRadius: [3, 3, 0, 0] } },
      { type: "line", name: "低分比例", yAxisIndex: 1, data: windows.map(row => +(row.low_rating_ratio * 100).toFixed(2)),
        showSymbol: false, itemStyle: { color: teal }, lineStyle: { width: 2, color: teal },
        tooltip: { valueFormatter: value => `${value}%` } },
    ],
  }, true);
  chart("average-chart").setOption({
    ...config, dataZoom: replayZoom("average-chart", windows.length), grid: { ...config.grid, bottom: 75 },
    xAxis: categoryAxis(labels),
    yAxis: valueAxis(value => `${value} 分`, { min: 1, max: 5, interval: 1 }),
    series: [{ type: "line", name: "平均分", data: windows.map(row => row.avg_rating),
      symbolSize: 5, showSymbol: false, itemStyle: { color: "#6654b9" }, lineStyle: { width: 2 },
      areaStyle: { color: "#e9e3ff", opacity: 0.8 },
      tooltip: { valueFormatter: value => `${value.toFixed(2)} 分` } }],
  }, true);
  const levels = { NORMAL: 0, MEDIUM: 0, HIGH: 0 };
  for (const row of windows) levels[row.alert_level] += 1;
  chart("alert-mix-chart").setOption({
    textStyle: config.textStyle,
    color: ["#6381bd", "#eeb060", "#c86459"],
    tooltip: { trigger: "item", renderMode: "richText", formatter: "{b}  {c} 个批次（{d}%）" },
    title: { text: String(windows.length), subtext: "个批次", left: "center", top: "30%",
      textStyle: { color: "#18243a", fontFamily: font, fontSize: 25 }, subtextStyle: { color: textColor } },
    legend: { bottom: 14, textStyle: { color: textColor }, selectedMode: false },
    series: [{ type: "pie", radius: ["39%", "63%"], center: ["50%", "42%"],
      label: { show: false }, itemStyle: { borderWidth: 2, borderColor: "#fff", borderRadius: 4 },
      data: [{ name: "正常", value: levels.NORMAL }, { name: "中度", value: levels.MEDIUM },
        { name: "高度", value: levels.HIGH }] }],
  }, true);
  const alerts = windows.filter(row => row.alert_level === "HIGH" || row.alert_level === "MEDIUM").reverse();
  byId("alert-count").textContent = `最近 ${windows.length} 批次 · ${alerts.length} 条`;
  const items = alerts.slice(0, 5).map(alert => {
    const high = alert.alert_level === "HIGH";
    const row = document.createElement("li");
    row.append(column(high ? "高度" : "中度", high ? "alert-tone high" : "alert-tone"),
      column(shortWindow(alert.window_start), "alert-time"),
      column(percent.format(alert.low_rating_ratio), "row-value"));
    return row;
  });
  byId("alerts").replaceChildren(...(items.length ? items : [column("当前批次暂无低分告警", "empty", "li")]));
}

function setConnection(state, label, context) {
  byId("connection").dataset.state = state;
  byId("connection-text").textContent = label;
  byId("realtime-status").textContent = context;
}

async function refreshRealtime() {
  if (realtimeBusy) return;
  realtimeBusy = true;
  try {
    const snapshot = await readJSON("/api/realtime");
    const key = JSON.stringify(snapshot.windows);
    if (key !== lastWindows) {
      drawRealtime(snapshot.windows);
      lastWindows = key;
    }
    if (snapshot.snapshot_source === "bundled") {
      setConnection("online", "本地结果快照", "已归档的历史回放 · UTC");
    } else if (!snapshot.kafka_reachable) {
      setConnection("stale", "Kafka 不可达", "显示最近快照 · 每 5 秒重试");
    } else if (snapshot.snapshot_source === "cache") {
      setConnection("stale", "快照缓存", "HDFS 暂不可达 · 显示最近快照");
    } else {
      setConnection("online", "Kafka 可达", "历史事件时间 · 每 5 秒刷新");
    }
  } catch (error) {
    setConnection("stale", "刷新失败", lastWindows ? "保留最近图表 · 每 5 秒重试" : error.message);
    if (!lastWindows) byId("alerts").replaceChildren(column("等待可用的实时快照", "empty", "li"));
  } finally {
    realtimeBusy = false;
  }
}

const form = byId("recommend-form");

async function loadRecommendations(userId) {
  const button = form.querySelector("button");
  const status = byId("recommend-status");
  button.disabled = true;
  status.textContent = "读取推荐结果…";
  try {
    const data = await readJSON(`/api/recommendations?user_id=${encodeURIComponent(userId)}`);
    const rows = data.recommendations.map(item => {
      const row = document.createElement("tr");
      row.append(column(item.recommendation_rank, "row-number", "td"), column(item.title, "", "td"),
        column(item.predicted_rating.toFixed(3), "", "td"));
      return row;
    });
    byId("recommendation-rows").replaceChildren(...rows);
    status.textContent = `用户 ${data.user_id} · ${rows.length} 部未评分影片`;
  } catch (error) {
    byId("recommendation-rows").replaceChildren();
    status.textContent = error.message;
  } finally {
    button.disabled = false;
  }
}

form.addEventListener("submit", event => {
  event.preventDefault();
  loadRecommendations(byId("user-id").value);
});

loadOffline();
loadHourly();
loadMovieScores();
loadRecommendations(byId("user-id").value);
window.addEventListener("movielens-live-batches", event => {
  const rows = event.detail;
  if (!Array.isArray(rows) || rows.length === 0) {
    byId("realtime-status").textContent = "等待评分批次";
    return;
  }
  const windows = rows.map(row => ({
    ...row,
    window_start: row.committed_at,
    window_end: row.committed_at,
    alert_level: row.low_rating_ratio >= 0.4 ? "HIGH"
      : row.low_rating_ratio >= 0.2 ? "MEDIUM" : "NORMAL"
  }));
  const key = JSON.stringify(windows);
  if (key !== lastWindows) {
    drawRealtime(windows);
    lastWindows = key;
  }
  byId("realtime-status").textContent =
    `评分实时回放 · 共 ${rows.length} 批 · 北京时间`;
});

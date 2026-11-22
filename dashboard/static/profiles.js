(() => {
  const charts = [];
  async function get(url) {
    const response = await fetch(url);
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || '读取失败');
    return data;
  }
  function chart(id, option) {
    const c = echarts.init(document.getElementById(id));
    c.setOption(option); charts.push(c);
  }
  async function task(name, work) {
    const status = document.getElementById(name + '-status');
    status.textContent = '正在读取集群结果…';
    try { await work(); status.textContent = ''; }
    catch (e) { status.textContent = e.message; }
  }
  task('age', async () => {
    const {rows} = await get('/api/profile/age-genre');
    rows.sort((a,b) => a.age_group-b.age_group);
    const genres = Object.keys(rows[0] || {}).filter(k => !['age_group','age_label'].includes(k)).sort();
    const data = rows.flatMap((r,y) => genres.map((g,x) => [x,y,r[g] || 0]));
    chart('age-chart', {tooltip:{position:'top'},grid:{left:90,right:20,bottom:120},
      xAxis:{type:'category',data:genres,axisLabel:{rotate:50}},yAxis:{type:'category',data:rows.map(r=>r.age_label)},
      visualMap:{min:0,max:Math.max(1,...data.map(d=>d[2])),calculable:true,orient:'horizontal',bottom:0,left:'center'},
      series:[{type:'heatmap',data}]});
  });
  task('gender', async () => {
    const {rows} = await get('/api/profile/gender');
    const genres = [...new Set(rows.map(r=>r.genre))].sort();
    chart('gender-chart',{tooltip:{trigger:'axis'},legend:{},grid:{bottom:100,left:55,right:20},
      xAxis:{type:'category',data:genres,axisLabel:{rotate:50}},yAxis:{type:'value',name:'占比 (%)'},
      series:['F','M'].map(g=>{const subset=rows.filter(r=>r.gender===g);const total=subset.reduce((s,r)=>s+r.rating_count,0);
        return {name:g==='F'?'女性':'男性',type:'bar',data:genres.map(t=>Number((100*(subset.find(r=>r.genre===t)?.rating_count||0)/(total||1)).toFixed(2)))};})});
  });
  let generation = 0;
  function loadGenre() {
    const current = ++generation;
    return task('genre',async()=>{
      const select=document.getElementById('genre-select');
      const data=await get('/api/genre/top10?genre='+encodeURIComponent(select.value));
      if (current !== generation) return;
      select.replaceChildren(...data.genres.map(g=>{const o=document.createElement('option');o.value=g;o.textContent=g;return o;}));
      select.value=data.genre;
      const body=document.getElementById('genre-rows');body.replaceChildren();
      data.movies.forEach(r=>{const tr=document.createElement('tr');[r.rank,r.title,r.rating_count,Number(r.avg_rating).toFixed(4)].forEach(v=>{const td=document.createElement('td');td.textContent=v;tr.appendChild(td);});body.appendChild(tr);});
    });
  }
  document.getElementById('genre-select').addEventListener('change',loadGenre);loadGenre();
  task('daily',async()=>{
    const {rows}=await get('/api/profile/daily');
    chart('daily-profile-chart',{tooltip:{trigger:'axis'},legend:{},grid:{left:65,right:25,bottom:70},
      xAxis:{type:'category',data:rows.map(r=>r.rating_date)},yAxis:{type:'value'},dataZoom:[{type:'inside'},{type:'slider'}],
      series:[{name:'每日评分量',type:'line',showSymbol:false,data:rows.map(r=>r.rating_count)},
        {name:'7 日移动平均',type:'line',showSymbol:false,data:rows.map(r=>r.moving_avg_7d)}]});
  });
  window.addEventListener('resize',()=>charts.forEach(c=>c.resize()));
})();

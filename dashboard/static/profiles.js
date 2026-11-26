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
    if (!rows.length) throw new Error('暂无年龄分布数据');
    rows.sort((a,b) => a.age_group-b.age_group);

    const names = {
      Action:'动作', Adventure:'冒险', Animation:'动画',
      "Children's":'儿童', Comedy:'喜剧', Crime:'犯罪',
      Documentary:'纪录片', Drama:'剧情', Fantasy:'奇幻',
      'Film-Noir':'黑色电影', Horror:'恐怖', Musical:'歌舞',
      Mystery:'悬疑', Romance:'爱情', 'Sci-Fi':'科幻',
      Thriller:'惊悚', War:'战争', Western:'西部'
    };

    const element = document.getElementById('age-chart');
    const panel = element.closest('.panel');
    element.style.setProperty('height','580px','important');
    element.style.minWidth = '0';

    if (panel) {
      panel.style.padding = '22px 24px';
      const heading = panel.querySelector('h3');
      if (heading) {
        heading.textContent = '不同年龄段的电影类型偏好';
        heading.style.margin = '0 0 16px';
      }
    }

    const controls = document.createElement('div');
    controls.style.cssText =
      'display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px';
    element.before(controls);

    const note = document.createElement('p');
    note.style.cssText =
      'font-size:12px;color:#7a8899;margin:0 0 8px;line-height:1.7';
    note.textContent =
      '占比按本年龄组的类型评分归属次数计算；一部电影可归属多个类型。';
    element.before(note);

    const c = echarts.init(element);
    charts.push(c);

    function draw(row) {
      const items = Object.keys(row)
        .filter(k=>!['age_group','age_label'].includes(k))
        .map(key=>({
          key,
          name:names[key] || key,
          count:Number(row[key] || 0)
        }))
        .sort((a,b)=>b.count-a.count || a.key.localeCompare(b.key));

      const total = items.reduce((sum,item)=>sum+item.count,0);
      items.forEach(item=>{
        item.share = total ? item.count / total * 100 : 0;
      });

      [...controls.children].forEach(button=>{
        const active = button.dataset.age === String(row.age_group);
        button.setAttribute('aria-pressed',String(active));
        button.style.background = active ? '#087e93' : '#f2f6f8';
        button.style.color = active ? '#fff' : '#52677b';
      });

      c.setOption({
        animationDuration:550,
        animationDurationUpdate:550,
        textStyle:{
          fontFamily:'Microsoft YaHei, PingFang SC, sans-serif',
          color:'#617084'
        },
        grid:{left:82,right:68,top:14,bottom:42},
        tooltip:{
          trigger:'axis',
          axisPointer:{type:'shadow'},
          renderMode:'richText',
          backgroundColor:'#fff',
          borderColor:'#dce7ed',
          padding:12,
          formatter:params=>{
            const item=items[params[0].dataIndex];
            return `${row.age_label} · ${item.name}\n类型评分占比：${item.share.toFixed(1)}%\n评分次数：${item.count.toLocaleString('zh-CN')}`;
          }
        },
        xAxis:{
          type:'value',
          min:0,
          max:Math.max(5,Math.ceil((items[0]?.share || 0)/5)*5),
          axisLine:{show:false},
          axisTick:{show:false},
          axisLabel:{formatter:'{value}%',fontSize:11,color:'#8a97a6'},
          splitLine:{lineStyle:{color:'#edf2f5'}}
        },
        yAxis:{
          type:'category',
          inverse:true,
          data:items.map(item=>item.name),
          axisLine:{show:false},
          axisTick:{show:false},
          axisLabel:{fontSize:12,color:'#526174',margin:14}
        },
        series:[{
          name:'类型评分占比',
          type:'bar',
          barWidth:15,
          data:items.map((item,index)=>({
            value:item.share,
            itemStyle:{
              color:index===0?'#08768b':
                index===1?'#16879b':
                index===2?'#389caf':'#8bc8d5',
              borderRadius:[0,2,2,0]
            }
          })),
          label:{
            show:true,
            position:'right',
            distance:8,
            color:'#526174',
            fontSize:11,
            formatter:p=>`${Number(p.value).toFixed(1)}%`
          },
          emphasis:{itemStyle:{color:'#08768b'}}
        }]
      },true);
    }

    rows.forEach(row=>{
      const button=document.createElement('button');
      button.type='button';
      button.textContent=row.age_label;
      button.dataset.age=String(row.age_group);
      button.style.cssText =
        'border:0;border-radius:7px;padding:8px 13px;font-size:12px;cursor:pointer';
      button.onclick=()=>draw(row);
      controls.appendChild(button);
    });

    draw(rows.find(row=>Number(row.age_group)===25) || rows[0]);
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

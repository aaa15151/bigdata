(() => {
  const status = document.getElementById('graph-status');
  function list(id, rows, describe) {
    const parent = document.getElementById(id);
    parent.replaceChildren(...rows.map(r => {
      const item = document.createElement('li'); item.textContent = describe(r); return item;
    }));
  }
  async function load() {
    try {
      const response = await fetch('/api/graph');
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || '图数据读取失败');
      const m = data.metrics;
      status.textContent = `${m.vertices} 部电影 · ${m.undirected_pairs} 对关系 · ${m.communities} 个社区 · ${m.components} 个连通分量；当前展示 ${data.nodes.length} 个节点`;
      const ids = [...new Set(data.nodes.map(n=>n.community_id))].sort((a,b)=>a-b);
      const max = Math.max(...data.nodes.map(n=>n.pagerank),1);
      const container = document.getElementById('movie-graph');
      // Wait for real dimensions: hidden tab panels have zero layout width.
      await new Promise(resolve => {
        const ready = () => container.clientWidth > 0 && container.clientHeight > 0;
        if (ready()) { resolve(); return; }
        const observer = new ResizeObserver(() => {
          if (ready()) { observer.disconnect(); requestAnimationFrame(resolve); }
        });
        observer.observe(container);
      });
      const chart = echarts.init(container);
      chart.setOption({tooltip:{renderMode:'richText',formatter:p=>p.dataType==='edge' ? `共同喜欢人数：${p.data.value}` : `${p.data.name}\nPageRank：${p.data.value.toFixed(4)}\n社区：${p.data.community}\n邻居数：${p.data.degree}`},
        series:[{type:'graph',layout:'force',roam:true,draggable:true,
          categories:ids.map(id=>({name:`社区 ${id}`})),
          data:data.nodes.map(n=>({id:String(n.movie_id),name:n.title,value:n.pagerank,degree:n.degree,community:n.community_id,
            category:ids.indexOf(n.community_id),symbolSize:10+30*Math.sqrt(n.pagerank/max)})),
          links:data.edges.map(e=>({source:String(e.source),target:String(e.target),value:e.co_count})),
          label:{show:false},emphasis:{focus:'adjacency',label:{show:true}},
          lineStyle:{opacity:0.14,width:1},force:{initLayout:'circular',repulsion:130,edgeLength:[35,100],gravity:0.08},animation:false}]});
      const sizeObserver = new ResizeObserver(() => {
        if (container.clientWidth > 0 && container.clientHeight > 0) chart.resize();
      });
      sizeObserver.observe(container);
      list('graph-ranking',data.ranking,r=>`${r.title} · PageRank ${r.pagerank.toFixed(4)}`);
      list('graph-popular',data.popular,r=>`${r.title} · ${r.rating_count} 次评分`);
      list('graph-communities',[...data.communities].sort((a,b)=>b.movie_count-a.movie_count).slice(0,10),r=>
        `社区 ${r.community_id}：${r.movie_count} 部电影；${r.genre_counts.slice(0,3).map(g=>`${g.genre} ${g.count} 部`).join('、')}`);
    } catch(e) { status.textContent=e.message; }
  }
  load();
})();

(() => {
  const form = document.getElementById('similar-form');
  let generation = 0;
  async function query(event) {
    if (event) event.preventDefault();
    const current = ++generation;
    const status = document.getElementById('similar-status');
    const body = document.getElementById('similar-rows');
    body.replaceChildren();
    status.textContent = '正在读取相似电影，首次从集群加载可能较慢…';
    try {
      const response = await fetch('/api/movie/similar?movie_id=' + encodeURIComponent(document.getElementById('similar-id').value));
      const data = await response.json();
      if (current !== generation) return;
      if (!response.ok) throw new Error(data.error || '读取失败');
      for (const movie of data.movies) {
        const row = document.createElement('tr');
        for (const value of [movie.rank, movie.title, movie.co_count, Number(movie.similarity).toFixed(4)]) {
          const cell = document.createElement('td');
          cell.textContent = value;
          row.appendChild(cell);
        }
        body.appendChild(row);
      }
      status.textContent = data.movies.length ? `电影 ${data.movie_id} 的 ${data.movies.length} 部相似电影` : '该电影暂无满足条件的相似结果，请尝试 260。';
    } catch (error) {
      if (current === generation) status.textContent = error.message;
    }
  }
  form.addEventListener('submit', query);
  query();
})();

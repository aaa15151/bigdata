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

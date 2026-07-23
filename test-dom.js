// jsdom でページを実行し、ヘッダ連動・学習レベル・操作系を検証する
const fs = require('fs');
const { JSDOM, VirtualConsole } = require('jsdom');

const html = fs.readFileSync(require('path').join(__dirname, 'index.html'), 'utf8');
const vc = new VirtualConsole();
const errors = [];
vc.on('jsdomError', e => errors.push('jsdomError: ' + e.message));
vc.on('error', (...a) => errors.push('console.error: ' + a.join(' ')));

const dom = new JSDOM(html, { runScripts: 'dangerously', virtualConsole: vc, pretendToBeVisual: true, url: 'https://localhost/' });
const { window } = dom;
const d = window.document;

setTimeout(() => {
  const $ = id => d.getElementById(id);
  const fire = (el, type) => el.dispatchEvent(new window.Event(type, { bubbles: true }));
  const click = el => el.dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  const results = [];

  results.push(['load errors', errors.length ? errors.join(' / ') : 'none']);

  // ---- Lv1(初回)の姿 ----
  results.push(['initial level = L1', d.body.classList.contains('L1')]);
  results.push(['Lv1 DML has no WHERE', !$('ctxDml').innerHTML.includes('WHERE')]);
  results.push(['Lv1 DDL: PK first, no INDEX/ORDER BY', $('ctxDdl').innerHTML.includes('PRIMARY KEY') && !$('ctxDdl').innerHTML.includes('INDEX') && !$('ctxDdl').innerHTML.includes('ORDER BY')]);
  const e0 = errors.length;
  click($('btnInsert')); click($('btnMerge'));
  results.push(['Lv1 insert/merge error-free', errors.length === e0]);

  // ---- レベル遷移 ----
  d.querySelector('.lvchip[data-lv="2"]').dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  results.push(['chip → L2', d.body.classList.contains('L2')]);
  results.push(['Lv2 DDL: PK clause is term link', $('ctxDdl').innerHTML.includes('data-g="pk"')]);
  d.querySelector('.lvchip[data-lv="7"]').dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  results.push(['jump → L7', d.body.classList.contains('L7')]);
  const setChecks=(sel,vals)=>{ d.querySelectorAll(sel).forEach(c=>{ const want=vals.includes(c.value); if(c.checked!==want){ c.checked=want; fire(c,'change'); } }); };
  // 既定は全開なので、共通クエリの条件を明示的に立てる
  setChecks('.fMonth',['202607']); setChecks('.fTen',['B']);
  $('cPk').checked=true; fire($('cPk'),'change');
  $('cSkip').checked=true; fire($('cSkip'),'change');
  results.push(['Lv7 DML full query', $('ctxDml').innerHTML.includes('checkout') && $('ctxDml').innerHTML.includes('toYYYYMM(ts) = 202607')]);
  results.push(['Lv7 DDL has PROJECTION', $('ctxDdl').innerHTML.includes('PROJECTION')]);
  results.push(['localStorage remembers', window.localStorage.getItem('mtx-lv') === '7']);

  // ---- Lv7 でのヘッダ連動(既存チェック) ----
  $('prjOn').checked = false; fire($('prjOn'), 'change');
  results.push(['prjOn OFF removes PROJECTION', !$('ctxDdl').innerHTML.includes('PROJECTION')]);
  $('prjOn').checked = true; fire($('prjOn'), 'change');
  $('fPk').value = '1'; fire($('fPk'), 'change');
  results.push(['PK=(tenant_id) in DDL', $('ctxDdl').innerHTML.includes('(tenant_id)</i>')]);
  results.push(['PK=1 → user pruning gone', $('why3').innerHTML.includes('PRIMARY KEY が (tenant_id) だけ')]);
  $('fPk').value = '2'; fire($('fPk'), 'change');
  $('fOb').value='pk'; fire($('fOb'), 'change');
  results.push(['ORDER BY omitted note', $('ctxDdl').innerHTML.includes('ORDER BY 省略')]);
  $('fOb').value='ts'; fire($('fOb'), 'change');
  results.push(['ORDER BY = PK + ts', $('ctxDdl').innerHTML.includes('(tenant_id, user_id, ts)')]);
  setChecks('.fMonth',[]);
  results.push(['no month → no partition pred', !$('ctxDml').innerHTML.includes('toYYYYMM')]);
  setChecks('.fMonth',['202607']);
  results.push(['month pred back', $('ctxDml').innerHTML.includes('toYYYYMM(ts) = 202607')]);
  setChecks('.fTen',[]);
  results.push(['user-only → prefix warning', $('why3').innerHTML.includes('接頭辞') && !$('ctxDml').innerHTML.includes('tenant_id')]);
  const cutN=()=>{ const m=$('cut3').textContent.match(/入\s*(\d+)\s*→\s*(\d+)/); return m?[+m[1],+m[2]]:[NaN,NaN]; };
  $('cSkip').checked=false; fire($('cSkip'),'change');
  { const [a,b]=cutN(); results.push(['user-only → no index pruning', a>0 && a===b]); }
  $('cSkip').checked=true; fire($('cSkip'),'change');
  click($('btnPrjMat')); // 既存Partはp_url無し→MATERIALIZEで構築
  { const [a,b]=cutN(); results.push(['user-only + url → projection rescues', b<a]); }
  setChecks('.fTen',['B']);
  $('rPk').value = '777'; fire($('rPk'), 'input');
  results.push(['user_id=777 in DML', $('ctxDml').innerHTML.includes('user_id = 777')]);

  $('cSkip').checked=false; fire($('cSkip'),'change'); // n4>0 を保証(ゼロ件分岐を避ける)
  results.push(['L7: physical read-path hidden', !$('sb5').innerHTML.includes('NVMe') && $('sb5').innerHTML.includes('Lv8 物理レイヤーで')]);
  $('cSkip').checked=true; fire($('cSkip'),'change');

  // ---- Lv7 での操作系 ----
  const e1 = errors.length;
  click($('btnInsert')); click($('udRun')); click($('qPlay'));
  results.push(['Lv7 actions error-free', errors.length === e1]);
  results.push(['stages populated', ['sb1','sb2','sb3','sb4','sb5','sb6'].every(id => $(id) && $(id).innerHTML.length > 0)]);
  d.querySelector('.lvchip[data-lv="8"]').dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  results.push(['jump → L8(物理)', d.body.classList.contains('L8')]);
  $('cSkip').checked=false; fire($('cSkip'),'change');
  results.push(['L7 exec ≠ L8 phys: read-path gated', $('sb5').innerHTML.includes('NVMe')]);
  $('cSkip').checked=true; fire($('cSkip'),'change');
  click($('smtToggle'));
  results.push(['OSS mode ENGINE(Lv8)', $('ctxDdl').innerHTML.includes('ReplicatedMergeTree')]);
  click($('smtToggle'));

  // ---- Lv1 に戻しても壊れない ----
  const e2 = errors.length;
  d.querySelector('.lvchip[data-lv="1"]').dispatchEvent(new window.MouseEvent('click', { bubbles: true }));
  click($('btnInsert'));
  results.push(['back to Lv1 error-free', errors.length === e2 && d.body.classList.contains('L1')]);

  results.push(['runtime errors total', errors.length ? errors.join(' /// ') : 'none']);
  results.forEach(([k, v]) => console.log(k + ':', v));
  const fails = results.filter(([k, v]) => v === false);
  console.log(fails.length ? 'FAILED: ' + fails.map(f => f[0]).join(', ') : 'ALL PASS');
  window.close();
  process.exit(fails.length ? 1 : 0);
}, 700);

'use strict';
/* =====================================================================
   MergeTree Explorer V6 — イベント駆動シーン
   sim(座標を知らない) → emit(意味イベント) → director → 活性シーン
   シーン: S0 テーブル層 / S1 ストレージ層 / S2 クエリ実行
   ===================================================================== */

/* ---------- 0. 定数と純関数 ---------- */
const GPR=4, LANES=3, LTW=568;
const TODAY='20260729', YDAY='20260728';
const CELL=34, CELLH=24, GAP=6, IDXW=104, SKW=88;
const LANE_COL=[0x6fe3ff,0xffb46f,0xc9a2ff];
const CELLBG={norm:0xeef0f2,hit:0xd3f9d8,dead:0xfafafa,skip:0xffe8cc,del:0xffe3e3,upd:0xd0ebff};
const CELLFG={norm:0x2a2e39,hit:0x2b8a3e,dead:0xc3c6cc,skip:0xd9480f,del:0xc92a2a,upd:0x1971c2};
function fmtT(v){ return (8+Math.floor(v/60))+':'+String(v%60).padStart(2,'0'); }
const SVC=['frontend','checkout','cart','auth','search'];
const svcOf=v=>SVC[v%SVC.length];
const SPANOF={frontend:'GET /product',checkout:'POST /checkout',cart:'POST /cart/add',auth:'POST /login',search:'GET /search'};
const TRWIN=15; // 縮尺ルール: 同じ15分窓の行は1つのトレースのスパン
const traceIdOf=(d,v)=>('00000000'+((((+d)*2654435761)^(Math.floor(v/TRWIN)*40503+0x9e37))>>>0).toString(16)).slice(-8);
const spanIdOf=(d,v)=>('00000000'+((((+d)*97561)^(v*7561+0x51f3))>>>0).toString(16)).slice(-8);
const durOf=v=>((v*7919)%420+12);
const statOf=v=>durOf(v)>380?'Error':'Ok';
function mkGranules(vals){ const gs=[]; for(let i=0;i<vals.length;i+=GPR) gs.push(vals.slice(i,i+GPR)); return gs; }
function partW(){ return GPR*(CELL+GAP)+24+IDXW+10+SKW+14; }
function partH(p){ return p.granules.length*(CELLH+GAP)+46; }
function seqRun(steps){ let t=0; for(const [d,fn] of steps){ t+=d; setTimeout(fn,t); } }

/* ---------- 1. sim(座標なし) ---------- */
let parts=[], seq=0, busy=false, projOn=false, mutSeq=6;
let mvH={}, mvD={}, mvT={}, trSeq=0;
let PRED=600, SVCF='', IDXT='set', ENG='mt';
function seedParts(){
  const mk=(vals,lvl,name,day)=>{ seq++; parts.push({id:seq,name,day,granules:mkGranules(vals.sort((a,b)=>a-b)),lvl,del:{},upd:{}}); };
  mk([60,180,600,120,500,880,60,900,700,240],1,YDAY+'_1_3_1',YDAY);
  mk([40,100,400,40,700,400,40,500,900,50,300,600],2,TODAY+'_1_5_2',TODAY);
  mk([61,300,600,62,500,901,63,902,620],0,TODAY+'_6_6_0',TODAY);
}
const actParts=()=>parts.filter(p=>!p.dying);
function liveVals(p){ return p.granules.flatMap((g,gi)=>g.filter((v,ci)=>!p.del[gi*GPR+ci])); }
function liveRows(){ return actParts().flatMap(p=>p.granules.flatMap((g,gi)=>g.filter((v,ci)=>!p.del[gi*GPR+ci]).map(v=>({v,d:p.day})))).sort((a,b)=>(a.d===b.d?a.v-b.v:(a.d<b.d?-1:1))); }
function tableRows(){ return actParts().reduce((s,p)=>s+p.granules.length,0)*8192; }

/* ---------- 2. イベントバス ---------- */
const EVLOG=[];
let routeEvent=function(){}; // director(IIFE内)が起動時に差し替える
function emit(t,pl){ const e=Object.assign({t},pl||{}); EVLOG.push(e); routeEvent(e); }

/* ---------- 3. 操作(すべて emit で語る) ---------- */
function doInsert(){
  if(busy) return toast('実行中です','warn');
  busy=true;
  // トレース形のバッチ: 2〜3トレース × 各2〜4スパン(同じ15分窓に届く)
  const vals=[];
  const nt=2+Math.floor(Math.random()*2);
  for(let k=0;k<nt;k++){
    const w=Math.floor(Math.random()*64);
    const ns=2+Math.floor(Math.random()*3);
    for(let j=0;j<ns;j++) vals.push(w*TRWIN+Math.floor(Math.random()*TRWIN));
  }
  const sorted=[...vals].sort((a,b)=>a-b);
  showSql('INSERT INTO otel_events (Timestamp, ServiceName, …) VALUES '+vals.slice(0,3).map(v=>"('"+fmtT(v)+"', '"+svcOf(v)+"', …)").join(', ')+' …  -- '+(mkGranules(sorted).length*8192).toLocaleString()+' イベント');
  showMsg('実行中…');
  emit('insert.arrive',{vals:[...vals]});
  seqRun([
    [1100,()=>emit('insert.sorted',{vals:sorted})],
    [1100,()=>{ seq++; const p={id:seq,name:TODAY+'_'+seq+'_'+seq+'_0',day:TODAY,granules:mkGranules(sorted),lvl:0,del:{},upd:{}}; parts.push(p); emit('part.born',{pid:p.id}); }],
    [900,()=>{ sorted.forEach(v=>{ const h=Math.floor(v/60)*60; mvH[h]=(mvH[h]||0)+2048; }); emit('mv.fire',{mv:'hourly_mv',src:'otel_events',dst:'hourly_counts'}); }],
    [800,()=>{ mvD[TODAY]=(mvD[TODAY]||0)+sorted.length*2048; emit('mv.fire',{mv:'daily_mv',src:'hourly_counts',dst:'daily_counts'}); }],
    [700,()=>{ sorted.forEach(v=>{ const tid=traceIdOf(TODAY,v);
      const r=mvT[tid]||(mvT[tid]={s:v,e:v,n:0});
      r.s=Math.min(r.s,v); r.e=Math.max(r.e,v); r.n++; });
      emit('mv.fire',{mv:'trace_id_mv',src:'otel_events',dst:'trace_id_ts'}); }],
    [700,()=>{ emit('table.rows',{total:tableRows()}); showMsg('Ok.('+(sorted.length*2048).toLocaleString()+' 行 → 新しい Part)'); busy=false; }],
  ]);
}
function doMerge(){
  if(busy) return toast('実行中です','warn');
  const byDay={}; actParts().forEach(p=>{ (byDay[p.day]=byDay[p.day]||[]).push(p); });
  const day=Object.keys(byDay).sort().reverse().find(d=>byDay[d].length>=2);
  if(!day) return toast('マージ対象がない。同じ Partition に Part が2つ以上必要(INSERT を)','warn');
  busy=true;
  const group=byDay[day];
  showSql("OPTIMIZE TABLE otel_events PARTITION '"+day+"'  -- Partition は跨がない");
  showMsg('OPTIMIZE 実行中…');
  emit('merge.start',{pids:group.map(p=>p.id),day});
  setTimeout(()=>{
    let vals=group.flatMap(p=>liveVals(p));
    if(ENG==='rmt'){ const seen=new Set(); vals=vals.filter(v=>!seen.has(v)&&seen.add(v)); }
    vals.sort((a,b)=>a-b);
    group.forEach(p=>p.dying=true);
    seq++; const lvl=Math.max(...group.map(p=>p.lvl))+1;
    const np={id:seq,name:day+'_'+Math.min(...group.map(p=>p.id))+'_'+seq+'_'+lvl,day,granules:mkGranules(vals),lvl,del:{},upd:{}};
    parts.push(np);
    setTimeout(()=>{ parts=parts.filter(p=>!p.dying); },900);
    emit('part.merged',{into:np.id,from:group.map(p=>p.id)});
    emit('table.rows',{total:tableRows()});
    showMsg('Ok.('+group.length+' Part → 1 Part'+(ENG==='rmt'?'、同じ Timestamp は置換':'')+')');
    busy=false;
  },1500);
}
function doDelete(){
  if(busy) return toast('実行中です','warn');
  const target=SVCF||'checkout';
  let n=0;
  actParts().forEach(p=>p.granules.forEach((g,gi)=>g.forEach((v,ci)=>{ const k=gi*GPR+ci; if(!p.del[k]&&svcOf(v)===target){ p.del[k]=1; n++; } })));
  if(!n) return toast("service='"+target+"' の行が残っていない",'warn');
  showSql("DELETE FROM otel_events WHERE ServiceName = '"+target+"'  -- 軽量削除(_row_exists でマスク)");
  emit('delete.mask',{svc:target,tiles:n});
  emit('table.rows',{total:tableRows()});
  showMsg('Ok.('+(n*2048).toLocaleString()+' 行をマスク。実体はマージで消える)');
}
function doUpdate(){
  if(busy) return toast('実行中です','warn');
  const target=SVCF||'frontend';
  busy=true; mutSeq++;
  showSql("ALTER TABLE otel_events UPDATE SpanAttributes['tier'] = 'vip' WHERE ServiceName = '"+target+"'  -- mutation");
  showMsg('mutation 実行中…(is_done = 0)');
  emit('mutation.start',{svc:target});
  setTimeout(()=>{
    let n=0;
    actParts().forEach(p=>{
      let hit=false;
      p.granules.forEach((g,gi)=>g.forEach((v,ci)=>{ if(svcOf(v)===target){ p.upd[gi*GPR+ci]=1; hit=true; n++; } }));
      if(hit) p.name=p.name.replace(/(_\d+)?$/,'')+'_'+mutSeq;
    });
    emit('mutation.rewrite',{svc:target,tiles:n});
    showMsg('Ok. mutation 完了(Part を丸ごと書き換え、水色 = 更新行)');
    busy=false;
  },1600);
}
function doProj(){
  if(projOn) return toast('PROJECTION は作成済み','warn');
  projOn=true; actParts().forEach(p=>p.hasProj=true);
  showSql('ALTER TABLE otel_events ADD PROJECTION by_service (SELECT * ORDER BY ServiceName)');
  emit('projection.built',{});
  showMsg('Ok.(各 Part の中に ServiceName 順のコピーが育つ)');
}
function SQLQ(){
  return "SELECT toStartOfHour(Timestamp) AS h, count() FROM otel_events WHERE Timestamp >= '2026-07-29 "+fmtT(PRED)+"'"+(SVCF?" AND ServiceName = '"+SVCF+"'":'')+' GROUP BY h ORDER BY h';
}
function doSelect(){
  if(busy) return toast('実行中です','warn');
  if(!actParts().length) return toast('Part がない。まず INSERT を','warn');
  busy=true;
  emit('query.start',{sql:SQLQ()});
  showSql(SQLQ()+';'); showMsg('実行中…');
  const act=actParts();
  const cut=act.filter(p=>p.day<TODAY).map(p=>p.id);
  const kept=act.filter(p=>p.day===TODAY);
  const plan=kept.map(p=>{
    const gs=p.granules; let from=gs.length;
    for(let i=0;i<gs.length;i++){ const hi=(i+1<gs.length)?gs[i+1][0]:961; if(hi>PRED){ from=i; break; } }
    return {pid:p.id,from,to:gs.length-1};
  });
  const scan=[], skipped=[];
  plan.forEach(pl=>{
    const p=kept.find(q=>q.id===pl.pid);
    for(let gi=pl.from;gi<=pl.to;gi++){
      const g=p.granules[gi];
      const anySvc=!SVCF||g.some((v,ci)=>svcOf(v)===SVCF&&!p.del[gi*GPR+ci]);
      const fp=IDXT==='bloom'&&SVCF&&!anySvc&&(gi%3===0);
      if(anySvc||fp) scan.push({pid:pl.pid,gi,fp}); else if(SVCF) skipped.push({pid:pl.pid,gi});
    }
  });
  const total=kept.reduce((s,p)=>s+p.granules.length,0)+act.filter(p=>p.day<TODAY).reduce((s,p)=>s+p.granules.length,0);
  seqRun([
    [900,()=>emit('prune.partition',{cut,kept:kept.map(p=>p.id)})],
    [1300,()=>emit('prune.primary',{plan})],
    [1300,()=>emit('prune.skip',{skipped,scanN:scan.length,total})],
    [1100,()=>{
      const queues=[[],[],[]];
      scan.forEach((s,i)=>queues[i%LANES].push(s));
      emit('scan.assign',{queues});
      let done=0;
      const finish=()=>{
        const agg={};
        scan.forEach(s=>{ const p=parts.find(x=>x.id===s.pid); if(!p) return;
          p.granules[s.gi].forEach((v,ci)=>{ if(v>=PRED&&(!SVCF||svcOf(v)===SVCF)&&!p.del[s.gi*GPR+ci]){ const h=Math.floor(v/60)*60; agg[h]=(agg[h]||0)+2048; } });
        });
        const rows=Object.keys(agg).map(Number).sort((a,b)=>a-b).map(h=>[fmtT(h)+'〜',agg[h].toLocaleString()]);
        emit('agg.merge',{lanes:LANES});
        setTimeout(()=>{
          emit('query.result',{rows,scanned:scan.length,total});
          showResult(['toStartOfHour(Timestamp)','count()'],rows,rows.length+' rows ・ 読んだのは '+scan.length+' / '+total+' granule');
          showMsg(rows.length+' rows'); busy=false;
        },1200);
      };
      if(!scan.length){ setTimeout(finish,700); return; }
      queues.forEach((list,li)=>{
        list.forEach((s,k)=>{ setTimeout(()=>{
          const p=parts.find(x=>x.id===s.pid); if(!p){ done++; return; }
          const hits=p.granules[s.gi].reduce((c,v,ci)=>c+((v>=PRED&&(!SVCF||svcOf(v)===SVCF)&&!p.del[s.gi*GPR+ci])?1:0),0);
          emit('scan.granule',{lane:li,pid:s.pid,gi:s.gi,hits:hits*2048,fp:s.fp});
          done++; if(done===scan.length) setTimeout(finish,900);
        },600+k*1000); });
      });
    }],
  ]);
}

/* ---------- 4. クライアント(DOM)と共有UI ---------- */
const sqlEl=document.getElementById('sqlbar'), cstatEl=document.getElementById('cstat');
const resEl=document.getElementById('resgrid'), rescEl=document.getElementById('rescard');
const toastEl=document.getElementById('toast');
let toastT=null, resShown=false;
function showSql(s){ sqlEl.textContent=s; }
function showMsg(m){ cstatEl.textContent=m; }
function showResult(cols,rows,note){
  let html='<table><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr>';
  rows.forEach(r=>{ html+='<tr>'+r.map(v=>'<td>'+v+'</td>').join('')+'</tr>'; });
  html+='</table>'; if(note) html+='<div class="msg">'+note+'</div>';
  resEl.innerHTML=html; resShown=true; cstatEl.textContent=note||'';
}
function toast(m,cls){
  toastEl.textContent=m; toastEl.className='show '+(cls||'');
  clearTimeout(toastT); toastT=setTimeout(()=>toastEl.className='',5200);
}
let curStage=0;
function setStage(n){
  curStage=n;
  const box=document.getElementById('steps');
  if(!n){ box.classList.remove('show'); return; }
  box.classList.add('show');
  box.querySelectorAll('.step').forEach(el=>{
    const s=+el.dataset.s;
    el.classList.toggle('on',s===n); el.classList.toggle('done',s<n);
  });
}

/* ---------- 5. Pixi と シーン ---------- */
(async()=>{
const cv=document.getElementById('cv');
const app=new PIXI.Application();
await app.init({canvas:cv,resizeTo:window,backgroundAlpha:0,antialias:true,resolution:Math.min(2,devicePixelRatio||1),autoDensity:true});
let W=innerWidth,H=innerHeight,MOB=W<=900;
const world=new PIXI.Container(); app.stage.addChild(world);
const paper=new PIXI.Graphics(); world.addChild(paper);
const INS_Y=104;
function STW(){ return W-(MOB?0:404)-12; }
function railOff(){ if(MOB){ world.x=0; world.y=Math.min(innerHeight*0.46,420)+64; } else { world.x=404; world.y=0; } }
addEventListener('resize',()=>{ W=innerWidth; H=innerHeight; MOB=W<=900; railOff(); if(cur) cur.layout(); });
railOff();

// DOM チップ(名前レイヤ)
const inspStyle=document.createElement('style');
inspStyle.textContent=`
#insp{position:fixed;top:0;right:-440px;width:420px;height:100%;z-index:20;background:#1f1f1cf8;
  border-left:1px solid #3a3a35;transition:right .22s;padding:14px 16px;overflow-y:auto;
  font-family:"Hiragino Kaku Gothic ProN",sans-serif;color:#e8eaf0;box-sizing:border-box}
#insp.open{right:0}
#insp h2{font-size:14px;margin:2px 0 4px;color:#faff69;font-family:Inconsolata,Menlo,monospace}
#insp .sub{font-size:11px;color:#98a0b3;margin-bottom:10px}
#insp pre{font:11.5px Inconsolata,Menlo,monospace;line-height:1.55;background:#161613;border:1px solid #33332e;
  border-radius:8px;padding:10px 12px;white-space:pre-wrap;word-break:break-all;color:#cdd2dc}
#insp pre .k{color:#8ecbe8}
#insp .note{font-size:11.5px;color:#b3b6bd;line-height:1.75;margin:10px 0}
#insp .x{position:absolute;top:10px;right:12px;cursor:pointer;color:#b3b6bd;font-size:14px;
  border:1px solid #414141;border-radius:6px;padding:2px 9px;background:#282828}
@media (max-width:700px){ #insp{width:100%;right:-100%} }`;
document.head.appendChild(inspStyle);
const insp=document.createElement('div'); insp.id='insp'; document.body.appendChild(insp);
function openInsp(html){
  insp.innerHTML='<div class="x" id="inspx">✕</div>'+html;
  insp.classList.add('open');
  document.getElementById('inspx').onclick=()=>insp.classList.remove('open');
}
addEventListener('keydown',e=>{ if(e.key==='Escape') insp.classList.remove('open'); });
function spanJSONHtml(r){
  const d=r.d, v=r.v, w=Math.floor(v/TRWIN);
  const kin=liveRows().filter(x=>x.d===d&&Math.floor(x.v/TRWIN)===w);
  const minV=Math.min.apply(null,kin.map(x=>x.v));
  const svc=svcOf(v), err=statOf(v)==='Error';
  const iso=m=>'2026-'+d.slice(4,6)+'-'+d.slice(6)+'T'+String(8+Math.floor(m/60)).padStart(2,'0')+':'+String(m%60).padStart(2,'0')+':00Z';
  const j={
    traceId:traceIdOf(d,v)+'…', spanId:spanIdOf(d,v)+'…',
    parentSpanId:(v===minV?'':spanIdOf(d,minV)+'…'),
    name:SPANOF[svc], kind:'SPAN_KIND_SERVER',
    startTime:iso(v), duration:durOf(v)+'ms',
    status:{code:err?'STATUS_CODE_ERROR':'STATUS_CODE_OK'},
    resource:{'service.name':svc,'deployment.environment':'prod'},
    attributes:{'http.request.method':SPANOF[svc].split(' ')[0],'url.path':SPANOF[svc].split(' ')[1],'http.response.status_code':err?500:200}
  };
  if(err) j.events=[{name:'exception',attributes:{'exception.type':'TimeoutError'}}];
  const pre=JSON.stringify(j,null,2).replace(/"([^"]+)":/g,'"<span class=k>$1</span>":');
  return '<h2>'+SPANOF[svc]+' <span style="color:#98a0b3">('+svc+')</span></h2>'
    +'<div class="sub">スパン1本の実体 = OTLP の JSON 表現(縮尺: ID は実際 32/16 桁)</div>'
    +'<pre>'+pre+'</pre>'
    +'<div class="note">収集器から届くのはこの入れ子ドキュメント(ワイヤは protobuf、JSON 表現も標準)。'
    +'取り込みで <b>列に解かれて</b> otel_events の1行になる — resource/attributes は Map 列。'
    +'ネストのまま置かず列にするから、列単位のスキャンと圧縮が効く。</div>'
    +'<div class="note">parentSpanId が空 = ルートスパン。同じ traceId を持つスパンは '+kin.length+' 本'
    +'(縮尺ルール: 同じ15分窓 = 1トレース)。trace_id_ts はこの traceId → 時間範囲の索引。</div>';
}
const chipStyle=document.createElement('style');
chipStyle.textContent=`
.chip3{position:absolute;transform:translate(-50%,-100%);background:#ffffffee;color:#2a2e39;
  font:600 10px "Inter","Hiragino Kaku Gothic ProN",sans-serif;letter-spacing:.03em;
  border:1px solid #c9ccd3;border-radius:4px;padding:2px 8px;white-space:nowrap;pointer-events:auto;cursor:pointer;
  box-shadow:0 1px 3px rgba(20,20,40,.08)}
.chip3:hover{border-color:#8a8f99}
.chip3 b{color:#2b8a3e;margin-left:5px;font-family:"Inconsolata",Menlo,monospace}
.chip3.warn{border-color:#e8a0a0;color:#c92a2a}
.chip3.mv{border-color:#c8b2ec;color:#5d4a86}`;
document.head.appendChild(chipStyle);
const ov=document.createElement('div');
ov.style.cssText='position:fixed;inset:0;pointer-events:none;z-index:6;overflow:hidden';
document.body.appendChild(ov);
const chips=new Map(); let frame=0;
function chip(key,cls,onClick){
  let c=chips.get(key);
  if(!c){ c=document.createElement('div'); c.className='chip3 '+(cls||''); c.style.pointerEvents='auto'; ov.appendChild(c); c.onclick=onClick||null; chips.set(key,c); }
  c.__seen=frame; return c;
}
function placeChip(c,x,y){ c.style.left=(x+world.x)+'px'; c.style.top=(y+world.y)+'px'; }
function textV(str,size,fill,mono=true){
  return new PIXI.Text({text:str,style:{fontFamily:mono?'Inconsolata,Menlo,monospace':'Inter,sans-serif',fontWeight:'600',fontSize:size,fill,lineHeight:size*1.42}});
}
function panel(g,w,h,fill,line,lw=1,r=6){ g.clear(); g.roundRect(0,0,w,h,r).fill(fill).stroke({width:lw,color:line}); return g; }
function dotted(g,x1,y1,x2,y2,col){
  const dx=x2-x1,dy=y2-y1,len=Math.hypot(dx,dy),n=Math.floor(len/11);
  for(let i=0;i<n;i+=2){ g.moveTo(x1+dx*i/n,y1+dy*i/n).lineTo(x1+dx*(i+1)/n,y1+dy*(i+1)/n); }
  g.stroke({width:1.5,color:col,alpha:0.55});
}

// 飛行体(シーン横断の共有プール。シーン切替で消す)
let fly=[];
const flyC=new PIXI.Container(); world.addChild(flyC);
function flyChip(txt,col,fx,fy,tx,ty,dt,done){
  const c=new PIXI.Container();
  const t=textV(txt,11.5,0xffffff); t.x=8; t.y=3;
  const g=new PIXI.Graphics(); g.roundRect(0,0,t.width+16,t.height+6,5).fill(col).stroke({width:1,color:0x00000022});
  c.addChild(g,t); flyC.addChild(c);
  fly.push({c,fx,fy,tx,ty,t:0,dt:dt||0.02,done});
}
function tickFly(){
  fly=fly.filter(f=>{
    f.t+=f.dt;
    const e=f.t<1?(1-Math.pow(1-f.t,3)):1;
    f.c.x=f.fx+(f.tx-f.fx)*e; f.c.y=f.fy+(f.ty-f.fy)*e-Math.sin(Math.PI*Math.min(1,f.t))*36;
    if(f.t>=1){ f.c.destroy({children:true}); if(f.done) f.done(); return false; }
    return true;
  });
}
function clearFly(){ fly.forEach(f=>f.c.destroy({children:true})); fly=[]; }

/* ----- Part 描画(S1/S2 共用) ----- */
function buildPartView(){
  const cont=new PIXI.Container();
  const bg=new PIXI.Graphics(); cont.addChild(bg);
  const tiles=new PIXI.Container(); cont.addChild(tiles);
  const idxT=textV('',10.5,0x8a7300); idxT.x=24+GPR*(CELL+GAP)+8; idxT.y=30; cont.addChild(idxT);
  const skT=textV('',10.5,0x5d4a86); skT.x=24+GPR*(CELL+GAP)+IDXW+14; skT.y=30; cont.addChild(skT);
  const hd=textV('',10.5,0x9a9a90); hd.x=10; hd.y=8; cont.addChild(hd);
  return {cont,bg,tiles,idxT,skT,hd,cells:new Map()};
}
function updatePartView(v,p,marks){
  // marks: {pruned, ranged:{from,to}, skip:Set(gi), scanned:Map(gi->hits), fp:Set(gi)}
  const w=partW(), h=partH(p);
  const pruned=marks&&marks.pruned;
  panel(v.bg,w,h,0xffffff,p.flash>0?0x2b8a3e:(pruned?0xe8e8e4:0xd9dbe0),p.flash>0?2:1,8);
  v.cont.alpha=pruned?0.45:1;
  v.hd.text=(p.hasProj?'⚡ projection ':'')+(pruned?'✂ partition 対象外':'');
  v.idxT.text='primary.idx\n'+p.granules.map(g=>fmtT(g[0])).join('\n');
  v.skT.text='skip('+IDXT+')\n'+p.granules.map(g=>[...new Set(g.map(x=>svcOf(x)[0]))].join('')).join('\n');
  p.granules.forEach((g,gi)=>{
    g.forEach((val,ci)=>{
      const key=gi+'_'+ci;
      let cell=v.cells.get(key);
      if(!cell){
        cell={g:new PIXI.Graphics(),t:textV('',10.5,0x2a2e39)};
        v.tiles.addChild(cell.g); v.tiles.addChild(cell.t);
        v.cells.set(key,cell);
      }
      const x=24+ci*(CELL+GAP), y=30+gi*(CELLH+GAP);
      let st='norm';
      const dk=gi*GPR+ci;
      if(p.del[dk]) st='del'; else if(p.upd[dk]) st='upd';
      if(marks){
        if(pruned) st='dead';
        else if(marks.skip&&marks.skip.has(gi)) st='skip';
        else if(marks.ranged&&(gi<marks.ranged.from)) st='dead';
        else if(marks.scanned&&marks.scanned.has(gi)){
          st=(val>=PRED&&(!SVCF||svcOf(val)===SVCF)&&!p.del[dk])?'hit':'dead';
        }
      }
      cell.g.clear(); cell.g.roundRect(x,y,CELL,CELLH,4).fill(CELLBG[st]).stroke({width:1,color:0xdde0e4});
      cell.t.text=fmtT(val); cell.t.style.fill=CELLFG[st];
      cell.t.x=x+CELL/2-cell.t.width/2; cell.t.y=y+CELLH/2-cell.t.height/2;
    });
  });
  if(p.flash>0) p.flash=Math.max(0,p.flash-0.02);
}

/* ----- シーン基盤 ----- */
function mkScene(name){
  return {name,cont:new PIXI.Container(),enter(){},exit(){},layout(){},onEvent(){},tick(){}};
}
const scenes={};

/* ===== S0 テーブル層 ===== */
scenes.S0=(()=>{
  const s=mkScene('S0');
  const ltblBg=new PIXI.Graphics(), ltblTx=textV('',12,0x2a2e39);
  ltblTx.x=14; ltblTx.y=7;
  const ltbl=new PIXI.Container(); ltbl.addChild(ltblBg,ltblTx); s.cont.addChild(ltbl);
  const edges=new PIXI.Graphics(); s.cont.addChild(edges);
  const tgt=[0,1,2].map(()=>{ const c=new PIXI.Container(); const bg=new PIXI.Graphics(); const tx=textV('',11.5,0x5d4a86); tx.x=10; tx.y=22; c.addChild(bg,tx); s.cont.addChild(c); return {c,bg,tx}; });
  const secL=textV('TABLE — 論理(このNodeのテーブルとMVパイプ)',11,0x9a9a90,false);
  const secR=textV('DERIVED TABLES — MV(パイプ)の書き込み先',11,0x9a9a90,false);
  s.cont.addChild(secL,secR);
  let flash=0, pipePulse={}, dispRows=0, tgtFlash=[0,0,0], lastShown=[];
  ltbl.eventMode='static'; ltbl.cursor='pointer';
  ltbl.on('pointertap',ev=>{
    const pos=ev.getLocalPosition(ltbl);
    const li=Math.floor((pos.y-7)/17)-1; // 0行目=ヘッダ
    if(li>=0&&li<lastShown.length) openInsp(spanJSONHtml(lastShown[li]));
    else toast('行をクリックするとスパンの実体(JSON)が見える');
  });
  const MX=()=>36+LTW+150;
  const MW=()=>Math.min(300,STW()-MX()-24);
  s.enter=()=>{ dispRows=tableRows(); };
  s.onEvent=e=>{
    if(e.t==='insert.arrive'){
      flash=1;
      flyChip('INSERT '+(e.vals.length*2048).toLocaleString()+' 行',0x2f9e44,STW()*0.45,-8,36+LTW*0.5,INS_Y+140,0.018);
    }
    else if(e.t==='table.rows'){ /* dispRows が tick で追いつく */ }
    else if(e.t==='mv.fire'){
      const i={hourly_mv:0,daily_mv:1,trace_id_mv:2}[e.mv];
      pipePulse[e.mv]=1; tgtFlash[i]=1;
    }
    else if(e.t==='delete.mask'||e.t==='mutation.rewrite'){ flash=1; }
    else { flash=Math.max(flash,0.6); } // 既定演出: テーブルが点滅
  };
  s.tick=()=>{
    const rows=liveRows(); const n=tableRows();
    dispRows+=(n-dispRows)*0.08; if(Math.abs(n-dispRows)<50) dispRows=n;
    const pick=Math.max(1,Math.floor(rows.length/9));
    const shown=rows.filter((_,i)=>i%pick===0).slice(0,9); lastShown=shown;
    ltblTx.text='Timestamp   TraceId    SpanId     SpanName         ServiceName  Duration  Status\n'
      +shown.map(r=>(r.d.slice(4,6)+'-'+r.d.slice(6)+' '+fmtT(r.v)).padEnd(12)+(traceIdOf(r.d,r.v)+'…').padEnd(11)+(spanIdOf(r.d,r.v)+'…').padEnd(11)+SPANOF[svcOf(r.v)].padEnd(17)+svcOf(r.v).padEnd(13)+(durOf(r.v)+'ms').padEnd(10)+statOf(r.v)).join('\n')
      +'\n… 全 '+Math.round(dispRows).toLocaleString()+' 行 ・ 他の列: ParentSpanId, SpanKind, Attributes(Map), Events…';
    const hh=7+(shown.length+2)*17+12;
    panel(ltblBg,LTW,hh,0xffffff,flash>0?0x2b8a3e:0xd9dbe0,flash>0?2:1);
    ltblBg.rect(1,1,LTW-2,24).fill(0xfff9db);
    ltbl.x=36; ltbl.y=INS_Y+60;
    if(flash>0) flash=Math.max(0,flash-0.015);
    secL.x=24; secL.y=INS_Y+28; secR.x=MX(); secR.y=INS_Y+28;
    // 書き込み先テーブル
    const eH=Object.keys(mvH).map(Number).sort((a,b)=>a-b);
    const eD=Object.keys(mvD).sort(); const eT=Object.keys(mvT);
    const data=[
      {nm:'TABLE hourly_counts',rows:eH.slice(0,3).map(h=>fmtT(h)+'〜  '+mvH[h].toLocaleString()),n:eH.length,extra:eH.length>3?'+'+(eH.length-3)+' 行':''},
      {nm:'TABLE daily_counts',rows:eD.slice(0,2).map(d=>d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6)+'  '+mvD[d].toLocaleString()),n:eD.length,extra:''},
      {nm:'TABLE trace_id_ts',rows:eT.slice(-3).map(k=>k+'…  '+fmtT(mvT[k].s)+'–'+fmtT(mvT[k].e)+' ・ '+mvT[k].n+' span'),n:eT.length,extra:eT.length>3?'+'+(eT.length-3)+' トレース':''},
    ];
    let y=INS_Y+78;
    const pipes=[
      {mv:'hourly_mv',lbl:'MV hourly_mv ▸ INSERT のたび集計 →',fromT:true},
      {mv:'daily_mv',lbl:'MV daily_mv ▸ カスケード',fromT:false},
      {mv:'trace_id_mv',lbl:'MV trace_id_mv ▸ 別キーで索引化 →',fromT:true},
    ];
    edges.clear();
    data.forEach((d,i)=>{
      const o=tgt[i];
      const body=d.rows.length?d.rows.join('\n')+(d.extra?'\n'+d.extra:''):'(空 — 作成後の INSERT だけ反映)';
      const hh2=26+Math.max(1,body.split('\n').length)*16+10;
      panel(o.bg,MW(),hh2,0xf9f6ff,tgtFlash[i]>0?0x9775fa:0xddd0f0,tgtFlash[i]>0?2:1);
      o.bg.rect(1,1,MW()-2,16).fill(0xeee6fb);
      o.tx.text=body; o.tx.style.fill=d.n?0x5d4a86:0xadb5bd;
      o.c.x=MX(); o.c.y=y;
      const c=chip('s0t'+i,'mv',()=>toast(d.nm+' もただのテーブル。MVはここへ書くパイプ'));
      c.innerHTML=d.nm+' <b>'+d.n+'行</b>';
      placeChip(c,MX()+MW()/2,y+2);
      if(tgtFlash[i]>0) tgtFlash[i]=Math.max(0,tgtFlash[i]-0.02);
      // パイプ(エッジ): ラベルは宛先カードの真上に縦積み(名前being被り防止)
      const pp=pipes[i]; const pulse=pipePulse[pp.mv]||0;
      const midY=y+hh2/2;
      if(pp.fromT) dotted(edges,36+LTW+10,midY,MX(),midY,pulse>0?0x7048c8:0x5d4a86);
      else dotted(edges,MX()+MW()/2,y-72,MX()+MW()/2,y-44,pulse>0?0x7048c8:0x5d4a86);
      const ec=chip('s0e'+i,'mv',null);
      ec.textContent=pp.lbl;
      ec.style.opacity=pulse>0?'1':'0.85';
      ec.style.transform=pulse>0?'translate(-50%,-100%) scale(1.1)':'translate(-50%,-100%)';
      placeChip(ec,MX()+MW()/2,y-20);
      if(pulse>0) pipePulse[pp.mv]=Math.max(0,pulse-0.012);
      y+=hh2+78;
    });
    const tc=chip('s0tbl','',()=>zoomTo('S1'));
    tc.innerHTML='TABLE otel_events · '+Math.round(dispRows).toLocaleString()+'行';
    placeChip(tc,36+LTW/2,INS_Y+62);
    const zc=chip('s0zoom','warn',()=>zoomTo('S1'));
    zc.textContent='⊕ 中を見る(Part / granule)';
    placeChip(zc,36+LTW/2,INS_Y+60+ (7+(shown.length+2)*17+12) +14);
  };
  return s;
})();

/* ===== S1 ストレージ層 ===== */
scenes.S1=(()=>{
  const s=mkScene('S1');
  const views=new Map();
  const frameG=new PIXI.Graphics(); s.cont.addChild(frameG);
  const strip=new PIXI.Graphics(); s.cont.addChild(strip);
  const stripTiles=new PIXI.Container(); s.cont.addChild(stripTiles);
  const secL=textV('STORAGE — otel_events の中(Partition ⊃ Part ⊃ granule)',11,0x9a9a90,false);
  s.cont.addChild(secL);
  const stubs=[0,1,2].map(()=>{ const g=new PIXI.Graphics(); s.cont.addChild(g); return g; });
  let stubPulse=[0,0,0], insBatch=null, insPhase='';
  function partPos(i){ // 縦積み、partitionで字下げ
    const xs={}; let y=INS_Y+66;
    const list=actParts();
    return list.map((p,k)=>{ const pos={p,x:24,y}; y+=partH(p)+18; return pos; })[i];
  }
  s.enter=()=>{ insBatch=null; insPhase=''; };
  s.exit=()=>{ stripTiles.removeChildren().forEach(c=>c.destroy()); };
  s.onEvent=e=>{
    if(e.t==='insert.arrive'){ insBatch=e.vals; insPhase='arrive'; }
    else if(e.t==='insert.sorted'){ insBatch=e.vals; insPhase='sorted'; }
    else if(e.t==='part.born'){
      const p=parts.find(x=>x.id===e.pid); if(p) p.flash=1;
      insPhase='fly';
      setTimeout(()=>{ insBatch=null; insPhase=''; },900);
    }
    else if(e.t==='mv.fire'){ const i={hourly_mv:0,daily_mv:1,trace_id_mv:2}[e.mv]; stubPulse[i]=1; }
    else if(e.t==='part.merged'){ const p=parts.find(x=>x.id===e.into); if(p) p.flash=1; }
    else if(e.t==='delete.mask'||e.t==='mutation.rewrite'){ actParts().forEach(p=>p.flash=Math.max(p.flash||0,0.5)); }
  };
  s.tick=()=>{
    secL.x=24; secL.y=INS_Y+2;
    // INSERT 帯
    strip.clear();
    if(insBatch){
      strip.roundRect(16,INS_Y+18,STW()-32,36,6).fill(0xf2f1ec).stroke({width:1,color:0xdad9d0});
      const sc=chip('s1strip','',null);
      sc.textContent=insPhase==='arrive'?'INSERT ブロック(届いた順)':insPhase==='sorted'?'ソート済み → 8,192行ごとに granule 区切り':'新しい Part へ';
      placeChip(sc,STW()/2,INS_Y+16);
      stripTiles.removeChildren().forEach(c=>c.destroy());
      const list=insPhase==='arrive'?insBatch:[...insBatch];
      list.forEach((v,i)=>{
        const t=textV(fmtT(v),10,0x2a2e39);
        const g=new PIXI.Graphics(); g.roundRect(0,0,CELL,20,4).fill(insPhase==='arrive'?0xfff3bf:0xd3f9d8).stroke({width:1,color:0xdad9d0});
        const c=new PIXI.Container(); c.addChild(g,t); t.x=CELL/2-t.width/2; t.y=3;
        c.x=30+i*(CELL+4); c.y=INS_Y+26; stripTiles.addChild(c);
      });
    } else { const sc=chips.get('s1strip'); if(sc){sc.remove(); chips.delete('s1strip');} stripTiles.removeChildren().forEach(c=>c.destroy()); }
    // Parts
    let y=INS_Y+66; const seen=new Set();
    actParts().forEach(p=>{
      let v=views.get(p.id);
      if(!v){ v=buildPartView(); views.set(p.id,v); s.cont.addChild(v.cont); }
      seen.add(p.id);
      v.cont.x=24; v.cont.y=y;
      updatePartView(v,p,null);
      const c=chip('s1p'+p.id,'',()=>toast('Part '+p.name+' — 不変。書き換えは常に新しい Part(_'+mutSeq+') が生まれる'));
      c.textContent=p.name+' · '+p.granules.length+'g';
      placeChip(c,24+partW()/2,y-2);
      y+=partH(p)+18;
    });
    views.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); views.delete(id); } });
    // TABLE 囲み
    frameG.clear();
    frameG.roundRect(14,INS_Y+50,partW()+22,Math.max(120,y-INS_Y-50-4),10).stroke({width:1.5,color:0xcfd2c8});
    const fc=chip('s1tbl','',()=>zoomTo('S0'));
    fc.textContent='TABLE otel_events(⊖ テーブル層へ)';
    placeChip(fc,24+partW()/2,y+4);
    // MV パイプ口
    const sx=24+partW()+70;
    ['hourly_mv','daily_mv','trace_id_mv'].forEach((mv,i)=>{
      const g=stubs[i]; const yy=INS_Y+90+i*72;
      g.clear();
      g.roundRect(sx,yy,120,40,8).fill(stubPulse[i]>0?0xeee6fb:0xf6f4fb).stroke({width:stubPulse[i]>0?2:1,color:stubPulse[i]>0?0x9775fa:0xddd0f0});
      dotted(g,24+partW()+10,yy+20,sx,yy+20,0x5d4a86);
      const c=chip('s1mv'+i,'mv',()=>zoomTo('S0'));
      c.textContent='▸ '+mv;
      placeChip(c,sx+60,yy+20);
      if(stubPulse[i]>0) stubPulse[i]=Math.max(0,stubPulse[i]-0.015);
    });
  };
  return s;
})();

/* ===== S2 クエリ実行 ===== */
scenes.S2=(()=>{
  const s=mkScene('S2');
  const views=new Map();
  const laneG=[0,1,2].map(()=>{ const g=new PIXI.Graphics(); s.cont.addChild(g); return g; });
  const secL=textV('STORAGE — 枝刈り',11,0x9a9a90,false);
  const secR=textV('COMPUTE — CPU コアのレーン',11,0x9a9a90,false);
  s.cont.addChild(secL,secR);
  const introG=new PIXI.Graphics(); const introT=textV('otel_events を Part 群に開く…',12,0x6b6b60,false);
  s.cont.addChild(introG,introT);
  let marks=new Map(), lanes=[], intro=0, resRows=null;
  function laneGeom(i){
    const px=24+partW()+36;
    const sw=STW();
    const w=Math.max(250,Math.min(520,sw-px-40-250));
    return {x:px,y:INS_Y+60+i*126,w,h:114};
  }
  function resGeom(){ const g=laneGeom(0); return {x:g.x+g.w+16,y:g.y+8,w:Math.max(210,Math.min(280,STW()-(g.x+g.w)-30))}; }
  s.enter=()=>{ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); intro=1; resRows=null; };
  s.exit=()=>{ setStage(0); };
  s.onEvent=e=>{
    if(e.t==='query.start'){ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); resRows=null; setStage(1); }
    else if(e.t==='prune.partition'){ e.cut.forEach(id=>{ marks.set(id,Object.assign(getM(id),{pruned:true})); }); toast('① Partition 枝刈り: 日付条件に合わない Partition は索引すら見ない'); }
    else if(e.t==='prune.primary'){ e.plan.forEach(pl=>{ marks.set(pl.pid,Object.assign(getM(pl.pid),{ranged:{from:pl.from,to:pl.to}})); }); toast('① primary.idx の二分探索で各 Part の読む範囲(granule range)を確定'); }
    else if(e.t==='prune.skip'){
      e.skipped.forEach(sk=>{ const m=getM(sk.pid); m.skip=m.skip||new Set(); m.skip.add(sk.gi); });
      setStage(2);
      toast('② skip idx('+IDXT+'): ServiceName が居ない granule を読む前に落とす'+(IDXT==='bloom'?'(bloom は偽陽性あり)':'(set は正確)'));
    }
    else if(e.t==='scan.assign'){ e.queues.forEach((q,i)=>{ lanes[i].q=q.map(x=>({pid:x.pid,gi:x.gi})); }); setStage(3); toast('③ 生き残り granule を '+LANES+' レーン(= max_threads のCPUスレッド)のキューへ配分'); }
    else if(e.t==='scan.granule'){
      const l=lanes[e.lane]; l.done++; l.sum+=e.hits; l.cur={pid:e.pid,gi:e.gi};
      const m=getM(e.pid); m.scanned=m.scanned||new Set(); m.scanned.add(e.gi);
    }
    else if(e.t==='agg.merge'){
      setStage(4);
      const rg=resGeom();
      lanes.forEach((l,i)=>{ const g=laneGeom(i); flyChip('Σ '+l.sum.toLocaleString(),[0x0e7490,0xb45309,0x7c3aed][i],g.x+g.w*0.86,g.y+g.h/2,rg.x+rg.w/2,rg.y+30,0.02); });
    }
    else if(e.t==='query.result'){ resRows=e.rows; }
  };
  function getM(pid){ if(!marks.has(pid)) marks.set(pid,{}); return marks.get(pid); }
  s.tick=()=>{
    secL.x=24; secL.y=INS_Y+28;
    const lg0=laneGeom(0); secR.x=lg0.x; secR.y=INS_Y+28;
    // 導入カット
    if(intro>0){
      intro=Math.max(0,intro-0.012);
      introG.clear();
      introG.roundRect(24,INS_Y+60,LTW*0.8,120,10).fill({color:0xffffff,alpha:intro}).stroke({width:1,color:0xd9dbe0,alpha:intro});
      introT.text='otel_events(論理)を Part 群(物理)に開く…';
      introT.alpha=intro; introT.x=44; introT.y=INS_Y+80;
    } else { introG.clear(); introT.alpha=0; }
    // Parts(左列)
    let y=INS_Y+60; const seen=new Set();
    actParts().forEach(p=>{
      let v=views.get(p.id);
      if(!v){ v=buildPartView(); views.set(p.id,v); s.cont.addChild(v.cont); }
      seen.add(p.id);
      v.cont.x=24; v.cont.y=y; v.cont.alpha=(1-intro);
      updatePartView(v,p,marks.get(p.id));
      const c=chip('s2p'+p.id,(marks.get(p.id)||{}).pruned?'warn':'',null);
      c.textContent=p.name+(marks.get(p.id)&&marks.get(p.id).pruned?' ✂':'');
      placeChip(c,24+partW()/2,y-2);
      y+=partH(p)+16;
    });
    views.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); views.delete(id); } });
    // レーン
    lanes.forEach((l,i)=>{
      const g=laneGeom(i), gr=laneG[i];
      gr.clear();
      gr.roundRect(g.x,g.y,g.w,g.h,10).fill(0xf4f7fa).stroke({width:1,color:0xdde4ea});
      gr.rect(g.x,g.y,g.w,3).fill(LANE_COL[i]);
      // キュー(未処理チップ)
      l.q.slice(l.done).slice(0,6).forEach((it,k)=>{
        gr.roundRect(g.x+12+k*30,g.y+14,26,18,4).fill(0xffffff).stroke({width:1.5,color:LANE_COL[i]});
      });
      // 処理中箱
      gr.roundRect(g.x+g.w*0.34,g.y+40,g.w*0.36,52,8).fill(0xe7f0f7);
      const c=chip('s2l'+i,'',null);
      c.innerHTML='lane '+(i+1)+' · '+l.done+'/'+l.q.length+' granule <b>Σ '+l.sum.toLocaleString()+'</b>';
      placeChip(c,g.x+g.w/2,g.y-2);
      const cc=chip('s2lc'+i,'',null);
      cc.textContent='キューから 1 granule ずつ直列に SIMD 評価';
      cc.style.opacity='0.66';
      placeChip(cc,g.x+g.w/2,g.y+g.h-8);
    });
    // RESULT カード(DOM)を配置
    const rg=resGeom(), show=(resShown&&curName==='S2');
    const st=(show?1:0)+':'+(rg.x|0);
    if(rescEl.__st!==st){ rescEl.__st=st; rescEl.style.display=show?'block':'none';
      rescEl.style.left=(rg.x+world.x)+'px'; rescEl.style.top=(rg.y+world.y)+'px'; rescEl.style.width=rg.w+'px'; }
  };
  return s;
})();

/* ---------- 6. director とズームナビ ---------- */
let cur=null, curName='', zoomBefore='S0';
const director={
  route(e){
    if(e.t==='query.start'&&curName!=='S2'){ zoomBefore=curName||'S0'; switchTo('S2'); }
    if(cur) cur.onEvent(e);
  },
};
routeEvent=e=>director.route(e);
function switchTo(name){
  if(curName===name) return;
  if(cur){ cur.exit(); world.removeChild(cur.cont); }
  // シーン所有のDOMチップを一掃(次フレームのsweepに任せず即時)
  chips.forEach((c,k)=>{ c.remove(); }); chips.clear();
  clearFly();
  rescEl.style.display='none'; rescEl.__st=null; // S2の持ち物は退場時に隠す
  curName=name; cur=scenes[name];
  world.addChild(cur.cont);
  world.addChild(flyC); // 最前面へ
  cur.enter(); renderCrumb();
}
function zoomTo(name){ if(busy&&name!==curName) return toast('実行中です。終わってから','warn'); switchTo(name); }
const crumbEl=document.getElementById('crumb');
function renderCrumb(){
  const seg=(lbl,on,fn)=>'<span class="cr'+(on?' on':'')+'" data-go="'+(fn||'')+'">'+lbl+'</span>';
  let html=seg('node-1',false,'')+'<span class="sep">›</span>';
  if(curName==='S0') html+=seg('tables',true);
  else if(curName==='S1') html+=seg('tables',false,'S0')+'<span class="sep">›</span>'+seg('otel_events',true);
  else html+=seg('tables',false,'S0')+'<span class="sep">›</span>'+seg('クエリ実行',true)+' <span class="cr" data-go="'+zoomBefore+'">⊖ ステージへ戻る</span>';
  crumbEl.innerHTML=html;
  crumbEl.querySelectorAll('.cr').forEach(el=>{
    const go=el.dataset.go;
    if(go) el.onclick=()=>zoomTo(go);
  });
}

/* ---------- 7. レール配線と初期化 ---------- */
document.getElementById('bIns').onclick=doInsert;
document.getElementById('bMerge').onclick=doMerge;
document.getElementById('bDel').onclick=doDelete;
document.getElementById('bUpd').onclick=doUpdate;
document.getElementById('bProj').onclick=doProj;
document.getElementById('bSel').onclick=doSelect;
document.getElementById('engSel').onchange=e=>{ ENG=e.target.value; toast(ENG==='rmt'?'ReplacingMergeTree: マージ時に同じ Timestamp の行を置換(重複排除)':'MergeTree: 追記のみ'); };
document.getElementById('idxSel').onchange=e=>{ IDXT=e.target.value; };
const predR=document.getElementById('predR');
if(predR){ predR.oninput=e=>{ PRED=+e.target.value; const lb=document.getElementById('predV'); if(lb) lb.textContent=fmtT(PRED); showSql(SQLQ()+';'); }; }
const svcSel=document.getElementById('svcSel');
if(svcSel){ svcSel.onchange=e=>{ SVCF=e.target.value; showSql(SQLQ()+';'); }; }
const bAmb=document.getElementById('bAmb');
let demoT=null;
if(bAmb) bAmb.onclick=()=>{
  if(demoT){ clearInterval(demoT); demoT=null; bAmb.textContent='▶ 自動再生'; return; }
  bAmb.textContent='⏸ 停止';
  const steps=['insert','select','insert','merge','select'];
  let i=0;
  demoT=setInterval(()=>{ if(busy) return; const st=steps[i%steps.length]; i++;
    if(st==='insert') doInsert(); else if(st==='merge') doMerge(); else doSelect(); },1200);
};
document.getElementById('bReset').onclick=()=>{
  if(busy) return toast('実行中です','warn');
  parts=[]; seq=0; mvH={}; mvD={}; mvT={}; trSeq=0; projOn=false; mutSeq=6;
  seedParts(); showDefault(); switchTo('S0');
  toast('初期状態に戻した');
};
function showDefault(){ showSql(SQLQ()+';'); showMsg('待機中'); resShown=false; }
function renderShelf(){
  const g=actParts().reduce((s,p)=>s+p.granules.length,0);
  const el0=document.getElementById('tc0');
  el0.innerHTML='<div class="tn">otel_events <b>'+(g*8192).toLocaleString()+'</b></div><div class="ts">Part ×'+actParts().length+' ・ granule ×'+g+' ・ ORDER BY Timestamp</div>';
  el0.onclick=()=>zoomTo('S1');
  const mk=(id,nm,rows,sub)=>{ const el=document.getElementById(id);
    el.innerHTML='<div class="tn">'+nm+' <b>'+rows+'行</b></div><div class="ts">'+sub+'</div>';
    el.onclick=()=>zoomTo('S0'); };
  mk('tc1','hourly_counts',Object.keys(mvH).length,'ORDER BY hour ・ 集計の受け皿');
  mk('tc2','daily_counts',Object.keys(mvD).length,'ORDER BY day');
  mk('tc3','trace_id_ts',Object.keys(mvT).length,'ORDER BY (TraceId, Start) ・ TraceId→時間範囲');
  const pk=(id,nm,path)=>{ const el=document.getElementById(id);
    el.innerHTML='<div class="tn">'+nm+'</div><div class="ts">'+path+'</div>';
    el.onclick=()=>zoomTo('S0'); };
  pk('mvp1','hourly_mv','otel_events → hourly_counts ・ GROUP BY hour');
  pk('mvp2','daily_mv','hourly_counts → daily_counts');
  pk('mvp3','trace_id_mv','otel_events → trace_id_ts ・ GROUP BY TraceId で min/max');
}
setInterval(renderShelf,400);

seedParts(); showDefault();
switchTo('S0');
const clientEl=document.getElementById('client');
app.ticker.add(()=>{
  frame++;
  if(frame%20===0) crumbEl.style.top=(clientEl.offsetHeight+20)+'px';
  paper.clear();
  paper.roundRect(6,8,STW(),innerHeight-16-(MOB?world.y:0),12).fill(0xf8f8f6).stroke({width:1,color:0xe2e2dd});
  if(cur) cur.tick();
  tickFly();
  chips.forEach((c,k)=>{ if(c.__seen!==frame){ c.remove(); chips.delete(k); } });
});
window.__v6={ get parts(){return parts;}, get scene(){return curName;}, EVLOG, zoomTo };
})().catch(e=>{ document.title='PXERR: '+(e&&e.message||e); console.error(e); });

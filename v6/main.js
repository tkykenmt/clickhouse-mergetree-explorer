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
const keyCmp=(a,b)=>{ const sa=svcOf(a),sb=svcOf(b); return sa<sb?-1:sa>sb?1:a-b; }; // ORDER BY (ServiceName, Timestamp)
const keySort=a=>[...a].sort(keyCmp);
const SVCTINT={frontend:0xe9eef8,checkout:0xfdeee2,cart:0xe6f4ea,auth:0xf3ecfa,search:0xfbf6df};
const SPANOF={frontend:'GET /product',checkout:'POST /checkout',cart:'POST /cart/add',auth:'POST /login',search:'GET /search'};
let Z=1.18;                          // 全体拡大率(SPは自動フィット+ピンチ可変)
let SCROLL=0, SCROLLX=0, YBASE=120, CONTENT_H=700;
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
let mvHParts=[], hpSeq=0;      // otel_traces_1h の Part(状態行の束)
let S1T='otel_traces', SCENE='S0'; // S1 の対象テーブル / 現在シーン
let PRED=600, SVCF='', IDXT='minmax', ENG='mt';
function seedParts(){
  const mk=(vals,lvl,name,day)=>{ seq++; parts.push({id:seq,name,day,granules:mkGranules(keySort(vals)),lvl,del:{},upd:{}}); };
  mk([60,181,602,123,504,880,64,903,700,247],1,YDAY+'_1_3_1',YDAY);
  mk([40,101,402,43,704,400,44,500,901,52,303,600],2,TODAY+'_1_5_2',TODAY);
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
  const sorted=keySort(vals);
  showMsg('collector が OTLP バッチ受信 → exporter が otel_raw へバルク INSERT');
  showSql('-- exporter が発行(ネイティブプロトコルのバルク INSERT)\nINSERT INTO otel_raw (Timestamp, TraceId, SpanId, SpanName, ServiceName, ResourceAttributes, SpanAttributes, Duration, StatusCode, ...) VALUES\n'+vals.slice(0,3).map(v=>"  ('2026-07-29 "+fmtT(v)+"', '"+traceIdOf(TODAY,v)+"…', '"+spanIdOf(TODAY,v)+"…', '"+SPANOF[svcOf(v)]+"', '"+svcOf(v)+"', {...}, {...}, "+durOf(v)+"000000, 'OK', ...)").join(',\n')+',\n  … -- '+(sorted.length*2048).toLocaleString()+' 行');
  emit('insert.arrive',{vals:[...vals]});
  seqRun([
    [1100,()=>emit('insert.sorted',{vals:sorted})],
    [1100,()=>{ seq++; const p={id:seq,name:TODAY+'_'+seq+'_'+seq+'_0',day:TODAY,granules:mkGranules(sorted),lvl:0,del:{},upd:{}}; parts.push(p); emit('part.born',{pid:p.id}); }],
    [900,()=>{ const hrs=new Set(sorted.map(v=>Math.floor(v/60)+'|'+svcOf(v))).size;
      emit('mv.fire',{mv:'otel_traces_1h_mv',src:'otel_traces',dst:'otel_traces_1h',
        inLbl:(sorted.length*2048).toLocaleString()+' 行のブロック',outLbl:'GROUP BY (hour, Service) → 状態 '+hrs+' 行'}); }],
    [1800,()=>{ const pr={};
      sorted.forEach(v=>{ const k=String(Math.floor(v/60)*60).padStart(4,'0')+'|'+svcOf(v);
        const r=mvH[k]||(mvH[k]={c:0,d:0}); r.c+=2048; r.d+=durOf(v)*2048;
        const q=pr[k]||(pr[k]={c:0,d:0}); q.c+=2048; q.d+=durOf(v)*2048; });
      mvHParts.push({id:++hpSeq,rows:pr,flash:1});
      emit('mv.applied',{mv:'otel_traces_1h_mv'}); }],
    [900,()=>emit('mv.fire',{mv:'otel_traces_1d_mv',src:'otel_traces_1h',dst:'otel_traces_1d',
        inLbl:'hourly の増分',outLbl:'day 合計 → 1 行'})],
    [1800,()=>{ mvD[TODAY]=(mvD[TODAY]||0)+sorted.length*2048; emit('mv.applied',{mv:'otel_traces_1d_mv'}); }],
    [900,()=>{ const tn=new Set(sorted.map(v=>Math.floor(v/TRWIN))).size;
      emit('mv.fire',{mv:'otel_traces_trace_id_ts_mv',src:'otel_traces',dst:'otel_traces_trace_id_ts',
        inLbl:(sorted.length*2048).toLocaleString()+' 行のブロック',outLbl:'GROUP BY TraceId → '+tn+' 行'}); }],
    [1800,()=>{ sorted.forEach(v=>{ const tid=traceIdOf(TODAY,v);
      const r=mvT[tid]||(mvT[tid]={s:v,e:v,n:0});
      r.s=Math.min(r.s,v); r.e=Math.max(r.e,v); r.n++; });
      emit('mv.applied',{mv:'otel_traces_trace_id_ts_mv'}); }],
    [900,()=>{ emit('table.rows',{total:tableRows()}); showMsg('Ok.('+(sorted.length*2048).toLocaleString()+' 行 → 新しい Part)'); busy=false; }],
  ]);
}
function doMerge(tgt2){
  if(busy) return toast('実行中です','warn');
  if(tgt2==='1h'||(SCENE==='S1'&&S1T==='otel_traces_1h')){
    if(mvHParts.length<2) return toast('Part が1つ以下。Collector からバッチを流すと増える','warn');
    busy=true;
    emit('merge.start',{pids:mvHParts.map(q=>q.id),day:TODAY});
    setTimeout(()=>{
      const fold={};
      mvHParts.forEach(q=>Object.keys(q.rows).forEach(k=>{ const r=fold[k]||(fold[k]={c:0,d:0}); r.c+=q.rows[k].c; r.d+=q.rows[k].d; }));
      mvHParts=[{id:++hpSeq,rows:fold,flash:1}];
      emit('part.merged',{into:hpSeq,from:[]});
      toast('AggregatingMergeTree のマージ: 同じ ORDER BY キー (hour, Service) の行は「状態」(sum と count)を結合して1行になる。avg はここでは計算されない — 読む側が avgMerge で確定する');
      busy=false;
    },1400);
    return;
  }
  const byDay={}; actParts().forEach(p=>{ (byDay[p.day]=byDay[p.day]||[]).push(p); });
  const day=Object.keys(byDay).sort().reverse().find(d=>byDay[d].length>=2);
  if(!day) return toast('マージ対象がない。同じ Partition に Part が2つ以上必要(INSERT を)','warn');
  busy=true;
  const group=byDay[day];
  showSql("OPTIMIZE TABLE otel_traces PARTITION '"+day+"'  -- Partition は跨がない");
  showMsg('OPTIMIZE 実行中…');
  emit('merge.start',{pids:group.map(p=>p.id),day});
  setTimeout(()=>{
    let vals=group.flatMap(p=>liveVals(p));
    if(ENG==='rmt'){ const seen=new Set(); vals=vals.filter(v=>!seen.has(v)&&seen.add(v)); }
    vals=keySort(vals);
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
  showSql("DELETE FROM otel_traces WHERE ServiceName = '"+target+"'  -- 軽量削除(_row_exists でマスク)");
  emit('delete.mask',{svc:target,tiles:n});
  emit('table.rows',{total:tableRows()});
  showMsg('Ok.('+(n*2048).toLocaleString()+' 行をマスク。実体はマージで消える)');
}
function doUpdate(){
  if(busy) return toast('実行中です','warn');
  const target=SVCF||'frontend';
  busy=true; mutSeq++;
  showSql("ALTER TABLE otel_traces UPDATE SpanAttributes['tier'] = 'vip' WHERE ServiceName = '"+target+"'  -- mutation");
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
  showSql('ALTER TABLE otel_traces ADD PROJECTION by_service (SELECT * ORDER BY ServiceName)');
  emit('projection.built',{});
  showMsg('Ok.(各 Part の中に ServiceName 順のコピーが育つ)');
}
function SQLQ(){
  return "SELECT toStartOfHour(Timestamp) AS h, count() FROM otel_traces WHERE Timestamp >= '2026-07-29 "+fmtT(PRED)+"'"+(SVCF?" AND ServiceName = '"+SVCF+"'":'')+' GROUP BY h ORDER BY h';
}
let LKUP=0; // otel_traces_trace_id_ts ルックアップカードの分だけレーンを下げる(S2が設定)
function laneRun(scan,total,tileHit,buildRows){
  const queues=[[],[],[]];
  scan.forEach((s,i)=>queues[i%LANES].push(s));
  emit('scan.assign',{queues});
  let done=0;
  const finish=()=>{
    const o=buildRows();
    emit('agg.merge',{lanes:LANES});
    setTimeout(()=>{
      emit('query.result',{rows:o.rows,scanned:scan.length,total});
      showResult(o.cols,o.rows,o.note);
      showMsg(o.rows.length+' rows'); busy=false;
      if(o.toast) toast(o.toast);
    },1200);
  };
  if(!scan.length){ setTimeout(finish,700); return; }
  queues.forEach((list,li)=>{
    list.forEach((s,k)=>{ setTimeout(()=>{
      const p=parts.find(x=>x.id===s.pid); if(!p){ done++; return; }
      const hits=p.granules[s.gi].reduce((c,v,ci)=>c+(tileHit(p,s.gi,ci,v)?1:0),0);
      emit('scan.granule',{lane:li,pid:s.pid,gi:s.gi,hits:hits*2048,fp:s.fp});
      done++; if(done===scan.length) setTimeout(finish,900);
    },600+k*1000); });
  });
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
  // 主キー = (ServiceName, Timestamp)。境界キー(各granuleの先頭)だけで判定する
  const plan=kept.map(p=>{
    const gs=p.granules, keep=[];
    for(let i=0;i<gs.length;i++){
      const lo=gs[i][0], nx=(i+1<gs.length)?gs[i+1][0]:null;
      const sameSvc=nx!=null&&svcOf(lo)===svcOf(nx);
      if(SVCF){
        const sHi=nx!=null?svcOf(nx):'\uffff';
        if(!(svcOf(lo)<=SVCF&&SVCF<=sHi)) continue;                    // サービスの接頭辞で除外
        if(sameSvc&&svcOf(lo)===SVCF&&nx<=PRED) continue;             // ブロック内は ts 単調 → 除外可
      } else {
        if(sameSvc&&nx<=PRED) continue;                                // 一般化排他: 単一サービス内だけ除外できる
      }
      keep.push(i);
    }
    return {pid:p.id,keep};
  });
  const scan=[], skipped=[];
  plan.forEach(pl=>{
    const p=kept.find(q=>q.id===pl.pid);
    pl.keep.forEach(gi=>{
      const g=p.granules[gi];
      if(IDXT==='minmax'&&Math.max.apply(null,g)<PRED){ skipped.push({pid:pl.pid,gi}); return; }
      scan.push({pid:pl.pid,gi});
    });
  });
  const total=kept.reduce((s,p)=>s+p.granules.length,0)+act.filter(p=>p.day<TODAY).reduce((s,p)=>s+p.granules.length,0);
  seqRun([
    [900,()=>emit('prune.partition',{cut,kept:kept.map(p=>p.id)})],
    [1300,()=>emit('prune.primary',{plan,note:SVCF?
      "① 主キー先頭は ServiceName — service='"+SVCF+"' のブロックを二分探索で特定(そのブロック内は ts も単調)":
      '① 主キー先頭は ServiceName — ts だけでは接頭辞が欠け、単一サービスに収まる granule しか排除できない(一般化排他)'})],
    [1300,()=>emit('prune.skip',{skipped,scanN:scan.length,total,note:IDXT==='minmax'?
      '② skip idx minmax(Timestamp): granule ごとの ts の min–max で「含み得ない」granule を読む前に落とす':
      '② skip idx なし: 主キーで残った granule を全部読む'})],
    [1100,()=>{
      const tileHit=(p,gi,ci,v)=>v>=PRED&&(!SVCF||svcOf(v)===SVCF)&&!p.del[gi*GPR+ci];
      laneRun(scan,total,tileHit,()=>{
        const agg={};
        scan.forEach(s=>{ const p=parts.find(x=>x.id===s.pid); if(!p) return;
          p.granules[s.gi].forEach((v,ci)=>{ if(tileHit(p,s.gi,ci,v)){ const h=Math.floor(v/60)*60; agg[h]=(agg[h]||0)+2048; } });
        });
        const rows=Object.keys(agg).map(Number).sort((a,b)=>a-b).map(h=>[fmtT(h)+'〜',agg[h].toLocaleString()]);
        return {cols:['toStartOfHour(Timestamp)','count()'],rows,note:rows.length+' rows ・ 読んだのは '+scan.length+' / '+total+' granule'};
      });
    }],
  ]);
}

function doTraceSelect(){
  if(busy) return toast('実行中です','warn');
  const keys=Object.keys(mvT);
  if(!keys.length) return toast('otel_traces_trace_id_ts が空。まず INSERT を(MV は作成後の INSERT だけ反映)','warn');
  const tid=keys[keys.length-1], r=mvT[tid], w=Math.floor(r.s/TRWIN);
  busy=true;
  const sql="WITH '"+tid+"…' AS tid, (SELECT min(Start) FROM otel_traces_trace_id_ts WHERE TraceId = tid) AS s, (SELECT max(End)+1 FROM otel_traces_trace_id_ts WHERE TraceId = tid) AS e SELECT Timestamp, ServiceName, SpanName FROM otel_traces WHERE Timestamp >= s AND Timestamp < e";
  emit('query.start',{sql,mode:'trace'});
  showSql(sql+';'); showMsg('実行中…');
  const act=actParts();
  const total=act.reduce((s2,p)=>s2+p.granules.length,0);
  seqRun([
    [1000,()=>emit('trace.lookup',{tid,s:r.s,e:r.e,n:r.n})],
    [2000,()=>{
      const cut=act.filter(p=>p.day!==TODAY).map(p=>p.id);
      emit('prune.partition',{cut,kept:act.filter(p=>p.day===TODAY).map(p=>p.id)});
    }],
    [1300,()=>{
      const kept=act.filter(p=>p.day===TODAY);
      const plan=kept.map(p=>({pid:p.id,keep:p.granules.map((_,i)=>i)}));
      const scan=[], skipped=[];
      kept.forEach(p=>{
        p.granules.forEach((g,gi)=>{
          const mn=Math.min.apply(null,g), mx=Math.max.apply(null,g);
          if(mx<r.s||mn>r.e) skipped.push({pid:p.id,gi}); else scan.push({pid:p.id,gi});
        });
      });
      emit('prune.primary',{plan,note:'① 主キーは (ServiceName, …, Timestamp) — TraceId も時刻も接頭辞に無いので、主キーでは絞れない'});
      setTimeout(()=>{
        emit('prune.skip',{skipped,scanN:scan.length,total,note:'② otel_traces_trace_id_ts がくれた Start–End を minmax(Timestamp) に当て、範囲外の granule を落とす'});
      },1300);
      setTimeout(()=>{
        const tileHit=(p,gi,ci,v)=>p.day===TODAY&&Math.floor(v/TRWIN)===w&&!p.del[gi*GPR+ci];
        laneRun(scan,total,tileHit,()=>{
          const spans=[];
          scan.forEach(s2=>{ const p=parts.find(x=>x.id===s2.pid); if(!p) return;
            p.granules[s2.gi].forEach((v,ci)=>{ if(tileHit(p,s2.gi,ci,v)) spans.push(v); });
          });
          spans.sort((a,b)=>a-b);
          const rows=spans.map(v=>[fmtT(v),svcOf(v),SPANOF[svcOf(v)]]);
          return {cols:['Timestamp','ServiceName','SpanName'],rows,
            note:rows.length+' spans ・ 読んだのは '+scan.length+' / '+total+' granule',
            toast:'otel_traces_trace_id_ts が Start–End をくれるから、主キー(Timestamp)の枝刈りが効いて '+scan.length+' / '+total+' granule で済む。TraceId 直では時刻の手掛かりが無く全 granule が候補になる'};
        });
      },2500);
    }],
  ]);
}
/* ---------- 4. クライアント(DOM)と共有UI ---------- */
let CURSQL=''; const cstatEl=document.getElementById('cstat');
const resEl=document.getElementById('resgrid'), rescEl=document.getElementById('rescard');
const toastEl=document.getElementById('toast');
let toastT=null, resShown=false;
function showSql(s){ CURSQL=s; }
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
world.scale.set(Z);
const paper=new PIXI.Graphics(); world.addChild(paper);
const INS_Y=16;
function STW(){ return (W-26)/Z; }
function fitZ(){ Z=(W<=900)?Math.max(0.5,Math.min(1.18,(W-26)/690)):1.18; world.scale.set(Z); }
function railOff(){ fitZ(); }
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
const TBLINFO={
  otel_raw:{t:'TABLE otel_raw',
    d:'コレクタが書き込む受け口。ENGINE = Null は /dev/null で、行を1行も溜めない — ただし着いたブロックに対して MV は発火する。既定の ClickStack は otel_traces へ直書きだが、変換を挟む本番構成では、この「手前のワイドテーブル」を置くのが定石(公式 schema-design、ClickHouse 自身の 19PiB ログ基盤も同型)。',
    ddl:`CREATE TABLE otel_raw
(
  -- otel_traces と同じワイドスキーマ
  Timestamp DateTime64(9),
  TraceId String, SpanId String, ...,
  ResourceAttributes Map(...),
  SpanAttributes     Map(...)
)
ENGINE = Null  -- 溜めない。MVだけ発火`,
    live:()=>'0 行(常に) ・ Part なし'},
  transform_mv:{t:'MATERIALIZED VIEW transform_mv',
    d:'受け口に着いたブロックを列に解いて(型付け・抽出・整形)otel_traces へ書く変換トリガ。スキーマを変えたくなったら ALTER TABLE ... MODIFY QUERY でこの MV を差し替える。',
    ddl:`CREATE MATERIALIZED VIEW transform_mv
TO otel_traces AS
SELECT
  Timestamp, TraceId, SpanId,
  ParentSpanId, SpanName, SpanKind,
  ServiceName,
  ResourceAttributes, SpanAttributes,
  Duration, StatusCode
FROM otel_raw`,
    live:()=>'発火 '+EVLOG.filter(e=>e.t==='insert.arrive').length+' 回'},
  otel_traces:{t:'TABLE otel_traces',
    d:'OTel のスパンを列に解いて置くワイドテーブル。到着は JSON(行クリックで見える)、格納は型付き列+Map 列。ORDER BY の先頭が ServiceName なので、時刻だけの検索は主キーが効かず minmax skip idx が救う。',
    ddl:`CREATE TABLE otel_traces
(
  Timestamp      DateTime64(9) CODEC(Delta, ZSTD),
  TraceId        String,
  SpanId         String,
  ParentSpanId   String,
  SpanName       LowCardinality(String),
  SpanKind       LowCardinality(String),
  ServiceName    LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String),
  SpanAttributes     Map(LowCardinality(String), String),
  Duration       UInt64,
  StatusCode     LowCardinality(String),
  Events Nested(Timestamp DateTime64(9),
                Name LowCardinality(String), ...),
  INDEX idx_ts Timestamp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ServiceName, SpanName,
          toDateTime(Timestamp))`,
    live:()=>{ const g=actParts().reduce((s,q)=>s+q.granules.length,0);
      return 'Part ×'+actParts().length+' ・ granule ×'+g+' ・ '+(g*8192).toLocaleString()+' 行'; }},
  otel_traces_1h:{t:'TABLE otel_traces_1h',
    d:'チャート・アラート加速用のロールアップ。AggregatingMergeTree に count と avgState(Duration) の部分集計状態を積み、HyperDX が EXPLAIN ESTIMATE で最小コストのビューを自動選択する(2025-12 に ClickStack 本体機能化)。実物のドキュメント例は1分粒度(otel_traces_1m)— この画面は縮尺で1時間。',
    ddl:`CREATE TABLE otel_traces_1h
(
  Timestamp   DateTime,
  ServiceName LowCardinality(String),
  count       SimpleAggregateFunction(sum, UInt64),
  avg__Duration AggregateFunction(avg, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (Timestamp, ServiceName)`,
    live:()=>{ const rows=mvHParts.reduce((s,q)=>s+Object.keys(q.rows).length,0); return rows+' 行(状態) / キー '+Object.keys(mvH).length+' ・ Part ×'+mvHParts.length; }},
  otel_traces_1d:{t:'TABLE otel_traces_1d',
    d:'階層ロールアップ(1h → 1d)の受け皿。ClickStack の既定には無い一般パターン。ソースは otel_traces_1h で、MV の宛先への書き込みもただの INSERT なので連鎖発火する(カスケード)。',
    ddl:`CREATE TABLE otel_traces_1d
(
  day    Date,
  count  SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY day`,
    live:()=>Object.keys(mvD).length+' 行'},
  otel_traces_trace_id_ts:{t:'TABLE otel_traces_trace_id_ts',
    d:'otel_traces_trace_id_ts_mv の書き込み先。本体の ORDER BY に TraceId が(実質)無い問題を、TraceId 先頭の別テーブルで解く索引。TraceId 検索はまずここを引いて Start–End を得る。',
    ddl:`CREATE TABLE otel_traces_trace_id_ts
(
  TraceId String,
  Start   DateTime64(9),
  End     DateTime64(9),
  INDEX idx_trace_id TraceId
    TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (TraceId, toUnixTimestamp(Start))`,
    live:()=>Object.keys(mvT).length+' 行(トレース)'},
  otel_traces_1h_mv:{t:'MATERIALIZED VIEW otel_traces_1h_mv',
    d:'テーブルではなくトリガ。otel_traces への INSERT のたび、挿入ブロックだけを (hour, Service) で畳み、部分集計状態(avgState 等)を otel_traces_1h へ書く。読む側は avgMerge で畳み直す。作成前の過去データは対象外(バックフィルは Null テーブル経由)。同じソースに複数の MV があるとき、既定(parallel_view_processing=0)は uuid 順に1本ずつ直列発火 — この画面の順番再生は実物どおり。=1 で並列化できるが順序保証は消える。',
    ddl:`CREATE MATERIALIZED VIEW otel_traces_1h_mv
TO otel_traces_1h AS
SELECT
  toStartOfHour(Timestamp) AS Timestamp,
  ServiceName,
  count() AS count,
  avgState(Duration) AS avg__Duration
FROM otel_traces
GROUP BY Timestamp, ServiceName`,
    live:()=>'発火 '+EVLOG.filter(e=>e.t==='mv.fire'&&e.mv==='otel_traces_1h_mv').length+' 回'},
  otel_traces_1d_mv:{t:'MATERIALIZED VIEW otel_traces_1d_mv',
    d:'ソースは otel_traces_1h。MV の宛先への書き込みもただの INSERT なので、そこに付いた MV が連鎖して発火する(カスケード)。ClickStack の既定セットには無い、一般の階層ロールアップ。',
    ddl:`CREATE MATERIALIZED VIEW otel_traces_1d_mv
TO otel_traces_1d AS
SELECT
  toDate(Timestamp) AS day,
  sum(count) AS count
FROM otel_traces_1h
GROUP BY day`,
    live:()=>'発火 '+EVLOG.filter(e=>e.t==='mv.fire'&&e.mv==='otel_traces_1d_mv').length+' 回'},
  otel_traces_trace_id_ts_mv:{t:'MATERIALIZED VIEW otel_traces_trace_id_ts_mv',
    d:'同じワイドテーブルを別キー(TraceId)で引けるように解す MV。GROUP BY は挿入ブロック内でしか畳まれないため、同じ TraceId の行が複数積まれ、読む側が min/max で畳む。',
    ddl:`CREATE MATERIALIZED VIEW otel_traces_trace_id_ts_mv
TO otel_traces_trace_id_ts AS
SELECT
  TraceId,
  min(Timestamp) AS Start,
  max(Timestamp) AS End
FROM otel_traces
WHERE TraceId != ''
GROUP BY TraceId`,
    live:()=>'発火 '+EVLOG.filter(e=>e.t==='mv.fire'&&e.mv==='otel_traces_trace_id_ts_mv').length+' 回'},
};
function tableInsp(k){
  const o=TBLINFO[k]; if(!o) return;
  openInsp('<h2>'+o.t+'</h2><div class="sub">'+o.live()+'</div>'
    +'<div class="note">'+o.d+'</div><pre>'+o.ddl+'</pre>');
}
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
    +'取り込みで <b>列に解かれて</b> otel_traces の1行になる — resource/attributes は Map 列。'
    +'ネストのまま置かず列にするから、列単位のスキャンと圧縮が効く。</div>'
    +'<div class="note">parentSpanId が空 = ルートスパン。同じ traceId を持つスパンは '+kin.length+' 本'
    +'(縮尺ルール: 同じ15分窓 = 1トレース)。otel_traces_trace_id_ts はこの traceId → 時間範囲の索引。</div>';
}
const chipStyle=document.createElement('style');
chipStyle.textContent=`
.chip3{position:absolute;transform:translate(-50%,-100%);background:#ffffffee;color:#2a2e39;
  font:600 11.5px "Inter","Hiragino Kaku Gothic ProN",sans-serif;letter-spacing:.03em;
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
function placeChip(c,x,y){ c.style.left=(x*Z+world.x)+'px'; c.style.top=(y*Z+world.y)+'px'; }
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
function flyChip(txt,col,fx,fy,tx,ty,dt,done,flat){
  const c=new PIXI.Container();
  const t=textV(txt,11.5,0xffffff); t.x=8; t.y=3;
  const g=new PIXI.Graphics(); g.roundRect(0,0,t.width+16,t.height+6,5).fill(col).stroke({width:1,color:0x00000022});
  c.addChild(g,t); flyC.addChild(c);
  fly.push({c,fx,fy,tx,ty,t:0,dt:dt||0.02,done,flat});
}
function tickFly(){
  const dones=[]; // done内のflyChip追加がfilter再代入で消えないよう、後で実行(V2の教訓)
  fly=fly.filter(f=>{
    f.t+=f.dt;
    const e=f.t<1?(1-Math.pow(1-f.t,3)):1;
    f.c.x=f.fx+(f.tx-f.fx)*e; f.c.y=f.fy+(f.ty-f.fy)*e-(f.flat?0:Math.sin(Math.PI*Math.min(1,f.t))*36);
    if(f.t>=1){ f.c.destroy({children:true}); if(f.done) dones.push(f.done); return false; }
    return true;
  });
  dones.forEach(fn=>fn());
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
function updatePartView(v,p,marks,hitFn){
  // marks: {pruned, ranged:{from,to}, skip:Set(gi), scanned:Map(gi->hits), fp:Set(gi)}
  const w=partW(), h=partH(p);
  const pruned=marks&&marks.pruned;
  panel(v.bg,w,h,0xffffff,p.flash>0?0x2b8a3e:(pruned?0xe8e8e4:0xd9dbe0),p.flash>0?2:1,8);
  v.cont.alpha=pruned?0.45:1;
  v.hd.text=(p.hasProj?'⚡ projection ':'')+(pruned?'✂ partition 対象外':'');
  v.idxT.text='primary.idx\n'+p.granules.map(g=>svcOf(g[0]).slice(0,2)+' '+fmtT(g[0])).join('\n');
  v.skT.text=IDXT==='minmax'?('minmax(ts)\n'+p.granules.map(g=>fmtT(Math.min.apply(null,g))+'–'+fmtT(Math.max.apply(null,g))).join('\n')):'skip idx\n(なし)';
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
        else if(marks.pkKeep&&!marks.pkKeep.has(gi)) st='dead';
        else if(marks.skip&&marks.skip.has(gi)) st='skip';
        else if(marks.scanned&&marks.scanned.has(gi)){
          const hit=hitFn?hitFn(p,gi,ci,val):(val>=PRED&&(!SVCF||svcOf(val)===SVCF)&&!p.del[dk]);
          st=hit?'hit':'dead';
        }
      }
      const bgc=(st==='norm')?(SVCTINT[svcOf(val)]||CELLBG.norm):CELLBG[st];
      cell.g.clear(); cell.g.roundRect(x,y,CELL,CELLH,4).fill(bgc).stroke({width:1,color:0xdde0e4});
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
  ltblTx.x=14; ltblTx.y=28;
  const ltTitle=textV('TABLE otel_traces',11.5,0x6b5d00); ltTitle.x=12; ltTitle.y=5;
  const ltCount=textV('',11.5,0x2b8a3e); ltCount.y=5;
  const ltMg=textV('⇄',13,0x6b5d00); ltMg.y=4;
  const ltbl=new PIXI.Container(); ltbl.addChild(ltblBg,ltblTx,ltTitle,ltCount,ltMg); s.cont.addChild(ltbl);
  const edges=new PIXI.Graphics(); s.cont.addChild(edges);
  const colBg=new PIXI.Graphics();
  const colTitle=textV('OTel Collector',11.5,0xe8e6dc); colTitle.x=12; colTitle.y=8;
  const colHint=textV('OTLP/HTTP ▸ クリックでバッチ送信',10.5,0x9a9a90); colHint.y=9;
  const col=new PIXI.Container(); col.addChild(colBg,colTitle,colHint); s.cont.addChild(col);
  col.eventMode='static'; col.cursor='pointer';
  col.on('pointertap',()=>{ if(busy){ toast('実行中です','warn'); return; } doInsert(); });
  const rawBg=new PIXI.Graphics(), rawTx=textV('',11.5,0x8a8a80);
  rawTx.x=12; rawTx.y=26;
  const rawTitle=textV('TABLE otel_raw',11.5,0x77771f); rawTitle.x=12; rawTitle.y=4;
  const rawCount=textV('0 行(常に)',11.5,0x8a8a80); rawCount.y=4;
  const raw=new PIXI.Container(); raw.addChild(rawBg,rawTx,rawTitle,rawCount); s.cont.addChild(raw);
  raw.eventMode='static'; raw.cursor='pointer';
  raw.on('pointertap',()=>tableInsp('otel_raw'));
  let rawFlash=0, rawPulse=0;
  const TKEYS=['otel_traces_1h','otel_traces_1d','otel_traces_trace_id_ts'];
  const tgt=[0,1,2].map((_,i)=>{ const c=new PIXI.Container(); const bg=new PIXI.Graphics(); const tx=textV('',11.5,0x2a2e39); tx.x=10; tx.y=26; const tt=textV('',11,0x6b5d00); tt.x=10; tt.y=5; const tcn=textV('',11,0x2b8a3e); tcn.y=5; const tmg=textV(i===0?'⇄':'',12.5,0x6b5d00); tmg.y=4; c.addChild(bg,tx,tt,tcn,tmg); s.cont.addChild(c);
    c.eventMode='static'; c.cursor='pointer';
    c.on('pointertap',ev=>{ const p2=ev.getLocalPosition(c); if(i===0&&p2.y<=22&&p2.x>(c.__w||270)-36){ doMerge('1h'); } else tableInsp(TKEYS[i]); });
    return {c,bg,tx,tt,tcn,tmg}; });
  const secL=textV('',11,0x9a9a90,false);
  const secR=textV('',11,0x9a9a90,false);
  s.cont.addChild(secL,secR);
  let flash=0, pipePulse={}, dispRows=0, tgtFlash=[0,0,0], lastShown=[], G=null;
  ltbl.eventMode='static'; ltbl.cursor='pointer';
  ltbl.on('pointertap',ev=>{
    const pos=ev.getLocalPosition(ltbl);
    if(pos.y<=24){ if(pos.x>LTW-40){ doMerge(); } else tableInsp('otel_traces'); return; }
    const li=Math.floor((pos.y-28)/17)-1; // 0行目=ヘッダ
    if(li>=0&&li<lastShown.length) openInsp(spanJSONHtml(lastShown[li]));
    else toast('行をクリックするとスパンの実体(JSON)が見える');
  });
  const MX=()=>36+LTW+150;
  const MW=()=>Math.min(300,STW()-MX()-24);
  s.enter=()=>{ dispRows=tableRows(); };
  s.onEvent=e=>{
    if(e.t==='insert.arrive'){
      const cx2=G?G.trX:171, cy2=G?G.rawCy:INS_Y+120;
      flyChip('OTLP バッチ '+(e.vals.length*2048).toLocaleString()+' 行(JSON)',0x2f9e44,cx2,INS_Y+40,cx2,cy2,0.016,()=>{ rawFlash=1; },true);
    }
    else if(e.t==='insert.sorted'){
      rawPulse=1;
      if(G) flyChip('列に解いてソート',0x7048c8,G.trX,G.rawBtm,G.trX,G.tblY-6,0.02,()=>{ flash=1; },true);
    }
    else if(e.t==='table.rows'){ /* dispRows が tick で追いつく */ }
    else if(e.t==='mv.fire'){
      pipePulse[e.mv]=1;
      const sg=G&&G.segs.find(s2=>s2.mv===e.mv);
      if(sg){
        const midY=(sg.y1+sg.y2)/2;
        flyChip(e.inLbl||'ブロック',0x2f9e44,sg.x,sg.y1-26,sg.x,midY,0.016,()=>{
          pipePulse[e.mv]=1; // 加工の瞬間にもう一度脈動
          flyChip(e.outLbl||'集計行',0x7048c8,sg.x,midY,sg.x,sg.dcy,0.016,null,true);
        },true);
      }
    }
    else if(e.t==='mv.applied'){
      const i={otel_traces_1h_mv:0,otel_traces_1d_mv:1,otel_traces_trace_id_ts_mv:2}[e.mv];
      if(i!=null) tgtFlash[i]=1;
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
    const hh=28+(shown.length+2)*17+10;
    // 受け口(ENGINE = Null)
    const colY=INS_Y+6, colH=34;
    panel(colBg,LTW,colH,0x26261f,0x4a4a40,1,8);
    colHint.x=LTW-12-colHint.width;
    col.x=36; col.y=colY;
    const rawY=colY+colH+30, rawH=50;
    panel(rawBg,LTW,rawH,0xfcfcf8,rawFlash>0?0x2b8a3e:0xdcdcd2,rawFlash>0?2:1);
    rawBg.rect(1,1,LTW-2,22).fill(0xf1f1e8);
    rawCount.x=LTW-12-rawCount.width;
    rawTx.text='ENGINE = Null — 行を溜めない受け口。着いたブロックに MV だけが発火する';
    raw.x=36; raw.y=rawY;
    if(rawFlash>0) rawFlash=Math.max(0,rawFlash-0.02);
    panel(ltblBg,LTW,hh,0xffffff,flash>0?0x2b8a3e:0xd9dbe0,flash>0?2:1);
    ltblBg.rect(1,1,LTW-2,24).fill(0xfff9db);
    ltMg.x=LTW-24;
    ltCount.text=Math.round(dispRows).toLocaleString()+' 行'; ltCount.x=LTW-36-ltCount.width;
    ltbl.x=36; ltbl.y=rawY+rawH+52;
    if(flash>0) flash=Math.max(0,flash-0.015);
    secL.x=24; secL.y=INS_Y+28; secR.x=MX(); secR.y=INS_Y+28;
    // 書き込み先テーブル: テーブル → MV(線上) → 先テーブル を一直線に
    const eH=Object.keys(mvH).sort();
    const eD=Object.keys(mvD).sort(); const eT=Object.keys(mvT);
    G={rawCy:rawY+26,rawBtm:rawY+rawH,trX:36+Math.floor((LTW-28)/2)/2,tblY:ltbl.y,segs:[]};
    // 派生テーブル: データの流れは上→下(INSERT は落ちて積もる)
    const colW=Math.floor((LTW-28)/2), lx=36, rx2=36+colW+28;
    const lcx=lx+colW/2, rcx=rx2+colW/2;
    const topY=ltbl.y+hh+68;
    const bodies=[
      {key:'otel_traces_1h',nm:'otel_traces_1h',hdr:'hour   Service    count   avg(Dur)',rows:eH.slice(0,3).map(k=>{const q=mvH[k]; const hs=+k.split('|')[0], sv=k.split('|')[1]; return fmtT(hs).padEnd(7)+sv.padEnd(11)+q.c.toLocaleString().padEnd(8)+Math.round(q.d/q.c)+'ms';}),n:eH.length,extra:eH.length>3?'+'+(eH.length-3)+' 行':''},
      {key:'otel_traces_1d',nm:'otel_traces_1d',hdr:'day      count()',rows:eD.slice(0,2).map(d2=>(d2.slice(4,6)+'-'+d2.slice(6)).padEnd(9)+mvD[d2].toLocaleString()),n:eD.length,extra:''},
      {key:'otel_traces_trace_id_ts',nm:'otel_traces_trace_id_ts',hdr:'TraceId      Start–End      n',rows:eT.slice(-3).map(k=>(k+'…').padEnd(13)+(fmtT(mvT[k].s)+'–'+fmtT(mvT[k].e)).padEnd(14)+mvT[k].n),n:eT.length,extra:eT.length>3?'+'+(eT.length-3)+' 行':''},
    ];
    const geom=b=>{ const body=b.rows.length?b.rows.join('\n')+(b.extra?'\n'+b.extra:''):'(空 — INSERT 待ち)';
      return {body,h:26+(1+body.split('\n').length)*16+8}; };
    const q0=geom(bodies[0]), q1=geom(bodies[1]), q2=geom(bodies[2]);
    const pos=[
      {x:lx,y:topY,w:colW,body:q0.body,h:q0.h},
      {x:lx,y:topY+q0.h+64,w:colW,body:q1.body,h:q1.h},
      {x:rx2,y:topY,w:colW,body:q2.body,h:q2.h},
    ];
    G.segs=[
      {mv:'otel_traces_1h_mv',x:lcx,y1:ltbl.y+hh,y2:topY,dcy:topY+30,side:'L'},
      {mv:'otel_traces_1d_mv',x:lcx,y1:topY+q0.h,y2:topY+q0.h+64,dcy:topY+q0.h+64+30,side:'L'},
      {mv:'otel_traces_trace_id_ts_mv',x:rcx,y1:ltbl.y+hh,y2:topY,dcy:topY+30,side:'R'},
    ];
    edges.clear();
    bodies.forEach((d,i)=>{
      const o=tgt[i], q=pos[i];
      panel(o.bg,q.w,q.h,0xffffff,tgtFlash[i]>0?0x9775fa:0xd9dbe0,tgtFlash[i]>0?2:1);
      o.bg.rect(1,1,q.w-2,22).fill(0xfff9db);
      o.tx.text=d.hdr+'\n'+q.body;
      o.tx.style.fill=d.n?0x2a2e39:0x9aa0a8;
      o.tt.text='TABLE '+d.nm;
      o.c.__w=q.w; if(o.tmg.text) o.tmg.x=q.w-22;
      o.tcn.text=d.n+' 行'; o.tcn.x=q.w-(i===0?34:10)-o.tcn.width;
      o.c.x=q.x; o.c.y=q.y;
      if(tgtFlash[i]>0) tgtFlash[i]=Math.max(0,tgtFlash[i]-0.02);
    });
    // パイプ: 縦の実線+下向き矢印、ラベルは線の脇
    G.segs.forEach((sg,i)=>{
      const pulse=pipePulse[sg.mv]||0;
      const col=pulse>0?0x7048c8:0xb9a6dd;
      edges.moveTo(sg.x,sg.y1).lineTo(sg.x,sg.y2-7).stroke({width:pulse>0?2.5:1.5,color:col});
      edges.poly([sg.x,sg.y2,sg.x-4.5,sg.y2-8,sg.x+4.5,sg.y2-8]).fill(col);
      const ec=chip('s0e'+i,'mv',()=>tableInsp(sg.mv));
      ec.textContent='MV '+sg.mv+' ▸';
      const base=sg.side==='L'?'translate(-100%,-50%)':'translate(0,-50%)';
      ec.style.transform=pulse>0?base+' scale(1.12)':base;
      placeChip(ec,sg.side==='L'?sg.x-10:sg.x+10,(sg.y1+sg.y2)/2);
      if(pulse>0) pipePulse[sg.mv]=Math.max(0,pulse-0.01);
    });
    // Collector → otel_raw(送信の線)
    edges.moveTo(G.trX,colY+colH).lineTo(G.trX,rawY-7).stroke({width:1.5,color:0x9db08f});
    edges.poly([G.trX,rawY,G.trX-4.5,rawY-8,G.trX+4.5,rawY-8]).fill(0x9db08f);
    // 取り込みパイプ(縦): otel_raw → transform_mv → otel_traces
    {
      const px2=G.trX, y1=G.rawBtm, y2v=ltbl.y;
      const col=rawPulse>0?0x2b8a3e:0xb9c2ae;
      edges.moveTo(px2,y1).lineTo(px2,y2v-7).stroke({width:rawPulse>0?2.5:1.5,color:col});
      edges.poly([px2,y2v,px2-4.5,y2v-8,px2+4.5,y2v-8]).fill(col);
      const tec=chip('s0tr','mv',()=>tableInsp('transform_mv'));
      tec.textContent='MV transform_mv ▸ 列に解く';
      tec.style.transform=rawPulse>0?'translate(-100%,-50%) scale(1.08)':'translate(-100%,-50%)';
      placeChip(tec,px2-10,(y1+y2v)/2);
      if(rawPulse>0) rawPulse=Math.max(0,rawPulse-0.01);
    }
    const z1=chip('s0z1h','warn',()=>{ S1T='otel_traces_1h'; zoomTo('S1'); });
    z1.textContent='⊕ 中を見る(Part / 状態)';
    placeChip(z1,pos[0].x+pos[0].w/2,pos[0].y+pos[0].h+16);
    CONTENT_H=pos[1].y+pos[1].h+120;
    const zc=chip('s0zoom','warn',()=>{ S1T='otel_traces'; zoomTo('S1'); });
    zc.textContent='⊕ 中を見る(Part / granule)';
    placeChip(zc,36+LTW/2,ltbl.y+hh+14);
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
  const secL=textV('STORAGE — otel_traces の中(Partition ⊃ Part ⊃ granule)',11,0x9a9a90,false);
  s.cont.addChild(secL);
  const stubs=[0,1,2].map(()=>{ const g=new PIXI.Graphics(); s.cont.addChild(g); return g; });
  let stubPulse=[0,0,0], insBatch=null, insPhase='';
  const colB=new PIXI.Container();
  const colBg2=new PIXI.Graphics();
  const colT2=textV('OTel Collector',11,0xe8e6dc); colT2.x=12; colT2.y=7;
  const colH2=textV('OTLP/HTTP ▸ クリックでバッチ送信',10,0x9a9a90); colH2.y=8;
  colB.addChild(colBg2,colT2,colH2); s.cont.addChild(colB);
  colB.eventMode='static'; colB.cursor='pointer';
  colB.on('pointertap',()=>{ if(busy){ toast('実行中です','warn'); return; } doInsert(); });
  const hViews=new Map();
  const mkZone=()=>{ const cont=new PIXI.Container(),bg=new PIXI.Graphics(),tx=textV('',11.5,0x2a2e39); tx.x=12; tx.y=24; cont.addChild(bg,tx); s.cont.addChild(cont); return {cont,bg,tx}; };
  const tZone=mkZone(), dZone=mkZone();
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
    else if(e.t==='mv.fire'){ const i={otel_traces_1h_mv:0,otel_traces_trace_id_ts_mv:1}[e.mv]; if(i!=null) stubPulse[i]=1; }
    else if(e.t==='part.merged'){ const p=parts.find(x=>x.id===e.into); if(p) p.flash=1; }
    else if(e.t==='delete.mask'||e.t==='mutation.rewrite'){ actParts().forEach(p=>p.flash=Math.max(p.flash||0,0.5)); }
  };
  s.tick=()=>{
    if(false&&S1T==='otel_traces_1h'){
      colB.visible=false;
      frameG.clear(); strip.clear(); stubs.forEach(g=>g.clear());
      stripTiles.removeChildren().forEach(c=>c.destroy());
      secL.text='STORAGE — otel_traces_1h の中(AggregatingMergeTree)'; secL.x=24; secL.y=INS_Y+2;
      let y=INS_Y+46; const seen=new Set();
      mvHParts.forEach(q=>{
        let v=hViews.get(q.id);
        if(!v){ v={cont:new PIXI.Container(),bg:new PIXI.Graphics(),tx:textV('',11.5,0x2a2e39)};
          v.tx.x=12; v.tx.y=26; v.cont.addChild(v.bg,v.tx); s.cont.addChild(v.cont); hViews.set(v.id=q.id,v); }
        seen.add(q.id);
        const keys=Object.keys(q.rows).sort();
        const rows=keys.map(k=>{ const r=q.rows[k], hs=+k.split('|')[0], sv=k.split('|')[1];
          return fmtT(hs).padEnd(7)+sv.padEnd(11)+('c='+r.c.toLocaleString()).padEnd(10)+'avgState{sum:'+Math.round(r.d/1000).toLocaleString()+'s, n:'+r.c.toLocaleString()+'}'; });
        const h2=26+Math.max(1,rows.length)*16+12;
        panel(v.bg,560,h2,0xffffff,q.flash>0?0x9775fa:0xd9dbe0,q.flash>0?2:1,8);
        v.bg.rect(1,1,558,22).fill(0xf1ecfa);
        v.tx.text=rows.join('\n')||'(空)';
        v.cont.x=24; v.cont.y=y;
        const c=chip('h'+q.id,'mv',()=>tableInsp('otel_traces_1h'));
        c.textContent='part '+q.id+' ・ 状態 '+keys.length+' 行';
        placeChip(c,24+120,y+2);
        if(q.flash>0) q.flash=Math.max(0,q.flash-0.02);
        y+=h2+22;
      });
      hViews.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); hViews.delete(id); } });
      const mg2=chip('h-merge','warn',()=>doMerge('1h'));
      mg2.textContent='⇄ マージ';
      placeChip(mg2,24+470,y+8);
      const bk=chip('h-back','warn',()=>{ S1T='otel_traces'; zoomTo('S0'); });
      bk.textContent='⊖ テーブル層へ';
      placeChip(bk,24+280,y+8);
      const nt=chip('h-note','',null);
      nt.textContent=mvHParts.length>1?'同じ (hour, Service) キーが複数 Part に居る → ⇄ マージで状態が結合される':'マージ済み: キーごとに1行。avg の確定は読む側の avgMerge';
      nt.style.opacity='0.8';
      placeChip(nt,24+280,y+34);
      CONTENT_H=y+130;
      return;
    }
    hViews.forEach((v,id)=>{ v.cont.destroy({children:true}); hViews.delete(id); });
    secL.text='';
    const cw3=partW()+22;
    panel(colBg2,cw3,30,0x26261f,0x4a4a40,1,8);
    colH2.x=cw3-12-colH2.width;
    colB.visible=true; colB.x=14; colB.y=INS_Y;
    // INSERT 帯
    strip.clear();
    if(insBatch){
      strip.roundRect(16,INS_Y+40,STW()-32,36,6).fill(0xf2f1ec).stroke({width:1,color:0xdad9d0});
      const sc=chip('s1strip','',null);
      sc.textContent=insPhase==='arrive'?'OTLP バッチ(届いた順)':insPhase==='sorted'?'ORDER BY (ServiceName, Timestamp) でソート → granule 区切り':'新しい Part へ';
      placeChip(sc,STW()/2,INS_Y+38);
      stripTiles.removeChildren().forEach(c=>c.destroy());
      const list=insPhase==='arrive'?insBatch:[...insBatch];
      list.forEach((v,i)=>{
        const t=textV(fmtT(v),10,0x2a2e39);
        const g=new PIXI.Graphics(); g.roundRect(0,0,CELL,20,4).fill(insPhase==='arrive'?0xfff3bf:0xd3f9d8).stroke({width:1,color:0xdad9d0});
        const c=new PIXI.Container(); c.addChild(g,t); t.x=CELL/2-t.width/2; t.y=3;
        c.x=30+i*(CELL+4); c.y=INS_Y+48; stripTiles.addChild(c);
      });
    } else { const sc=chips.get('s1strip'); if(sc){sc.remove(); chips.delete('s1strip');} stripTiles.removeChildren().forEach(c=>c.destroy()); }
    // Parts
    let y=INS_Y+100; const seen=new Set();
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
    frameG.roundRect(14,INS_Y+84,partW()+22,Math.max(120,y-INS_Y-84-4),10).stroke({width:1.5,color:0xcfd2c8});
    const mg3=chip('s1merge','warn',()=>doMerge());
    mg3.textContent='⇄ マージ';
    placeChip(mg3,24+partW()-50,INS_Y+80);
    const fc=chip('s1tbl','',()=>zoomTo('S0'));
    fc.textContent='TABLE otel_traces(⊖ テーブル層へ)';
    placeChip(fc,24+partW()/2,INS_Y+80);
    // 派生テーブルの物理層も上→下(S0 と同じ2列・granuleタイルで)
    const colW2=470, lxx=24, rxx=24+colW2+44;
    const fR=14+partW()+22, fbF=INS_Y+84+Math.max(120,y-INS_Y-84-4);
    const cy0=fbF+70;
    [{mv:'otel_traces_1h_mv',x:lxx+colW2/2,side:'L'},{mv:'otel_traces_trace_id_ts_mv',x:rxx+colW2/2,side:'R'}].forEach((sg,i)=>{
      const g=stubs[i], pu=stubPulse[i]>0, col=pu?0x7048c8:0xb9a6dd;
      const ex=Math.min(sg.x,fR-50);
      g.clear();
      g.moveTo(ex,fbF).lineTo(ex,fbF+18);
      if(ex!==sg.x) g.lineTo(sg.x,fbF+18);
      g.lineTo(sg.x,cy0-9);
      g.stroke({width:pu?2.5:1.5,color:col});
      g.poly([sg.x,cy0-2,sg.x-4.5,cy0-10,sg.x+4.5,cy0-10]).fill(col);
      const c=chip('s1mv'+i,'mv',()=>tableInsp(sg.mv));
      c.textContent='MV '+sg.mv+' ▸';
      const base=sg.side==='L'?'translate(-100%,-50%)':'translate(0,-50%)';
      c.style.transform=pu?base+' scale(1.1)':base;
      placeChip(c,sg.side==='L'?sg.x-10:sg.x+10,fbF+34);
      if(stubPulse[i]>0) stubPulse[i]=Math.max(0,stubPulse[i]-0.015);
    });
    // タイル描画ヘルパ(状態行=タイル、右に primary.idx 先頭キー)
    const tileCard=(v,w,items,firstKey,band)=>{
      if(!v.tiles){ v.tiles=new PIXI.Container(); v.cont.addChild(v.tiles); v.cells=new Map();
        v.idx=textV('',10,0x8a7300); v.idx.x=12+GPR*(CELL+GAP)+16; v.idx.y=26; v.cont.addChild(v.idx); }
      items.forEach((it,ci)=>{ let cell=v.cells.get(ci);
        if(!cell){ cell={g:new PIXI.Graphics(),t:textV('',10.5,0x2a2e39)}; v.tiles.addChild(cell.g); v.tiles.addChild(cell.t); v.cells.set(ci,cell); }
        const x=12+(ci%GPR)*(CELL+GAP), yy=26+Math.floor(ci/GPR)*(CELLH+GAP);
        cell.g.clear(); cell.g.roundRect(x,yy,CELL,CELLH,4).fill(it.bg).stroke({width:1,color:0xdde0e4});
        cell.t.text=it.txt; cell.t.x=x+CELL/2-cell.t.width/2; cell.t.y=yy+5; cell.g.visible=cell.t.visible=true;
      });
      for(let ci=items.length;v.cells.has(ci);ci++){ const cell=v.cells.get(ci); cell.g.visible=cell.t.visible=false; }
      const rowsN=Math.max(1,Math.ceil(items.length/GPR));
      const h2=26+rowsN*(CELLH+GAP)+12;
      panel(v.bg,w,h2,0xffffff,(v.fl||0)>0?0x9775fa:0xd9dbe0,(v.fl||0)>0?2:1,8);
      v.bg.rect(1,1,w-2,18).fill(band);
      v.idx.text='primary.idx\n'+firstKey;
      v.tx.text=items.length?'':'(空 — INSERT 待ち)';
      return h2;
    };
    // 1h: 状態 Part 群(左列)
    let hy=cy0;
    const hc=chip('s1h-h','mv',()=>tableInsp('otel_traces_1h'));
    hc.textContent='TABLE otel_traces_1h ・ part ×'+mvHParts.length;
    placeChip(hc,lxx+colW2/2,hy-12);
    const hmg=chip('s1h-mg','warn',()=>doMerge('1h'));
    hmg.textContent='⇄';
    placeChip(hmg,lxx+colW2-14,hy-12);
    const seenH=new Set();
    const hparts=mvHParts.length?mvHParts:[{id:-1,rows:{},flash:0}];
    hparts.forEach(q=>{
      let v=hViews.get(q.id);
      if(!v){ v={cont:new PIXI.Container(),bg:new PIXI.Graphics(),tx:textV('',11,0x9aa0a8)}; v.tx.x=12; v.tx.y=28; v.cont.addChild(v.bg,v.tx); s.cont.addChild(v.cont); hViews.set(q.id,v); }
      seenH.add(q.id);
      const keys=Object.keys(q.rows).sort();
      v.fl=q.flash;
      const items=keys.map(k=>{ const hs=+k.split('|')[0], sv=k.split('|')[1];
        return {txt:fmtT(hs),bg:SVCTINT[sv]||0xeef0f2}; });
      const fk=keys[0]?fmtT(+keys[0].split('|')[0])+' '+keys[0].split('|')[1].slice(0,2):'—';
      const h2=tileCard(v,colW2,items,fk,0xf1ecfa);
      v.cont.x=lxx; v.cont.y=hy;
      if(q.id>0){ const pc=chip('s1hp'+q.id,'',()=>tableInsp('otel_traces_1h'));
        pc.textContent='part '+q.id+' ・ granule ×'+Math.max(1,Math.ceil(keys.length/GPR))+' ・ 状態 '+keys.length+' 行';
        placeChip(pc,lxx+130,hy+1); }
      if(q.flash>0) q.flash=Math.max(0,q.flash-0.02);
      hy+=h2+18;
    });
    hViews.forEach((v,id)=>{ if(!seenH.has(id)){ v.cont.destroy({children:true}); hViews.delete(id); } });
    // trace_id_ts(右列)
    const eT2=Object.keys(mvT);
    const tItems=eT2.map(k=>({txt:k.slice(0,4),bg:0xf3ecfa}));
    const tH=tileCard(tZone,colW2,tItems,eT2[0]?eT2[0].slice(0,8)+'…':'—',0xf1ecfa);
    tZone.cont.x=rxx; tZone.cont.y=cy0;
    const tc2=chip('s1t-h','mv',()=>tableInsp('otel_traces_trace_id_ts'));
    tc2.textContent='TABLE otel_traces_trace_id_ts ・ '+eT2.length+' 行';
    placeChip(tc2,rxx+colW2/2,cy0-12);
    // 1d(左列の下、1h からのカスケード)
    stubs[2].clear();
    stubs[2].moveTo(lxx+colW2/2,hy-6).lineTo(lxx+colW2/2,hy+26).stroke({width:1.5,color:0xb9a6dd});
    stubs[2].poly([lxx+colW2/2,hy+33,lxx+colW2/2-4.5,hy+25,lxx+colW2/2+4.5,hy+25]).fill(0xb9a6dd);
    const dmc=chip('s1d-mv','mv',()=>tableInsp('otel_traces_1d_mv'));
    dmc.textContent='MV otel_traces_1d_mv ▸';
    dmc.style.transform='translate(-100%,-50%)';
    placeChip(dmc,lxx+colW2/2-10,hy+12);
    const eD2=Object.keys(mvD).sort();
    const dItems=eD2.map(d2=>({txt:d2.slice(4,6)+'/'+d2.slice(6),bg:0xfff3bf}));
    const dH=tileCard(dZone,colW2,dItems,eD2[0]?eD2[0].slice(4,6)+'-'+eD2[0].slice(6):'—',0xfff3bf);
    dZone.cont.x=lxx; dZone.cont.y=hy+42;
    const dc2=chip('s1d-h','mv',()=>tableInsp('otel_traces_1d'));
    dc2.textContent='TABLE otel_traces_1d ・ '+eD2.length+' 行';
    placeChip(dc2,lxx+colW2/2,hy+30);
    CONTENT_H=Math.max(hy+42+dH,cy0+tH)+130;
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
  const introG=new PIXI.Graphics(); const introT=textV('otel_traces を Part 群に開く…',12,0x6b6b60,false);
  s.cont.addChild(introG,introT);
  let marks=new Map(), lanes=[], intro=0, resRows=null, lk=null, curHit=null;
  const lkG=new PIXI.Graphics(); s.cont.addChild(lkG);
  const lkTx=textV('',11.5,0x5d4a86); s.cont.addChild(lkTx);
  function laneGeom(i){
    const px=24+partW()+36;
    const sw=STW();
    const w=Math.max(250,Math.min(520,sw-px-40-250));
    return {x:px,y:INS_Y+60+LKUP*64+i*126,w,h:114};
  }
  function resGeom(){ const g=laneGeom(0); return {x:g.x+g.w+16,y:g.y+8,w:Math.max(210,Math.min(280,STW()-(g.x+g.w)-30))}; }
  s.enter=()=>{ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); intro=1; resRows=null; lk=null; curHit=null; LKUP=0; };
  s.exit=()=>{ setStage(0); LKUP=0; };
  s.onEvent=e=>{
    if(e.t==='query.start'){ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); resRows=null; lk=null; curHit=null; LKUP=0; setStage(1); }
    else if(e.t==='trace.lookup'){ lk=e; LKUP=1; const w=Math.floor(e.s/TRWIN); curHit=(p,gi,ci,v)=>p.day===TODAY&&Math.floor(v/TRWIN)===w&&!p.del[gi*GPR+ci]; toast('⓪ まず otel_traces_trace_id_ts を TraceId で引く(ORDER BY の先頭なので一発)→ Start–End の時間範囲を得る'); }
    else if(e.t==='prune.partition'){ e.cut.forEach(id=>{ marks.set(id,Object.assign(getM(id),{pruned:true})); }); toast('① Partition 枝刈り: 日付条件に合わない Partition は索引すら見ない'); }
    else if(e.t==='prune.primary'){ e.plan.forEach(pl=>{ getM(pl.pid).pkKeep=new Set(pl.keep); }); toast(e.note||'① 主キーの境界だけで読む granule を確定'); }
    else if(e.t==='prune.skip'){
      e.skipped.forEach(sk=>{ const m=getM(sk.pid); m.skip=m.skip||new Set(); m.skip.add(sk.gi); });
      setStage(2);
      toast(e.note||'② skip idx で読む前に落とす');
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
      introT.text='otel_traces(論理)を Part 群(物理)に開く…';
      introT.alpha=intro; introT.x=44; introT.y=INS_Y+80;
    } else { introG.clear(); introT.alpha=0; }
    // Parts(左列)
    let y=INS_Y+60; const seen=new Set();
    actParts().forEach(p=>{
      let v=views.get(p.id);
      if(!v){ v=buildPartView(); views.set(p.id,v); s.cont.addChild(v.cont); }
      seen.add(p.id);
      v.cont.x=24; v.cont.y=y; v.cont.alpha=(1-intro);
      updatePartView(v,p,marks.get(p.id),curHit);
      const c=chip('s2p'+p.id,(marks.get(p.id)||{}).pruned?'warn':'',null);
      c.textContent=p.name+(marks.get(p.id)&&marks.get(p.id).pruned?' ✂':'');
      placeChip(c,24+partW()/2,y-2);
      y+=partH(p)+16;
    });
    views.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); views.delete(id); } });
    // otel_traces_trace_id_ts ルックアップカード
    if(lk){
      const g0=laneGeom(0);
      lkG.clear();
      lkG.roundRect(g0.x,INS_Y+56,g0.w,52,8).fill(0xf9f6ff).stroke({width:2,color:0x9775fa});
      lkG.rect(g0.x+1,INS_Y+57,g0.w-2,16).fill(0xeee6fb);
      lkTx.text=lk.tid+'…   Start '+fmtT(lk.s)+' – End '+fmtT(lk.e)+' ・ '+lk.n+' span';
      lkTx.x=g0.x+12; lkTx.y=INS_Y+80; lkTx.visible=true;
      const c=chip('s2lk','mv',null);
      c.textContent='① TABLE otel_traces_trace_id_ts ▸ TraceId で範囲を特定';
      placeChip(c,g0.x+g0.w/2,INS_Y+58);
    } else { lkG.clear(); lkTx.visible=false; }
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
    CONTENT_H=Math.max(y+60,laneGeom(2).y+180);
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
  rescEl.style.display='none'; rescEl.__st=null; SCROLL=0; // S2の持ち物は退場時に隠す
  curName=name; SCENE=name; cur=scenes[name];
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
  else if(curName==='S1') html+=seg('tables',false,'S0')+'<span class="sep">›</span>'+seg('otel_traces',true);
  else html+=seg('tables',false,'S0')+'<span class="sep">›</span>'+seg('クエリ実行',true)+' <span class="cr" data-go="'+zoomBefore+'">⊖ ステージへ戻る</span>';
  crumbEl.innerHTML=html;
  crumbEl.querySelectorAll('.cr').forEach(el=>{
    const go=el.dataset.go;
    if(go) el.onclick=()=>zoomTo(go);
  });
}

/* ---------- 7. レール配線と初期化 ---------- */

let STMT='sel';
function wbSQL(){
  if(STMT==='sel') return SQLQ()+';';
  if(STMT==='tid'){ const ks=Object.keys(mvT); const tid=ks.length?ks[ks.length-1]+'…':'<TraceId>';
    return "WITH '"+tid+"' AS tid, (SELECT min(Start) FROM otel_traces_trace_id_ts WHERE TraceId = tid) AS s, (SELECT max(End)+1 FROM otel_traces_trace_id_ts WHERE TraceId = tid) AS e SELECT Timestamp, ServiceName, SpanName FROM otel_traces WHERE Timestamp >= s AND Timestamp < e;"; }
  if(STMT==='del') return "DELETE FROM otel_traces WHERE ServiceName = '"+(SVCF||'checkout')+"';";
  if(STMT==='upd') return "ALTER TABLE otel_traces UPDATE SpanAttributes['tier'] = 'vip' WHERE ServiceName = '"+(SVCF||'frontend')+"';";
  return 'ALTER TABLE otel_traces ADD PROJECTION by_service (SELECT * ORDER BY ServiceName);';
}
const wbEl=document.getElementById('wbsql');
function updWb(){ wbEl.textContent=wbSQL(); }
wbEl.onclick=()=>openInsp('<h2>🛠 Workbench</h2><div class="sub">実行される文(パラメータ連動)</div><pre>'+wbSQL().replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</pre>');
document.getElementById('stmtSel').onchange=e=>{ STMT=e.target.value; updWb(); };
document.getElementById('bRun').onclick=()=>{ ({sel:doSelect,tid:doTraceSelect,del:doDelete,upd:doUpdate,proj:doProj})[STMT](); };
document.getElementById('bSql').onclick=()=>{
  openInsp('<h2>❯ 現在の文</h2><div class="sub">最後に実行された(される)DML/クエリ</div><pre>'
    +CURSQL.replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</pre>'
    +'<div class="note">SELECT はツールバーの WHERE と連動する。OTLP バッチの INSERT は人が書くものではなく、collector の exporter が発行する。</div>');
};

document.getElementById('engSel').onchange=e=>{ ENG=e.target.value; toast(ENG==='rmt'?'ReplacingMergeTree: マージ時に同じ Timestamp の行を置換(重複排除)':'MergeTree: 追記のみ'); };
document.getElementById('idxSel').onchange=e=>{ IDXT=e.target.value; toast(IDXT==='minmax'?'minmax(Timestamp): 主キー先頭が ServiceName の並びで、時刻条件を救う skip idx':'skip idx なし: 主キーだけで戦う(時刻だけの検索が重くなる)'); };
const predR=document.getElementById('predR');
if(predR){ predR.oninput=e=>{ PRED=+e.target.value; const lb=document.getElementById('predV'); if(lb) lb.textContent=fmtT(PRED); updWb(); }; }
const svcSel=document.getElementById('svcSel');
if(svcSel){ svcSel.onchange=e=>{ SVCF=e.target.value; updWb(); }; }
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
  parts=[]; seq=0; mvH={}; mvD={}; mvT={}; trSeq=0; mvHParts=[]; hpSeq=0; S1T='otel_traces'; projOn=false; mutSeq=6;
  seedParts(); showDefault(); updWb(); switchTo('S0');
  toast('初期状態に戻した');
};
function showDefault(){ showSql(SQLQ()+';'); showMsg('待機中'); resShown=false; }


seedParts(); showDefault();
switchTo('S0');
const sbEl=document.getElementById('searchbar');
const stepsEl=document.getElementById('steps');
function measureBars(){
  let y=12+sbEl.offsetHeight+8;
  if(stepsEl.classList.contains('show')){ stepsEl.style.top=y+'px'; y+=stepsEl.offsetHeight+8; }
  crumbEl.style.top=(y+2)+'px';
  YBASE=y+40;
  const maxS=Math.max(0, CONTENT_H*Z-(innerHeight-YBASE)+50);
  if(SCROLL>maxS) SCROLL=maxS;
  world.y=YBASE-SCROLL;
  const maxX=Math.max(0,1040*Z-(W-24));
  if(SCROLLX>maxX) SCROLLX=maxX;
  world.x=12-SCROLLX;
}
addEventListener('wheel',e=>{ SCROLL=Math.max(0,SCROLL+e.deltaY); SCROLLX=Math.max(0,SCROLLX+e.deltaX); measureBars(); },{passive:true});
let tchs=null;
addEventListener('touchstart',e=>{
  if(e.touches.length===1) tchs={x:e.touches[0].clientX,y:e.touches[0].clientY};
  else if(e.touches.length===2){ const dx=e.touches[0].clientX-e.touches[1].clientX,dy=e.touches[0].clientY-e.touches[1].clientY; tchs={pd:Math.hypot(dx,dy)}; }
},{passive:true});
addEventListener('touchend',()=>{ tchs=null; },{passive:true});
addEventListener('touchmove',e=>{
  if(!tchs) return;
  if(e.touches.length===1&&tchs.x!=null){
    const t0=e.touches[0];
    SCROLL=Math.max(0,SCROLL-(t0.clientY-tchs.y));
    SCROLLX=Math.max(0,SCROLLX-(t0.clientX-tchs.x));
    tchs.x=t0.clientX; tchs.y=t0.clientY; measureBars();
  } else if(e.touches.length===2&&tchs.pd){
    const dx=e.touches[0].clientX-e.touches[1].clientX,dy=e.touches[0].clientY-e.touches[1].clientY;
    const d=Math.hypot(dx,dy);
    Z=Math.max(0.45,Math.min(2.2,Z*d/tchs.pd));
    tchs.pd=d; world.scale.set(Z); measureBars();
  }
},{passive:true});
addEventListener('resize',measureBars);
app.ticker.add(()=>{
  frame++;
  if(frame%10===0||frame<5) measureBars();
  paper.clear();
  paper.roundRect(0,0,STW(),Math.max((innerHeight-YBASE)/Z,CONTENT_H+30),12).fill(0xf8f8f6).stroke({width:1,color:0xe2e2dd});
  if(cur) cur.tick();
  tickFly();
  chips.forEach((c,k)=>{ if(c.__seen!==frame){ c.remove(); chips.delete(k); } });
});
window.__v6={ get parts(){return parts;}, get scene(){return curName;}, EVLOG, zoomTo };
})().catch(e=>{ document.title='PXERR: '+(e&&e.message||e); console.error(e); });

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
let S1T='otel_traces', SCENE='S0';
let REPL=false, n2Parts=[];   // node-2(レプリカ)が持つ Part id
function doAddReplica(){
  if(REPL) return toast('レプリカは追加済み','warn');
  if(busy) return toast('実行中です','warn');
  REPL=true; busy=true;
  showSql("-- Keeper のパスとレプリカ名を持つエンジンに置き換える\nCREATE TABLE otel_traces ( … )\nENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')\nORDER BY (ServiceName, SpanName, toDateTime(Timestamp));");
  showMsg('node-2 を追加 → Keeper 経由で既存 Part を複製');
  emit('repl.setup',{});
  const ids=actParts().map(q=>q.id);
  ids.forEach((id,i)=>setTimeout(()=>{ emit('repl.fetch',{pid:id});
    setTimeout(()=>{ n2Parts.push(id); emit('repl.synced',{pid:id});
      if(i===ids.length-1){ busy=false; toast('ReplicatedMergeTree: データは各レプリカが1コピーずつ持ち、Keeper が「誰が何を持つか」の唯一の台帳になる。ノード同士は直接会話しない'); } },700);
  },600+i*900));
} // S1 の対象テーブル / 現在シーン
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
    [1500,()=>emit('insert.sorted',{vals:sorted})],
    [1600,()=>{ seq++; const p={id:seq,name:TODAY+'_'+seq+'_'+seq+'_0',day:TODAY,granules:mkGranules(sorted),lvl:0,del:{},upd:{}}; parts.push(p); emit('part.born',{pid:p.id}); }],
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
    [700,()=>{ if(REPL){ const np=actParts()[actParts().length-1];
      emit('repl.log',{pid:np.id});
      setTimeout(()=>emit('repl.fetch',{pid:np.id}),700);
      setTimeout(()=>{ n2Parts.push(np.id); emit('repl.synced',{pid:np.id}); },1500); } }],
    [900,()=>{ emit('table.rows',{total:tableRows()}); showMsg('Ok.('+(sorted.length*2048).toLocaleString()+' 行 → 新しい Part)'); busy=false; }],
  ]);
}
function doMerge(tgt2){
  if(busy) return toast('実行中です','warn');
  if(tgt2==='1h'||(SCENE==='S1'&&S1T==='otel_traces_1h')){
    if(mvHParts.length<2) return toast('Part が1つ以下。Collector からバッチを流すと増える','warn');
    busy=true;
    emit('merge.start',{pids:mvHParts.map(q=>q.id),day:TODAY});
    { const gp={};
      mvHParts.forEach(q=>Object.keys(q.rows).forEach(k=>{ (gp[k]=gp[k]||[]).push({c:q.rows[k].c,d:q.rows[k].d}); }));
      const gl=Object.keys(gp).sort().map(k=>({key:k,srcs:gp[k],
        res:gp[k].reduce((a,r)=>({c:a.c+r.c,d:a.d+r.d}),{c:0,d:0})}));
      const dup=gl.filter(g2=>g2.srcs.length>1);
      emit('agg.align',{groups:gl.slice().sort((a,b)=>b.srcs.length-a.srcs.length).slice(0,3),keys:gl.length,dup:dup.length}); }
    setTimeout(()=>{
      const fold={};
      mvHParts.forEach(q=>Object.keys(q.rows).forEach(k=>{ const r=fold[k]||(fold[k]={c:0,d:0}); r.c+=q.rows[k].c; r.d+=q.rows[k].d; }));
      mvHParts=[{id:++hpSeq,rows:fold,flash:1}];
      emit('part.merged',{into:hpSeq,from:[]});
      toast('AggregatingMergeTree のマージ: 同じ ORDER BY キー (hour, Service) の行は「状態」(sum と count)を結合して1行になる。avg はここでは計算されない — 読む側が avgMerge で確定する');
      busy=false;
    },2600);
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
/* ---------- REAL: 公開プレイグラウンドの実テーブルを読む ---------- */
let hUpdWb=()=>{}, hOpenInsp=()=>{}, hZoomS1=()=>{};  // IIFE内の関数への橋
const RQ={
  ep:'https://sql-clickhouse.clickhouse.com/?user=demo&default_format=JSONCompact',
  db:'otel_clickpy', tbl:'otel_traces',
  on:false, status:'', parts:[], seen:new Set(), svcs:[], expl:null, ddl:'', gen:[]
};
function chq(sql){
  return fetch(RQ.ep,{method:'POST',body:sql}).then(r=>r.text()).then(txt=>{
    let j; try{ j=JSON.parse(txt); }catch(e){ throw new Error(txt.slice(0,140)); }
    return j.data||[];
  });
}
const rstEl=()=>document.getElementById('realst');
function rst(msg,err){ const e=rstEl(); if(!e) return; e.textContent=msg; e.className=err?'err':''; RQ.status=msg; }
function realParts(){
  return chq("SELECT name, level, rows, marks, bytes_on_disk, partition FROM system.parts WHERE database='"+RQ.db+"' AND table='"+RQ.tbl+"' AND active ORDER BY name")
    .then(rows=>{
      const now=new Set(rows.map(r=>r[0])), had=RQ.seen.size>0;
      const born=rows.map(r=>r[0]).filter(n=>had&&!RQ.seen.has(n));
      const gone=[...RQ.seen].filter(n=>!now.has(n));
      RQ.seen=now;
      RQ.parts=rows.map(r=>({name:r[0],level:+r[1],rows:+r[2],marks:+r[3],bytes:+r[4],part:r[5],fresh:born.indexOf(r[0])>=0?1:0}));
      const tr=RQ.parts.reduce((a,b)=>a+b.rows,0), tg=RQ.parts.reduce((a,b)=>a+b.marks,0);
      rst('REAL '+RQ.db+'.'+RQ.tbl+' ・ Part '+rows.length+' ・ granule '+tg.toLocaleString()+' ・ '+tr.toLocaleString()+' 行');
      if(born.length) toast('実データ: Part が '+born.length+' 個生まれた('+born.slice(0,2).join(', ')+')');
      if(gone.length) toast('実データ: Part が '+gone.length+' 個消えた — マージで畳まれた');
    });
}
function realExplain(){
  const hrs=Math.max(1,Math.round((961-PRED)/60));
  const wh="Timestamp >= now() - INTERVAL "+hrs+" HOUR"+(SVCF?" AND ServiceName = '"+SVCF+"'":'');
  const sql="EXPLAIN indexes=1 SELECT toStartOfHour(Timestamp) AS h, count() FROM "+RQ.db+"."+RQ.tbl+" WHERE "+wh+" GROUP BY h ORDER BY h";
  RQ.sql="SELECT toStartOfHour(Timestamp) AS h, count() FROM "+RQ.db+"."+RQ.tbl+"\nWHERE "+wh+"\nGROUP BY h ORDER BY h";
  return chq(sql).then(rows=>{
    const L=rows.map(r=>String(r[0]));
    const steps=[]; let cur=null;
    L.forEach(s=>{
      const nm=s.match(/^\s{10,}(Min-Max|Partition|PrimaryKey|Skip)\s*$/);
      if(nm){ cur={name:nm[1]}; steps.push(cur); return; }
      if(!cur) return;
      let m=s.match(/Parts:\s*(\d+)\/(\d+)/); if(m){ cur.pk=+m[1]; cur.pn=+m[2]; }
      m=s.match(/Granules:\s*(\d+)\/(\d+)/); if(m){ cur.gk=+m[1]; cur.gn=+m[2]; }
      m=s.match(/Search Algorithm:\s*(.+)$/); if(m) cur.alg=m[1].trim();
      m=s.match(/^\s{12,}(Name|Description):\s*(.+)$/); if(m&&!cur.desc) cur.desc=m[2].trim();
    });
    RQ.expl=steps.filter(s=>s.gn);
    return RQ.expl;
  });
}
function realBoot(){
  rst('接続中…');
  return Promise.all([
    realParts(),
    chq("SELECT ServiceName, count() c FROM "+RQ.db+"."+RQ.tbl+" GROUP BY 1 ORDER BY c DESC LIMIT 8").then(r=>{ RQ.svcs=r.map(x=>x[0]); }),
    chq("SHOW CREATE TABLE "+RQ.db+"."+RQ.tbl).then(r=>{ RQ.ddl=r.length?String(r[0][0]):''; }),
    chq("SELECT level, count(), sum(rows), formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database='stockhouse' AND table='crypto_trades' AND active GROUP BY level ORDER BY level").then(r=>{ RQ.gen=r; }).catch(()=>{}),
    realExplain()
  ]).then(()=>{
    const sel=document.getElementById('svcSel');
    if(sel&&RQ.svcs.length){ sel.innerHTML='<option value="">*(すべて)</option>'+RQ.svcs.map(s=>'<option>'+s+'</option>').join(''); SVCF=''; }
    hUpdWb();
  }).catch(e=>rst('取得失敗: '+e.message,true));
}
function realInsp(){
  const st=RQ.expl||[];
  const bar=(k,n,col)=>{ const w=340, f=n?Math.max(2,Math.round(w*k/n)):0;
    return '<svg class="fv" viewBox="0 0 '+(w+16)+' 26" width="100%" height="26">'
      +'<rect x="8" y="6" width="'+w+'" height="14" rx="3" fill="#20201b" stroke="#3a3a32"/>'
      +'<rect x="8" y="6" width="'+f+'" height="14" rx="3" fill="'+col+'"/></svg>'; };
  let h='<h2>REAL '+RQ.db+'.'+RQ.tbl+'</h2><div class="sub">'+RQ.status+'</div>';
  h+='<div class="note">公開プレイグラウンド(user=demo・読み取り専用)の実テーブル。書き込みは打てないので、'
    +'INSERT の振付は縮尺シミュレータの担当。ここは<b>読み取りの答え合わせ</b>。</div>';
  if(RQ.sql) h+='<pre>'+RQ.sql.replace(/</g,'&lt;')+'</pre>';
  if(st.length){
    h+='<div class="sub">EXPLAIN indexes=1 — 索引が実際に落とした量</div>';
    st.forEach(s=>{
      const pct=s.gn?Math.round(1000*s.gk/s.gn)/10:0;
      h+='<div class="note" style="margin:6px 0 0"><b>'+s.name+'</b> — granule '+s.gk.toLocaleString()+' / '+s.gn.toLocaleString()
        +' ('+pct+'%)'+(s.pn?' ・ parts '+s.pk+'/'+s.pn:'')+(s.alg?'<br>Search Algorithm: '+s.alg:'')+'</div>'
        +bar(s.gk,s.gn,pct>60?'#e8d34a':(pct>20?'#f0a500':'#6fc78a'));
    });
  }
  if(RQ.parts.length){
    const top=RQ.parts.slice().sort((a,b)=>b.rows-a.rows).slice(0,8);
    h+='<div class="sub">system.parts(実物)</div><pre>name                        L      rows   granule\n'
      +top.map(q=>q.name.padEnd(26)+String(q.level).padEnd(3)+String(q.rows).padStart(10)+String(q.marks).padStart(9)).join('\n')+'</pre>';
  }
  if(RQ.gen.length){
    h+='<div class="sub">マージ世代の実例 — stockhouse.crypto_trades</div><pre>level  parts        rows       size\n'
      +RQ.gen.map(r=>String(r[0]).padStart(5)+String(r[1]).padStart(7)+String(r[2]).padStart(12)+'  '+r[3]).join('\n')+'</pre>'
      +'<div class="note">level は「何度畳まれたか」。level 0 の小さな Part が生まれ続け、'
      +'畳まれて世代が上がり、最終的に数十 GiB の塊になる。縮尺図では作れない数字。</div>';
  }
  if(RQ.ddl) h+='<div class="sub">SHOW CREATE TABLE</div><pre>'+RQ.ddl.replace(/</g,'&lt;')+'</pre>';
  hOpenInsp(h);
}

/* ---------- 4. クライアント(DOM)と共有UI ---------- */
let CURSQL=''; const cstatEl=document.getElementById('cstat');
const resEl=document.getElementById('resgrid'), rescEl=document.getElementById('rescard');
rescEl.querySelector('#resX').onclick=()=>{ resShown=false; };
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
#insp svg.fv{display:block;margin:8px 0 4px;background:#161613;border:1px solid #33332e;border-radius:8px;padding:4px}
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
const SVGW=376;
function svgWrap(h,inner){ return '<svg class="fv" viewBox="0 0 '+SVGW+' '+h+'" width="100%" height="'+h+'">'
  +'<style>.fv text{font:9.5px Inconsolata,Menlo,monospace;fill:#cdd2dc}.fv .lb{fill:#8b8b84;font-size:8.5px}'
  +'.fv .hi{fill:#e8d34a}.fv rect{stroke-width:1}</style>'+inner+'</svg>'; }
function svgIdxMap(p,gi){
  const n=p.granules.length, w=Math.min(96,Math.floor((SVGW-24)/n)), x0=12;
  let s='<text x="12" y="10" class="lb">primary.idx(granule ごと1エントリ・メモリ常駐)</text>';
  p.granules.forEach((g,i)=>{
    const x=x0+i*w, on=i===gi;
    s+='<rect x="'+x+'" y="16" width="'+(w-6)+'" height="18" rx="3" fill="'+(on?'#3a350f':'#23231f')+'" stroke="'+(on?'#e8d34a':'#4a4a40')+'"/>'
      +'<text x="'+(x+5)+'" y="29" '+(on?'class="hi"':'')+'>'+svcOf(g[0]).slice(0,2)+' '+fmtT(g[0])+'</text>'
      +'<line x1="'+(x+(w-6)/2)+'" y1="34" x2="'+(x+(w-6)/2)+'" y2="48" stroke="'+(on?'#e8d34a':'#4a4a40')+'"/>'
      +'<rect x="'+x+'" y="48" width="'+(w-6)+'" height="22" rx="3" fill="'+(on?'#1d3324':'#20201b')+'" stroke="'+(on?'#6fc78a':'#4a4a40')+'"/>'
      +'<text x="'+(x+5)+'" y="63" '+(on?'style="fill:#6fc78a"':'')+'>g'+i+' 8192行</text>';
  });
  s+='<text x="12" y="84" class="lb">エントリは先頭行のキーだけ。二分探索で「候補の granule」を決める</text>';
  return svgWrap(92,s);
}
function svgMark(p,gi){
  const nb=5, bw=(SVGW-24)/nb, hb=Math.min(nb-1,Math.floor(gi*1.4)%nb);
  let s='<text x="12" y="10" class="lb">Timestamp.bin — 圧縮ブロックの列(境界は granule と一致しない)</text>';
  for(let i=0;i<nb;i++){ const x=12+i*bw, on=i===hb;
    s+='<rect x="'+x+'" y="16" width="'+(bw-4)+'" height="20" rx="2" fill="'+(on?'#3a350f':'#23231f')+'" stroke="'+(on?'#e8d34a':'#4a4a40')+'"/>'
      +'<text x="'+(x+4)+'" y="30" class="lb">blk'+i+'</text>'; }
  const mx=12+hb*bw+bw*0.45;
  s+='<line x1="'+mx+'" y1="12" x2="'+mx+'" y2="40" stroke="#e8d34a" stroke-dasharray="2 2"/>'
    +'<text x="'+(mx+4)+'" y="50" class="hi">① 圧縮内オフセット</text>'
    +'<text x="12" y="70" class="lb">展開後のバイト列 — granule はここで 8,192 行ごとに切れる</text>';
  const gn=p.granules.length, gw=(SVGW-24)/gn;
  for(let i=0;i<gn;i++){ const x=12+i*gw, on=i===gi;
    s+='<rect x="'+x+'" y="76" width="'+(gw-4)+'" height="20" rx="2" fill="'+(on?'#1d3324':'#20201b')+'" stroke="'+(on?'#6fc78a':'#4a4a40')+'"/>'
      +'<text x="'+(x+4)+'" y="90" class="lb"'+(on?' style="fill:#6fc78a"':'')+'>g'+i+'</text>'; }
  const gx=12+gi*gw;
  s+='<line x1="'+gx+'" y1="72" x2="'+gx+'" y2="100" stroke="#6fc78a" stroke-dasharray="2 2"/>'
    +'<text x="'+(gx+4)+'" y="110" style="fill:#6fc78a">② 展開後オフセット</text>'
    +'<text x="12" y="126" class="lb">.mrk2 の1エントリ = (①, ②, 行数) の3つ組</text>';
  return svgWrap(132,s);
}
function svgMinmax(p,gi){
  const g=p.granules[gi], mn=Math.min.apply(null,g), mx=Math.max.apply(null,g);
  const X=v=>12+(v/960)*(SVGW-24);
  let s='<text x="12" y="10" class="lb">skp_idx_ts = granule ごとの minmax(Timestamp)</text>'
    +'<rect x="12" y="16" width="'+(SVGW-24)+'" height="16" rx="2" fill="#20201b" stroke="#4a4a40"/>';
  p.granules.forEach((gg,i)=>{ const a=X(Math.min.apply(null,gg)), b=X(Math.max.apply(null,gg)), on=i===gi;
    s+='<rect x="'+a+'" y="18" width="'+Math.max(3,b-a)+'" height="12" rx="2" fill="'+(on?'#3a350f':'#2b2b26')+'" stroke="'+(on?'#e8d34a':'#3a3a32')+'"/>'; });
  s+='<line x1="'+X(PRED)+'" y1="12" x2="'+X(PRED)+'" y2="46" stroke="#e08585"/>'
    +'<text x="'+(X(PRED)+4)+'" y="44" style="fill:#e08585">WHERE ts ≥ '+fmtT(PRED)+'</text>'
    +'<text x="12" y="60" class="lb">この granule: '+fmtT(mn)+'–'+fmtT(mx)+' → '+(mx<PRED?'範囲外なので読まずに落とせる':'交差するので候補に残る')+'</text>';
  return svgWrap(66,s);
}
function svgPartFiles(p){
  const cols=['Timestamp','ServiceName','SpanName','Duration'];
  let s='<text x="12" y="10" class="lb">'+p.name+'/ — 列ごとのファイル + Part 全体のファイル</text>';
  cols.forEach((c,i)=>{ const y=16+i*22;
    s+='<rect x="12" y="'+y+'" width="120" height="17" rx="2" fill="#23231f" stroke="#4a4a40"/><text x="17" y="'+(y+12)+'">'+c+'.bin</text>'
      +'<rect x="140" y="'+y+'" width="96" height="17" rx="2" fill="#23231f" stroke="#4a4a40"/><text x="145" y="'+(y+12)+'" class="lb">'+c.slice(0,9)+'.mrk2</text>'; });
  s+='<rect x="248" y="16" width="116" height="17" rx="2" fill="#3a350f" stroke="#e8d34a"/><text x="253" y="28" class="hi">primary.idx</text>'
    +'<rect x="248" y="38" width="116" height="17" rx="2" fill="#322a44" stroke="#b49ae0"/><text x="253" y="50" style="fill:#b49ae0">skp_idx_ts.idx2</text>'
    +'<rect x="248" y="60" width="116" height="17" rx="2" fill="#23231f" stroke="#4a4a40"/><text x="253" y="72" class="lb">minmax_Timestamp</text>'
    +'<rect x="248" y="82" width="116" height="17" rx="2" fill="#23231f" stroke="#4a4a40"/><text x="253" y="94" class="lb">count.txt / checksums</text>'
    +'<text x="12" y="112" class="lb">granule '+p.granules.length+' 個 ×8,192 行 = '+(p.granules.length*8192).toLocaleString()+' 行</text>';
  return svgWrap(118,s);
}
function svgPartitions(cut){
  const act=actParts(), days={};
  act.forEach(q=>{ (days[q.day]=days[q.day]||[]).push(q); });
  const ks=Object.keys(days).sort(), bw=Math.min(168,(SVGW-24)/Math.max(1,ks.length));
  let s='<text x="12" y="10" class="lb">PARTITION BY toDate(Timestamp) — 日ごとにディレクトリが分かれる</text>';
  ks.forEach((d,i)=>{ const x=12+i*bw, dead=days[d].every(q=>cut.indexOf(q.id)>=0);
    s+='<rect x="'+x+'" y="16" width="'+(bw-8)+'" height="52" rx="4" fill="'+(dead?'#201f1c':'#1d3324')+'" stroke="'+(dead?'#4a4a40':'#6fc78a')+'" stroke-dasharray="'+(dead?'3 2':'0')+'"/>'
      +'<text x="'+(x+6)+'" y="30" '+(dead?'class="lb"':'style="fill:#6fc78a"')+'>'+d.slice(0,4)+'-'+d.slice(4,6)+'-'+d.slice(6)+'</text>';
    days[d].forEach((q,j)=>{ s+='<rect x="'+(x+6+j*30)+'" y="38" width="26" height="20" rx="2" fill="'+(dead?'#23231f':'#23231f')+'" stroke="'+(dead?'#3a3a32':'#4a4a40')+'"/>'
      +'<text x="'+(x+10+j*30)+'" y="52" class="lb">'+q.granules.length+'g</text>'; });
    if(dead) s+='<text x="'+(x+bw-32)+'" y="30" style="fill:#e08585">✂</text>';
  });
  s+='<text x="12" y="86" class="lb">条件の日付に合わない partition は索引すら開かない(minmax_Timestamp.idx / partition.dat で判定)</text>';
  return svgWrap(92,s);
}
function compInsp(kind,cut){
  const p=actParts().filter(q=>q.day===TODAY)[0]||actParts()[0]; if(!p) return;
  if(kind==='part') openInsp('<h2>① partition 枝刈り</h2><div class="sub">実体: partition.dat / minmax_Timestamp.idx(Part ごと)</div>'
    +svgPartitions(cut||[])
    +'<div class="note">最初に見るのは Part の中ではなく Part のメタデータ。日付が条件から外れた partition の Part は、primary.idx も skip 索引も開かずに丸ごと捨てる。DROP PARTITION が安いのも同じ理由。</div>');
  else if(kind==='idx') openInsp('<h2>② primary.idx の二分探索</h2><div class="sub">実体: '+p.name+'/primary.idx(疎索引・メモリ常駐)</div>'
    +svgIdxMap(p,-1)
    +'<div class="note">エントリは granule ごとに1つ、しかも先頭行のキーだけ。ORDER BY (ServiceName, SpanName, Timestamp) の接頭辞で絞れるときは二分探索が効き、'
    +'時刻だけの条件では単一サービスに収まる granule しか落とせない(一般化排他)。行を特定する索引ではない。</div>');
  else openInsp('<h2>③ skip 索引</h2><div class="sub">実体: '+p.name+'/skp_idx_ts.idx2(ディスク上のファイル)</div>'
    +svgMinmax(p,0)
    +'<div class="note">granule ごとの要約(ここでは minmax(Timestamp))を読み、「含み得ない」granule を .bin を読む前に落とす。'
    +'偽陽性はあるが偽陰性はない — 落とした granule に該当行が居ることはない。生き残ったものだけ .mrk2 経由で .bin を読む。</div>');
}
function partInsp(p){
  const gs=p.granules;
  const idx=gs.map((g,i)=>String(i).padStart(2)+'  '+svcOf(g[0]).padEnd(10)+fmtT(g[0])).join('\n');
  openInsp('<h2>PART '+p.name+'</h2><div class="sub">granule ×'+gs.length+' ・ マージ世代 L'+p.lvl+' ・ 不変(immutable)</div>'
    +'<div class="note">Part はディレクトリ。列ごとの .bin、granule 境界の .mrk2、疎索引 primary.idx、skip 索引が同居する。'
    +'書き換えは常に新しい Part を生む(mutation は _'+mutSeq+' のように版が上がる)。</div>'
    +'<pre>'+p.name+'/\n  Timestamp.bin  ServiceName.bin  SpanName.bin …\n'
    +'  Timestamp.mrk2  ServiceName.mrk2 …   ← granule 境界のオフセット\n'
    +'  primary.idx                          ← granule ごと先頭キー(常駐)\n'
    +'  skp_idx_ts.idx2                      ← minmax(Timestamp)\n'
    +'  partition.dat / minmax_Timestamp.idx ← パーティション枝刈り用</pre>'
    +svgPartFiles(p)
    +'<div class="sub">primary.idx(granule → 先頭キー)</div>'+svgIdxMap(p,-1)
    +'<pre>gi  ServiceName  Timestamp\n'+idx+'</pre>'
    +'<div class="note">granule の行をクリックすると、その granule の中身が見える。</div>');
}
function granuleInsp(p,gi){
  const g=p.granules[gi], lo=gi*8192, hi=lo+8191;
  const rows=g.map((v,ci)=>{ const del=p.del[gi*GPR+ci], upd=p.upd[gi*GPR+ci];
    return fmtT(v).padEnd(7)+svcOf(v).padEnd(11)+SPANOF[svcOf(v)].padEnd(16)+(durOf(v)+'ms').padEnd(8)
      +(del?'← _row_exists=0':upd?'← 更新済み':''); }).join('\n');
  const mn=Math.min.apply(null,g), mx=Math.max.apply(null,g);
  const co=(gi*45+p.id*7)*1024, uo=gi*8192*42;
  openInsp('<h2>granule '+gi+' <span style="color:#98a0b3">/ rows '+lo.toLocaleString()+'–'+hi.toLocaleString()+'</span></h2>'
    +'<div class="sub">'+p.name+' ・ 8,192 行の窓(既定の index_granularity)</div>'
    +'<div class="note">granule は行の窓であって、行を個別に指す索引は無い。読むときはこの窓ごと .bin から取り出す。'
    +'下の行は縮尺表示(1 行 = 2,048 行ぶんの代表値)。</div>'
    +'<pre>Timestamp  ServiceName  SpanName        Duration\n'+rows+'</pre>'
    +'<div class="sub">① primary.idx が指す</div>'+svgIdxMap(p,gi)
    +'<div class="sub">② skip 索引が落とす</div>'+svgMinmax(p,gi)
    +'<div class="sub">③ mark が .bin の位置を指す</div>'+svgMark(p,gi)
    +'<pre>primary.idx[' +gi+ ']   = ('+svcOf(g[0])+', '+fmtT(g[0])+')\n'
    +'skp_idx_ts    = minmax '+fmtT(mn)+'–'+fmtT(mx)+'\n'
    +'Timestamp.mrk2['+gi+'] = (圧縮内 '+co.toLocaleString()+', 展開後 '+uo.toLocaleString()+', 行 8192)</pre>'
    +'<div class="note">mark が2段オフセットなのは、granule の境界と圧縮ブロックの境界が一致しないから。'
    +'primary.idx で「この granule が候補か」を決め、minmax で落とし、生き残ったものだけ mark 経由で .bin を読む。'
    +'(オフセットの数値はこの画面用の縮尺)</div>');
}
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

/* ---- レイアウトエンジン: ノードとエッジを宣言 → 依存の深さでランク付け ---- */
function dagLayout(N,E,o){
  o=o||{};
  const gx=o.gx||28, gy=o.gy||56, x0=o.x0||36, y0=o.y0||0;
  const byId=new Map(); N.forEach(n=>byId.set(n.id,n));
  const pre=new Map(), suc=new Map();
  N.forEach(n=>{ pre.set(n.id,[]); suc.set(n.id,[]); });
  E.forEach(e=>{ if(byId.has(e.a)&&byId.has(e.b)){ pre.get(e.b).push(e.a); suc.get(e.a).push(e.b); } });
  const rk=new Map();
  const rank=id=>{ if(rk.has(id)) return rk.get(id);
    rk.set(id,0); let r=0;
    pre.get(id).forEach(q=>{ r=Math.max(r,rank(q)+1); });
    rk.set(id,r); return r; };
  N.forEach(n=>rank(n.id));
  const rows=new Map();
  N.forEach(n=>{ const r=rk.get(n.id); if(!rows.has(r)) rows.set(r,[]); rows.get(r).push(n); });
  const LR=(o.dir==='LR');
  const mSize=n=>LR?n.w:n.h, cSize=n=>LR?n.h:n.w;
  const pos=new Map(); let m=LR?x0:y0, cmax=0;
  [...rows.keys()].sort((a,b)=>a-b).forEach(r=>{
    const row=rows.get(r).slice().sort((a,b)=>(a.ord||0)-(b.ord||0));
    let c=LR?y0:x0;
    row.forEach(n=>{
      let nc=c;
      const ps=pre.get(n.id);
      if(ps.length===1&&suc.get(ps[0]).length===1&&pos.has(ps[0])){
        const Q=pos.get(ps[0]), pc=LR?Q.y:Q.x, pcs=LR?Q.h:Q.w;
        nc=Math.max(c,Math.round(pc+(pcs-cSize(n))/2));
      }
      pos.set(n.id,LR?{x:m,y:nc,w:n.w,h:n.h}:{x:nc,y:m,w:n.w,h:n.h});
      c=nc+cSize(n)+gx;
    });
    cmax=Math.max(cmax,c-gx);
    m+=Math.max.apply(null,row.map(mSize))+gy;
  });
  const routes=E.filter(e=>pos.has(e.a)&&pos.has(e.b)).map(e=>{
    const A=pos.get(e.a), B=pos.get(e.b);
    if(LR){ const ay=A.y+A.h/2, by=B.y+B.h/2, x1=A.x+A.w, x2=B.x, mx=Math.round((x1+x2)/2);
      return {e,d:'LR',ay,by,x1,x2,mx,straight:Math.abs(ay-by)<2,lx:Math.round((mx+x2)/2),ly:by-12}; }
    const ax=A.x+A.w/2, bx=B.x+B.w/2, y1=A.y+A.h, y2=B.y, my=Math.round((y1+y2)/2);
    return {e,d:'TB',ax,bx,y1,y2,my,straight:Math.abs(ax-bx)<2,lx:bx,ly:Math.round((my+y2)/2)};
  });
  return {pos,routes,bottom:LR?cmax:m-gy,right:LR?m-gy:cmax};
}
function drawRoute(g,r,col,fat){
  const w=fat?2.5:1.5;
  if(r.d==='LR'){
    if(r.straight) g.moveTo(r.x1,r.ay).lineTo(r.x2-7,r.ay).stroke({width:w,color:col});
    else g.moveTo(r.x1,r.ay).lineTo(r.mx,r.ay).lineTo(r.mx,r.by).lineTo(r.x2-7,r.by).stroke({width:w,color:col});
    g.poly([r.x2,r.by,r.x2-8,r.by-4.5,r.x2-8,r.by+4.5]).fill(col);
    return;
  }
  if(r.straight){ g.moveTo(r.ax,r.y1).lineTo(r.ax,r.y2-7).stroke({width:w,color:col}); }
  else { g.moveTo(r.ax,r.y1).lineTo(r.ax,r.my).lineTo(r.bx,r.my).lineTo(r.bx,r.y2-7).stroke({width:w,color:col}); }
  g.poly([r.bx,r.y2,r.bx-4.5,r.y2-8,r.bx+4.5,r.y2-8]).fill(col);
}

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
function updatePartView(v,p,marks,hitFn,hi){
  // marks: {pruned, ranged:{from,to}, skip:Set(gi), scanned:Map(gi->hits), fp:Set(gi)}
  const w=partW(), h=partH(p);
  const pruned=marks&&marks.pruned;
  panel(v.bg,w,h,0xffffff,p.flash>0?0x2b8a3e:(pruned?0xe8e8e4:0xd9dbe0),p.flash>0?2:1,8);
  v.cont.alpha=pruned?0.45:1;
  v.hd.text=(p.hasProj?'⚡ projection ':'')+(pruned?'✂ partition 対象外':'');
  if(hi){
    const gh=p.granules.length*(CELLH+GAP)+10, iy=24;
    if(hi==='part'){ v.bg.roundRect(2,2,w-4,22,6).stroke({width:2,color:pruned?0xe03131:0xf59f00}); }
    else if(hi==='idx'){ v.bg.roundRect(24+GPR*(CELL+GAP)+2,iy,IDXW-6,gh,5).stroke({width:2,color:0xf0a500}); }
    else if(hi==='skip'){ v.bg.roundRect(24+GPR*(CELL+GAP)+IDXW+8,iy,SKW+18,gh,5).stroke({width:2,color:0x9775fa}); }
  }
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
    const eH=Object.keys(mvH).sort(), eD=Object.keys(mvD).sort(), eT=Object.keys(mvT);
    const bodies=[
      {key:'otel_traces_1h',nm:'otel_traces_1h',hdr:'hour   Service    count   avg(Dur)',rows:eH.slice(0,3).map(k=>{const q2=mvH[k]; const hs=+k.split('|')[0], sv=k.split('|')[1]; return fmtT(hs).padEnd(7)+sv.padEnd(11)+q2.c.toLocaleString().padEnd(8)+Math.round(q2.d/q2.c)+'ms';}),n:eH.length,extra:eH.length>3?'+'+(eH.length-3)+' 行':''},
      {key:'otel_traces_1d',nm:'otel_traces_1d',hdr:'day      count()',rows:eD.slice(0,2).map(d2=>(d2.slice(4,6)+'-'+d2.slice(6)).padEnd(9)+mvD[d2].toLocaleString()),n:eD.length,extra:''},
      {key:'otel_traces_trace_id_ts',nm:'otel_traces_trace_id_ts',hdr:'TraceId      Start–End      n',rows:eT.slice(-3).map(k=>(k+'…').padEnd(13)+(fmtT(mvT[k].s)+'–'+fmtT(mvT[k].e)).padEnd(14)+mvT[k].n),n:eT.length,extra:eT.length>3?'+'+(eT.length-3)+' 行':''},
    ];
    const geom=b=>{ const body=b.rows.length?b.rows.join('\n')+(b.extra?'\n'+b.extra:''):'(空 — INSERT 待ち)';
      return {body,h:26+(1+body.split('\n').length)*16+8}; };
    const q=[geom(bodies[0]),geom(bodies[1]),geom(bodies[2])];
    const colW=Math.floor((LTW-28)/2);
    // ---- 宣言: 何が何の下流か。座標は解く ----
    const LO=dagLayout([
      {id:'col', w:LTW,  h:34},
      {id:'raw', w:LTW,  h:50},
      {id:'wide',w:LTW,  h:hh},
      {id:'d1h', w:colW, h:q[0].h, ord:0},
      {id:'dtr', w:colW, h:q[2].h, ord:1},
      {id:'d1d', w:colW, h:q[1].h},
    ],[
      {a:'col', b:'raw', kind:'send'},
      {a:'raw', b:'wide',kind:'ing', mv:'transform_mv'},
      {a:'wide',b:'d1h', mv:'otel_traces_1h_mv'},
      {a:'wide',b:'dtr', mv:'otel_traces_trace_id_ts_mv'},
      {a:'d1h', b:'d1d', mv:'otel_traces_1d_mv'},
    ],{x0:36,y0:INS_Y+8,gx:28,gy:58});
    const P=id=>LO.pos.get(id);
    // Collector
    panel(colBg,LTW,34,0x26261f,0x4a4a40,1,8);
    colHint.x=LTW-12-colHint.width;
    col.x=P('col').x; col.y=P('col').y;
    // otel_raw
    panel(rawBg,LTW,50,0xfcfcf8,rawFlash>0?0x2b8a3e:0xdcdcd2,rawFlash>0?2:1);
    rawBg.rect(1,1,LTW-2,22).fill(0xf1f1e8);
    rawCount.x=LTW-12-rawCount.width;
    rawTx.text='ENGINE = Null — 行を溜めない受け口。着いたブロックに MV だけが発火する';
    raw.x=P('raw').x; raw.y=P('raw').y;
    if(rawFlash>0) rawFlash=Math.max(0,rawFlash-0.02);
    // ワイドテーブル
    panel(ltblBg,LTW,hh,0xffffff,flash>0?0x2b8a3e:0xd9dbe0,flash>0?2:1);
    ltblBg.rect(1,1,LTW-2,24).fill(0xfff9db);
    ltMg.x=LTW-24;
    ltCount.text=Math.round(dispRows).toLocaleString()+' 行'; ltCount.x=LTW-36-ltCount.width;
    ltbl.x=P('wide').x; ltbl.y=P('wide').y;
    if(flash>0) flash=Math.max(0,flash-0.015);
    G={rawCy:P('raw').y+26,rawBtm:P('raw').y+50,tblY:P('wide').y,segs:[]};
    // 派生テーブル
    const ids=['d1h','d1d','dtr'];
    bodies.forEach((d,i)=>{
      const o=tgt[i], pp=P(ids[i]);
      panel(o.bg,pp.w,q[i].h,0xffffff,tgtFlash[i]>0?0x9775fa:0xd9dbe0,tgtFlash[i]>0?2:1);
      o.bg.rect(1,1,pp.w-2,22).fill(0xfff9db);
      o.tx.text=d.hdr+'\n'+q[i].body;
      o.tx.style.fill=d.n?0x2a2e39:0x9aa0a8;
      o.tt.text='TABLE '+d.nm;
      o.c.__w=pp.w; if(o.tmg.text) o.tmg.x=pp.w-22;
      o.tcn.text=d.n+' 行'; o.tcn.x=pp.w-(i===0?34:10)-o.tcn.width;
      o.c.x=pp.x; o.c.y=pp.y;
      if(tgtFlash[i]>0) tgtFlash[i]=Math.max(0,tgtFlash[i]-0.02);
    });
    // パイプ(解かれた経路をそのまま描く)
    edges.clear();
    LO.routes.forEach((r,i)=>{
      const mv=r.e.mv, ing=r.e.kind==='ing';
      const pulse=mv?(ing?rawPulse:(pipePulse[mv]||0)):0;
      const col2=r.e.kind==='send'?0x9db08f:(ing?(pulse>0?0x2b8a3e:0xb9c2ae):(pulse>0?0x7048c8:0xb9a6dd));
      drawRoute(edges,r,col2,pulse>0);
      if(!mv) return;
      const ec=chip('s0e'+i,'mv',()=>tableInsp(mv));
      ec.textContent='MV '+mv+' ▸'+(ing?' 列に解く':'');
      ec.style.transform=pulse>0?'translate(0,-50%) scale(1.1)':'translate(0,-50%)';
      placeChip(ec,r.lx+10,r.ly);
      if(!ing){ G.segs.push({mv,x:r.bx,y1:r.y1,y2:r.y2,dcy:r.y2+30});
        if(pulse>0) pipePulse[mv]=Math.max(0,pulse-0.01); }
    });
    if(rawPulse>0) rawPulse=Math.max(0,rawPulse-0.01);
    const zc=chip('s0zoom','warn',()=>{ S1T='otel_traces'; zoomTo('S1'); });
    zc.textContent='⊕ 中を見る(Part / granule)';
    placeChip(zc,P('wide').x+150,P('wide').y+hh+16);
    const z1=chip('s0z1h','warn',()=>{ S1T='otel_traces_1h'; zoomTo('S1'); });
    z1.textContent='⊕ 中を見る(Part / 状態)';
    placeChip(z1,P('d1h').x+110,P('d1h').y+q[0].h+16);
    CONTENT_H=LO.bottom+140;
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
  let stubPulse=[0,0,0], insBatch=null, insPhase='', segC={};
  let sTiles=[], sPhase='', bornPid=0;
  let mgFold=null;
  const mgC=new PIXI.Container(), mgBg=new PIXI.Graphics();
  const mgH=textV('',11,0x5d4a86), mgS=textV('',10,0x8a8a80);
  mgH.x=14; mgH.y=9; mgS.x=14; mgS.y=25;
  mgC.addChild(mgBg,mgH,mgS); s.cont.addChild(mgC); mgC.visible=false;
  const mgPool=new Map();
  const mgGet=k=>{ let o=mgPool.get(k);
    if(!o){ o={g:new PIXI.Graphics(),t:textV('',9.5,0x2a2e39)}; mgC.addChild(o.g,o.t); mgPool.set(k,o); }
    o.g.visible=o.t.visible=true; return o; };
  const clearS=()=>{ sTiles.forEach(o=>o.c.destroy({children:true})); sTiles=[]; sPhase=''; bornPid=0; };
  const rawB=new PIXI.Container();
  const rawBg2=new PIXI.Graphics();
  const rawT2=textV('TABLE otel_raw ・ ENGINE = Null — Part を持たない(発火のみ)',10.5,0x8a8a80);
  rawT2.x=12; rawT2.y=6;
  rawB.addChild(rawBg2,rawT2); s.cont.addChild(rawB);
  rawB.eventMode='static'; rawB.cursor='pointer';
  rawB.on('pointertap',()=>tableInsp('otel_raw'));
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
  const rbG=new PIXI.Graphics(); s.cont.addChild(rbG);
  function partPos(i){ // 縦積み、partitionで字下げ
    const xs={}; let y=INS_Y+66;
    const list=actParts();
    return list.map((p,k)=>{ const pos={p,x:24,y}; y+=partH(p)+18; return pos; })[i];
  }
  s.enter=()=>{ insBatch=null; insPhase=''; };
  s.exit=()=>{ clearS(); };
  s.onEvent=e=>{
    if(e.t==='insert.arrive'){
      clearS(); sPhase='arrive';
      e.vals.forEach((v,i)=>{
        const c=new PIXI.Container(), g=new PIXI.Graphics(), tx2=textV(fmtT(v),10,0x2a2e39);
        g.roundRect(0,0,CELL,22,4).fill(0xfff3bf).stroke({width:1,color:0xdad9d0});
        c.addChild(g,tx2); tx2.x=CELL/2-tx2.width/2; tx2.y=4;
        stripTiles.addChild(c);
        const SP0=P&&LO?null:null; const bx0=38, by0=(LO?P('strip')?P('strip').y+12:INS_Y+116:INS_Y+116);
        const x0=bx0+i*(CELL+4);
        sTiles.push({v,c,g,x:x0,y:by0,tx:x0,ty:by0,arc:0,p:1,sc:0,d:i*4,k:null});
      });
    }
    else if(e.t==='insert.sorted'){
      sPhase='sorted';
      const order=e.vals, used={};
      sTiles.forEach(o=>{
        let idx=0;
        for(let i=0;i<order.length;i++){ if(order[i]===o.v&&!used[i]){ used[i]=1; idx=i; break; } }
        o.k=idx; o.tx=30+idx*(CELL+4);
        o.arc=14+Math.min(52,Math.abs(o.tx-o.x)*0.28); o.p=0;
        o.g.clear(); o.g.roundRect(0,0,CELL,22,4).fill(SVCTINT[svcOf(o.v)]||0xd3f9d8).stroke({width:1,color:0xc7cbb8});
      });
    }
    else if(e.t==='part.born'){
      const p=parts.find(x=>x.id===e.pid); if(p){ p.flash=1; p.filling=1; }
      sPhase='born'; bornPid=e.pid;
      setTimeout(()=>{ const q=parts.find(x=>x.id===e.pid); if(q) q.filling=0; clearS(); },1500);
    }
    else if(e.t==='agg.align'){ mgFold={g:e.groups,keys:e.keys,dup:e.dup,t:0}; setTimeout(()=>{ mgFold=null; },5200); }
    else if(e.t==='mv.fire'){
      const i={otel_traces_1h_mv:0,otel_traces_trace_id_ts_mv:1}[e.mv];
      if(i!=null) stubPulse[i]=1;
      const sc2=segC[e.mv];
      if(sc2){
        flyChip(e.inLbl||'ブロック',0x2f9e44,sc2.x,sc2.y1-14,sc2.x,(sc2.y1+sc2.y2)/2,0.02,()=>{
          if(i!=null) stubPulse[i]=1;
          flyChip(e.outLbl||'集計行',0x7048c8,sc2.x,(sc2.y1+sc2.y2)/2,sc2.x,sc2.y2-6,0.02,null,true);
        },true);
      }
    }
    else if(e.t==='part.merged'){ const p=parts.find(x=>x.id===e.into); if(p) p.flash=1; }
    else if(e.t==='delete.mask'||e.t==='mutation.rewrite'){ actParts().forEach(p=>p.flash=Math.max(p.flash||0,0.5)); }
  };
  s.tick=()=>{
    if(false&&S1T==='otel_traces_1h'){
      colB.visible=false; rawB.visible=false;
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
    // ---- 高さを見積もる → 宣言 → 解く ----
    const pw=partW(), fw2=2*pw+46;
    const REALV=RQ.on&&RQ.parts.length>0;
    const lv={};
    const bkt=x=>x===0?0:(x<10?1:(x<100?2:3));
    const BKN=['level 0(生まれたまま)','level 1–9','level 10–99','level 100+'];
    if(REALV) RQ.parts.forEach(q2=>{ const b=bkt(q2.level), L=lv[b]||(lv[b]={n:0,rows:0,marks:0,fresh:0,lo:q2.level,hi:q2.level});
      L.n++; L.rows+=q2.rows; L.marks+=q2.marks; L.fresh+=q2.fresh;
      L.lo=Math.min(L.lo,q2.level); L.hi=Math.max(L.hi,q2.level); });
    const lvk=Object.keys(lv).map(Number).sort((a,b)=>a-b);
    let ca=0, cb=0;
    actParts().forEach(q2=>{ const h3=partH(q2)+18; if(ca<=cb) ca+=h3; else cb+=h3; });
    const frameH=REALV?(56+lvk.length*38+26):(34+Math.max(120,Math.max(ca,cb))+8);
    const cardH=(n2,per,ch)=>26+Math.max(1,Math.ceil(n2/per))*(ch+GAP)+12;
    const hp=mvHParts.length?mvHParts:[{id:-1,rows:{},flash:0}];
    const hHs=hp.map(q2=>cardH(Object.keys(q2.rows).length,3,34));
    const d1hH=hHs.reduce((a,b)=>a+b+18,0)-18+26;
    const colW2=470;
    const dtrH=cardH(Object.keys(mvT).length,GPR,CELLH)+8;
    const d1dH=cardH(Object.keys(mvD).length,GPR,CELLH)+8;
    const stripOn=sTiles.length>0;
    const LN=[{id:'col',w:fw2,h:30},{id:'raw',w:fw2,h:26}];
    const LE=[{a:'col',b:'raw',kind:'send'}];
    if(stripOn){ LN.push({id:'strip',w:fw2,h:46}); LE.push({a:'raw',b:'strip',kind:'ing',mv:'transform_mv'}); LE.push({a:'strip',b:'frame',kind:'ing2'}); }
    else LE.push({a:'raw',b:'frame',kind:'ing',mv:'transform_mv'});
    LN.push({id:'frame',w:fw2,h:frameH},
      {id:'d1h',w:colW2,h:d1hH,ord:0},{id:'dtr',w:colW2,h:dtrH,ord:1},{id:'d1d',w:colW2,h:d1dH});
    LE.push({a:'frame',b:'d1h',mv:'otel_traces_1h_mv'},
      {a:'frame',b:'dtr',mv:'otel_traces_trace_id_ts_mv'},
      {a:'d1h',b:'d1d',mv:'otel_traces_1d_mv'});
    const LO=dagLayout(LN,LE,{x0:24,y0:INS_Y+6,gx:44,gy:52});
    const P=id=>LO.pos.get(id), F=P('frame');
    panel(colBg2,fw2,30,0x26261f,0x4a4a40,1,8);
    colH2.x=fw2-12-colH2.width;
    colB.visible=true; colB.x=P('col').x; colB.y=P('col').y;
    panel(rawBg2,fw2,26,0xfcfcf8,0xdcdcd2,1,6);
    rawB.visible=true; rawB.x=P('raw').x; rawB.y=P('raw').y;

    // INSERT 帯
    strip.clear();
    if(sTiles.length){
      const SP=P('strip');
      strip.roundRect(SP.x,SP.y,SP.w,46,6).fill(0xf2f1ec).stroke({width:1,color:0xdad9d0});
      const sc=chip('s1strip','',null);
      sc.textContent=sPhase==='arrive'?'OTLP バッチが届く(到着順・未ソート)'
        :sPhase==='sorted'?'ORDER BY (ServiceName, Timestamp) でソート中'
        :'8,192 行ごとに granule へ区切って新しい Part に書き出す';
      placeChip(sc,SP.x+SP.w/2,SP.y-2);
      if(sPhase==='sorted'){
        for(let k=0;k*GPR<sTiles.length;k++){
          const n=Math.min(GPR,sTiles.length-k*GPR);
          strip.roundRect(SP.x+14+k*GPR*(CELL+4)-4,SP.y+7,n*(CELL+4)+2,32,4).stroke({width:1,color:0x8fa07e,alpha:0.85});
        }
      }
      sTiles.forEach(o=>{
        if(o.d>0){ o.d--; o.c.alpha=0; return; }
        o.sc=Math.min(1,o.sc+0.16); o.c.alpha=1;
        o.x+=(o.tx-o.x)*0.18; o.y+=(o.ty-o.y)*0.18;
        o.p=Math.min(1,o.p+0.045);
        o.c.x=o.x; o.c.y=o.y-Math.sin(Math.PI*o.p)*o.arc;
        o.c.scale.set(o.sc);
      });
    } else { const sc=chips.get('s1strip'); if(sc){sc.remove(); chips.delete('s1strip');} }
    // Parts
    let yL=F.y+34, yR=F.y+34; const seen=new Set();
    if(!REALV) actParts().forEach(p=>{
      let v=views.get(p.id);
      if(!v){ v=buildPartView(); views.set(p.id,v); s.cont.addChild(v.cont); }
      seen.add(p.id);
      const left=yL<=yR, px=F.x+14+(left?0:pw+18), py=left?yL:yR;
      v.cont.x=px; v.cont.y=py;
      updatePartView(v,p,null);
      v.cont.alpha=p.filling?0.32:1;
      if(!v.cont.__wired){
        v.cont.eventMode='static'; v.cont.cursor='pointer';
        v.cont.on('pointertap',ev2=>{
          const lp=ev2.getLocalPosition(v.cont), pp=v.cont.__p; if(!pp) return;
          const gi2=Math.floor((lp.y-30)/(CELLH+GAP));
          if(gi2>=0&&gi2<pp.granules.length&&lp.x>=18&&lp.x<=24+GPR*(CELL+GAP)) granuleInsp(pp,gi2);
          else partInsp(pp);
        });
        v.cont.__wired=1;
      }
      v.cont.__p=p;
      if(bornPid===p.id){
        sTiles.forEach(o=>{ if(o.k==null) return;
          const nx=px+24+(o.k%GPR)*(CELL+GAP), ny=py+30+Math.floor(o.k/GPR)*(CELLH+GAP);
          if(o.tx!==nx||o.ty!==ny){ o.tx=nx; o.ty=ny; if(!o.flew){ o.arc=34; o.p=0; o.flew=1; } }
        });
      }
      const c=chip('s1p'+p.id,'',()=>partInsp(p));
      c.textContent=p.name+' · '+p.granules.length+'g';
      placeChip(c,px+partW()/2,py-2);
      if(left) yL=py+partH(p)+18; else yR=py+partH(p)+18;
    });
    const y=REALV?F.y+F.h:Math.max(yL,yR);
    views.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); views.delete(id); } });
    // TABLE 囲み(2列ぶんの幅)
    frameG.clear();
    frameG.roundRect(F.x,F.y,F.w,F.h,10).stroke({width:1.5,color:0xcfd2c8});
    rbG.clear();
    if(REALV){
      views.forEach(v=>{ v.cont.visible=false; });
      const maxR=Math.max.apply(null,lvk.map(k=>lv[k].rows))||1, BW=Math.min(460,F.w-420);
      const hc2=chip('rhdr','',null);
      hc2.textContent='system.parts(実物) ・ マージ世代ごと';
      placeChip(hc2,F.x+150,F.y+30);
      lvk.forEach((k,i)=>{
        const yy=F.y+50+i*38, L=lv[k], bw=Math.max(4,Math.round(BW*L.rows/maxR));
        rbG.roundRect(F.x+230,yy,BW,24,4).fill(0xf6f6f2).stroke({width:1,color:0xe4e4de});
        rbG.roundRect(F.x+230,yy,bw,24,4).fill(L.fresh?0xd3f9d8:0xdce5f0).stroke({width:1,color:L.fresh?0x2b8a3e:0xbecdde});
        const c=chip('rl'+k,L.fresh?'warn':'mv',null);
        c.textContent=BKN[k]+(k?'('+L.lo+'–'+L.hi+')':'')+' ・ '+L.n+' part';
        placeChip(c,F.x+120,yy+4);
        const c2=chip('rr'+k,'',null);
        c2.textContent=L.rows.toLocaleString()+' 行 ・ granule '+L.marks.toLocaleString();
        placeChip(c2,F.x+244+bw,yy+4);
      });
      const c3=chip('rsum','warn',()=>realInsp());
      c3.textContent='▣ EXPLAIN と DDL(実物)を見る';
      placeChip(c3,F.x+F.w-140,F.y+F.h-6);
    } else views.forEach(v=>{ v.cont.visible=true; });
    const mg3=chip('s1merge','warn',()=>doMerge());
    mg3.textContent='⇄ マージ';
    placeChip(mg3,F.x+F.w-60,F.y-4);
    const fc=chip('s1tbl','',()=>zoomTo('S0'));
    fc.textContent=REALV?('TABLE '+RQ.db+'.'+RQ.tbl+'(実データ)'):'TABLE otel_traces(⊖ テーブル層へ)';
    placeChip(fc,F.x+140,F.y-4);
    // 派生テーブルの物理層も上→下(S0 と同じ2列・granuleタイルで)
    const lxx=P('d1h').x, rxx=P('dtr').x, fbF=F.y+F.h, cy0=P('d1h').y;
    stubs.forEach(g2=>g2.clear());
    LO.routes.forEach((r,i)=>{
      const mv=r.e.mv, ing=r.e.kind==='ing'||r.e.kind==='ing2';
      const si={otel_traces_1h_mv:0,otel_traces_trace_id_ts_mv:1}[mv];
      const pu=si!=null?stubPulse[si]>0:false;
      const col=r.e.kind==='send'?0x9db08f:(ing?0xb9c2ae:(pu?0x7048c8:0xb9a6dd));
      drawRoute(stubs[2],r,col,pu);
      if(!mv) return;
      const c=chip('s1mv'+i,'mv',()=>tableInsp(mv));
      c.textContent='MV '+mv+' ▸'+(ing?' 列に解く':'');
      c.style.transform=pu?'translate(0,-50%) scale(1.1)':'translate(0,-50%)';
      placeChip(c,r.lx+10,r.ly);
      if(si!=null){ segC[mv]={x:r.bx,y1:r.y1,y2:r.y2};
        if(stubPulse[si]>0) stubPulse[si]=Math.max(0,stubPulse[si]-0.015); }
    });
    // タイル描画ヘルパ(状態行=タイル、右に primary.idx 先頭キー)
    const tileCard=(v,w,items,firstKey,band,op)=>{
      const cw=(op&&op.cw)||CELL, ch=(op&&op.ch)||CELLH, per=(op&&op.per)||GPR;
      if(!v.tiles){ v.tiles=new PIXI.Container(); v.cont.addChild(v.tiles); v.cells=new Map();
        v.idx=textV('',10,0x8a7300); v.idx.y=26; v.cont.addChild(v.idx); }
      v.idx.x=12+per*(cw+GAP)+16;
      items.forEach((it,ci)=>{ let cell=v.cells.get(ci);
        if(!cell){ cell={g:new PIXI.Graphics(),t:textV('',10.5,0x2a2e39),s:textV('',9,0x5d4a86)}; v.tiles.addChild(cell.g); v.tiles.addChild(cell.t); v.tiles.addChild(cell.s); v.cells.set(ci,cell); }
        const x=12+(ci%per)*(cw+GAP), yy=26+Math.floor(ci/per)*(ch+GAP);
        cell.g.clear(); cell.g.roundRect(x,yy,cw,ch,4).fill(it.bg).stroke({width:1,color:0xdde0e4});
        cell.t.text=it.txt; cell.t.x=x+(it.sub?5:cw/2-cell.t.width/2); cell.t.y=yy+(it.sub?3:5);
        cell.s.text=it.sub||''; cell.s.x=x+5; cell.s.y=yy+16; cell.s.visible=!!it.sub;
        cell.g.visible=cell.t.visible=true;
      });
      for(let ci=items.length;v.cells.has(ci);ci++){ const cell=v.cells.get(ci); cell.g.visible=cell.t.visible=cell.s.visible=false; }
      const rowsN=Math.max(1,Math.ceil(items.length/per));
      const h2=26+rowsN*(ch+GAP)+12;
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
    placeChip(hc,lxx+108,hy-10);
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
      const items=keys.map(k=>{ const hs=+k.split('|')[0], sv=k.split('|')[1], r=q.rows[k];
        return {txt:fmtT(hs)+' '+sv.slice(0,2),sub:'Σ'+Math.round(r.d/1000)+'s/'+r.c,bg:SVCTINT[sv]||0xeef0f2}; });
      const fk=keys[0]?fmtT(+keys[0].split('|')[0])+' '+keys[0].split('|')[1].slice(0,2):'—';
      const h2=tileCard(v,colW2,items,fk,0xf1ecfa,{cw:104,ch:34,per:3});
      v.cont.x=lxx; v.cont.y=hy;
      if(q.id>0){ const pc=chip('s1hp'+q.id,'',()=>tableInsp('otel_traces_1h'));
        pc.textContent='part '+q.id+' ・ granule ×'+Math.max(1,Math.ceil(keys.length/GPR))+' ・ 状態 '+keys.length+' 行';
        placeChip(pc,lxx+130,hy+1); }
      if(q.flash>0) q.flash=Math.max(0,q.flash-0.02);
      hy+=h2+18;
    });
    hViews.forEach((v,id)=>{ if(!seenH.has(id)){ v.cont.destroy({children:true}); hViews.delete(id); } });
    // AggregatingMergeTree のマージ劇場
    mgPool.forEach(o=>{ o.g.visible=o.t.visible=false; });
    if(mgFold&&mgFold.g.length){
      mgFold.t++;
      s.cont.addChild(mgC);
      const gs=mgFold.g, mw=colW2-28, mh=44+gs.length*42+16;
      mgC.visible=true; mgC.alpha=0.98; mgC.x=lxx+14; mgC.y=cy0+18;
      panel(mgBg,mw,mh,0xfffdf7,0x9775fa,2,10);
      mgBg.rect(1,1,mw-2,26).fill(0xf1ecfa);
      mgH.text='AggregatingMergeTree のマージ — 同じ ORDER BY キーの状態を結合';
      mgS.text='キー '+mgFold.keys+' 種 ・ 複数 Part に同じキー '+mgFold.dup+' 件'+(mgFold.dup?' → 状態を足して1行に':' → 今回は結合なし、状態はそのまま運ばれる')+'(avg は確定しない)';
      const T=mgFold.t, p1=Math.max(0,Math.min(1,(T-16)/38)), p2=Math.max(0,Math.min(1,(T-58)/26));
      gs.forEach((gr,i)=>{
        const gy=48+i*42, hs=+gr.key.split('|')[0], sv=gr.key.split('|')[1];
        const kk=mgGet('k'+i); kk.g.clear();
        kk.g.roundRect(12,gy,92,26,4).fill(SVCTINT[sv]||0xeef0f2).stroke({width:1,color:0xdde0e4});
        kk.t.text=fmtT(hs)+' '+sv; kk.t.x=17; kk.t.y=gy+8;
        gr.srcs.slice(0,2).forEach((sr,j)=>{
          const o=mgGet('s'+i+'_'+j), x0=116+j*104, x1=150+j*8, x=x0+(x1-x0)*p1;
          o.g.clear(); o.g.roundRect(x,gy,96,26,4).fill(0xffffff).stroke({width:1,color:0xb9a6dd});
          o.g.alpha=o.t.alpha=1-p2*0.5;
          o.t.text='Σ'+Math.round(sr.d/1000)+'s/'+sr.c; o.t.x=x+7; o.t.y=gy+8;
          if(j===0&&gr.srcs.length>1){ const pl=mgGet('p'+i); pl.g.clear(); pl.t.text='+'; pl.t.x=x+104; pl.t.y=gy+8; pl.t.alpha=1-p2; }
        });
        if(p2>0){
          const combi=gr.srcs.length>1, col=combi?0x2b8a3e:0x9aa0a8, bgc=combi?0xe2f3e6:0xf4f4f0;
          const o=mgGet('r'+i);
          o.g.clear(); o.g.roundRect(276,gy,120,26,4).fill(bgc).stroke({width:combi?2:1,color:col});
          o.g.alpha=o.t.alpha=p2;
          o.t.text='Σ'+Math.round(gr.res.d/1000)+'s/'+gr.res.c; o.t.x=283; o.t.y=gy+8;
          const ar=mgGet('a'+i); ar.g.clear();
          ar.g.moveTo(258,gy+13).lineTo(270,gy+13).stroke({width:1.5,color:col});
          ar.g.poly([276,gy+13,269,gy+9,269,gy+17]).fill(col); ar.g.alpha=p2;
          ar.t.text=combi?'':'そのまま'; ar.t.x=236; ar.t.y=gy+30; ar.t.alpha=p2*0.9;
        }
      });
    } else { mgC.visible=false; }
    // trace_id_ts(右列)
    const eT2=Object.keys(mvT);
    const tItems=eT2.map(k=>({txt:k.slice(0,4),bg:0xf3ecfa}));
    const tH=tileCard(tZone,colW2,tItems,eT2[0]?eT2[0].slice(0,8)+'…':'—',0xf1ecfa);
    tZone.cont.x=rxx; tZone.cont.y=cy0;
    const tc2=chip('s1t-h','mv',()=>tableInsp('otel_traces_trace_id_ts'));
    tc2.textContent='TABLE otel_traces_trace_id_ts ・ '+eT2.length+' 行';
    placeChip(tc2,rxx+128,cy0-10);
    // 1d(左列の下、1h からのカスケード)
    const eD2=Object.keys(mvD).sort();
    const dItems=eD2.map(d2=>({txt:d2.slice(4,6)+'/'+d2.slice(6),bg:0xfff3bf}));
    const dH=tileCard(dZone,colW2,dItems,eD2[0]?eD2[0].slice(4,6)+'-'+eD2[0].slice(6):'—',0xfff3bf);
    dZone.cont.x=P('d1d').x; dZone.cont.y=P('d1d').y;
    const dc2=chip('s1d-h','mv',()=>tableInsp('otel_traces_1d'));
    dc2.textContent='TABLE otel_traces_1d ・ '+eD2.length+' 行';
    placeChip(dc2,P('d1d').x+104,P('d1d').y-10);
    CONTENT_H=LO.bottom+150;
  };
  return s;
})();

/* ===== S2 クエリ実行 ===== */
scenes.S2=(()=>{
  const s=mkScene('S2');
  const views=new Map();
  const rteG=new PIXI.Graphics(); s.cont.addChild(rteG);
  const laneG=[0,1,2].map(()=>{ const g=new PIXI.Graphics(); s.cont.addChild(g); return g; });
  const secL=textV('STORAGE — 枝刈り',11,0x9a9a90,false);
  const secR=textV('COMPUTE — CPU コアのレーン',11,0x9a9a90,false);
  s.cont.addChild(secL,secR);
  const introG=new PIXI.Graphics(); const introT=textV('otel_traces を Part 群に開く…',12,0x6b6b60,false);
  s.cont.addChild(introG,introT);
  let marks=new Map(), lanes=[], intro=0, resRows=null, lk=null, curHit=null, hiCol=null, cutIds=[];
  const lkG=new PIXI.Graphics(); s.cont.addChild(lkG);
  const lkTx=textV('',11.5,0x5d4a86); s.cont.addChild(lkTx);
  let LG=[{x:460,y:INS_Y+60,w:420,h:114},{x:460,y:INS_Y+186,w:420,h:114},{x:460,y:INS_Y+312,w:420,h:114}];
  let SG={x:24,y:INS_Y+60,w:partW(),h:200}, KG=null, LOR=null;
  function laneGeom(i){ return LG[i]; }
  function resGeom(){ const g=laneGeom(0); return {x:g.x+g.w+16,y:g.y+8,w:Math.max(210,Math.min(280,STW()-(g.x+g.w)-30))}; }
  s.enter=()=>{ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); intro=1; resRows=null; lk=null; curHit=null; LKUP=0; hiCol=null; cutIds=[]; };
  s.exit=()=>{ setStage(0); LKUP=0; hiCol=null; };
  s.onEvent=e=>{
    if(e.t==='query.start'){ marks=new Map(); lanes=[0,1,2].map(i=>({q:[],done:0,sum:0,cur:null})); resRows=null; lk=null; curHit=null; LKUP=0; hiCol=null; cutIds=[]; setStage(1); }
    else if(e.t==='trace.lookup'){ lk=e; LKUP=1; const w=Math.floor(e.s/TRWIN); curHit=(p,gi,ci,v)=>p.day===TODAY&&Math.floor(v/TRWIN)===w&&!p.del[gi*GPR+ci]; toast('⓪ まず otel_traces_trace_id_ts を TraceId で引く(ORDER BY の先頭なので一発)→ Start–End の時間範囲を得る'); }
    else if(e.t==='prune.partition'){ hiCol='part'; cutIds=e.cut.slice(); e.cut.forEach(id=>{ marks.set(id,Object.assign(getM(id),{pruned:true})); }); toast('① Partition 枝刈り: 日付条件に合わない Partition は索引すら見ない'); }
    else if(e.t==='prune.primary'){ hiCol='idx'; e.plan.forEach(pl=>{ getM(pl.pid).pkKeep=new Set(pl.keep); }); toast(e.note||'① 主キーの境界だけで読む granule を確定'); }
    else if(e.t==='prune.skip'){
      hiCol='skip';
      e.skipped.forEach(sk=>{ const m=getM(sk.pid); m.skip=m.skip||new Set(); m.skip.add(sk.gi); });
      setStage(2);
      toast(e.note||'② skip idx で読む前に落とす');
    }
    else if(e.t==='scan.assign'){ hiCol=null; e.queues.forEach((q,i)=>{ lanes[i].q=q.map(x=>({pid:x.pid,gi:x.gi})); }); setStage(3); toast('③ 生き残り granule を '+LANES+' レーン(= max_threads のCPUスレッド)のキューへ配分'); }
    else if(e.t==='scan.granule'){
      const l=lanes[e.lane]; l.done++; l.sum+=e.hits; l.cur={pid:e.pid,gi:e.gi};
      const m=getM(e.pid); m.scanned=m.scanned||new Set(); m.scanned.add(e.gi);
    }
    else if(e.t==='agg.merge'){
      setStage(4);
      lanes.forEach((l,i)=>{ const g=laneGeom(i); flyChip('Σ '+l.sum.toLocaleString(),[0x0e7490,0xb45309,0x7c3aed][i],g.x+g.w*0.86,g.y+g.h/2,g.x+g.w*0.5,-40,0.022); });
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
    // ---- 宣言(左→右): [trace索引] / storage → レーン×3 ----
    const sh=Math.max(160,actParts().reduce((a,q2)=>a+partH(q2)+16,0)-16);
    const laneW=Math.max(260,Math.min(520,STW()-(24+partW()+70)-300));
    const N2=[{id:'store',w:partW(),h:sh,ord:0}];
    const E2=[];
    if(lk){ N2.push({id:'lk',w:partW(),h:52,ord:-1}); }
    [0,1,2].forEach(i=>{ N2.push({id:'l'+i,w:laneW,h:114,ord:i}); E2.push({a:'store',b:'l'+i}); });
    LOR=dagLayout(N2,E2,{dir:'LR',x0:24,y0:INS_Y+(lk?60:36),gx:70,gy:12});
    SG=LOR.pos.get('store'); KG=lk?LOR.pos.get('lk'):null;
    [0,1,2].forEach(i=>{ LG[i]=LOR.pos.get('l'+i); });
    let y=SG.y; const seen=new Set();
    actParts().forEach(p=>{
      let v=views.get(p.id);
      if(!v){ v=buildPartView(); views.set(p.id,v); s.cont.addChild(v.cont); }
      seen.add(p.id);
      v.cont.x=SG.x; v.cont.y=y; v.cont.alpha=(1-intro);
      updatePartView(v,p,marks.get(p.id),curHit,hiCol);
      const c=chip('s2p'+p.id,(marks.get(p.id)||{}).pruned?'warn':'',()=>partInsp(p));
      c.textContent=p.name+(marks.get(p.id)&&marks.get(p.id).pruned?' ✂':'');
      placeChip(c,24+partW()/2,y-2);
      y+=partH(p)+16;
    });
    views.forEach((v,id)=>{ if(!seen.has(id)){ v.cont.destroy({children:true}); views.delete(id); } });
    // いま参照している部品(クリックで図解ドック)
    if(hiCol){
      const cc=chip('s2comp',hiCol==='skip'?'mv':'warn',()=>compInsp(hiCol,cutIds));
      cc.textContent=hiCol==='part'?'① partition.dat / minmax_Timestamp.idx ▸ 図解'
        :hiCol==='idx'?'② primary.idx(疎索引・常駐)を二分探索 ▸ 図解'
        :'③ skp_idx_ts.idx2 の minmax で落とす ▸ 図解';
      placeChip(cc,24+partW()/2,INS_Y+22);
    }
    // otel_traces_trace_id_ts ルックアップカード
    if(lk){
      const K=KG||{x:SG.x,y:INS_Y+56,w:SG.w,h:52};
      lkG.clear();
      lkG.roundRect(K.x,K.y,K.w,52,8).fill(0xf9f6ff).stroke({width:2,color:0x9775fa});
      lkG.rect(K.x+1,K.y+1,K.w-2,16).fill(0xeee6fb);
      lkG.moveTo(K.x+K.w/2,K.y+52).lineTo(K.x+K.w/2,SG.y-7).stroke({width:1.5,color:0x9775fa});
      lkG.poly([K.x+K.w/2,SG.y,K.x+K.w/2-4.5,SG.y-8,K.x+K.w/2+4.5,SG.y-8]).fill(0x9775fa);
      lkTx.text=lk.tid+'…   Start '+fmtT(lk.s)+' – End '+fmtT(lk.e)+' ・ '+lk.n+' span';
      lkTx.x=K.x+12; lkTx.y=K.y+24; lkTx.visible=true;
      const c=chip('s2lk','mv',null);
      c.textContent='① TABLE otel_traces_trace_id_ts ▸ TraceId で範囲を特定';
      placeChip(c,K.x+K.w/2,K.y+2);
    } else { lkG.clear(); lkTx.visible=false; }
    // レーン
    rteG.clear();
    if(LOR) LOR.routes.forEach(r=>{ drawRoute(rteG,r,0xc6cdd6,false); });
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
    CONTENT_H=Math.max(y+60,LG[2].y+180);

  };
  return s;
})();

/* ===== S3 クラスタ層 ===== */
scenes.S3=(()=>{
  const s=mkScene('S3');
  const gL=new PIXI.Graphics(), gN=new PIXI.Graphics();
  s.cont.addChild(gL,gN);
  const T=(txt,size,col)=>{ const o=textV(txt,size,col); s.cont.addChild(o); return o; };
  const n1t=T('node-1  clickhouse-server',11,0xe8e6dc);
  const n1s=T('ローカル storage — Part はここに書かれる',10,0x9a9a90);
  const n2t=T('node-2  clickhouse-server',11,0xe8e6dc);
  const n2s=T('',10,0x9a9a90);
  const ktt=T('Keeper ×3 — 「誰が何を持つか」の唯一の台帳(メタデータのみ)',10.5,0xb49ae0);
  const note=T('',10,0x8a8a80);
  const hit=new PIXI.Container(), hg=new PIXI.Graphics();
  const addT=textV('＋ レプリカを追加(ReplicatedMergeTree 化)',11.5,0xfaff69);
  hit.addChild(hg,addT); s.cont.addChild(hit);
  hit.eventMode='static'; hit.cursor='pointer';
  hit.on('pointertap',()=>doAddReplica());
  let kPulse=0, n2Pulse=0, fPulse=0;
  const geo=()=>{ const w=Math.min(620,STW()-80);
    const LOS=dagLayout([{id:'n1',w,h:126},{id:'kp',w,h:46},{id:'n2',w,h:126}],
      [{a:'n1',b:'kp'},{a:'kp',b:'n2'}],{x0:40,y0:INS_Y+34,gy:52});
    const A=LOS.pos.get('n1'), K=LOS.pos.get('kp'), B=LOS.pos.get('n2');
    return {x:A.x,w,y1:A.y,h1:A.h,yk:K.y,y2:B.y,h2:B.h,routes:LOS.routes}; };
  s.enter=()=>{ kPulse=n2Pulse=fPulse=0; };
  s.onEvent=e=>{
    if(e.t==='repl.setup'){ kPulse=1; toast('ENGINE を ReplicatedMergeTree に置き換え、Keeper のパスとレプリカ名を与える。データではなくメタデータが Keeper に載る'); }
    else if(e.t==='repl.log'){ kPulse=1; toast('① node-1 が Part を書いたら、Keeper のレプリケーションログに1行記帳する'); }
    else if(e.t==='repl.fetch'){ kPulse=1; fPulse=1;
      const G=geo();
      flyChip('fetch part',0x7048c8,G.x+30,G.y1+G.h1,G.x+30,G.y2,0.022,null,true);
      toast('② node-2 はログを見て、node-1 から Part 本体を fetch する。Keeper が運ぶのは指示、データはレプリカ間で直接'); }
    else if(e.t==='repl.synced'){ n2Pulse=1; }
  };
  s.tick=()=>{
    const G=geo(), act=actParts();
    gN.clear(); gL.clear();
    const drawNode=(y,h,ids,pulse)=>{
      gN.roundRect(G.x,y,G.w,h,10).fill(0x26261f).stroke({width:pulse>0?2:1,color:pulse>0?0x6fc78a:0x4a4a40});
      gN.roundRect(G.x+12,y+40,G.w-24,h-52,8).fill(0xf8f8f6).stroke({width:1,color:0xd9dbe0});
      ids.slice(0,8).forEach((q,i)=>{
        const bx=G.x+24+(i%4)*(Math.min(120,(G.w-70)/4)+8), by=y+52+Math.floor(i/4)*34;
        gN.roundRect(bx,by,Math.min(120,(G.w-70)/4),26,5).fill(0xeef0f2).stroke({width:1,color:0xdde0e4});
      });
    };
    drawNode(G.y1,G.h1,act,0);
    n1t.x=G.x+14; n1t.y=G.y1+11; n1s.x=G.x+14; n1s.y=G.y1+26;
    act.slice(0,8).forEach((q,i)=>{
      const cw2=Math.min(120,(G.w-70)/4), bx=G.x+24+(i%4)*(cw2+8), by=G.y1+52+Math.floor(i/4)*34;
      const c=chip('n1p'+q.id,'',()=>zoomTo('S1'));
      c.textContent=q.name.slice(9)+' · '+q.granules.length+'g';
      placeChip(c,bx+cw2/2,by+1);
    });
    const c1=chip('n1z','warn',()=>zoomTo('S0'));
    c1.textContent='⊕ この Node のテーブルを見る';
    placeChip(c1,G.x+G.w-96,G.y1+30);
    // Keeper
    ktt.x=G.x; ktt.y=G.yk-20; ktt.alpha=REPL?1:0.45;
    const kw=Math.min(150,(G.w-40)/3);
    for(let i=0;i<3;i++){
      gN.roundRect(G.x+i*(kw+16),G.yk,kw,46,8)
        .fill(kPulse>0?0x322a44:0x24222c).stroke({width:kPulse>0?2:1,color:REPL?0xb49ae0:0x3a3a35});
    }
    for(let i=0;i<3;i++){ const kc=chip('kp'+i,'mv',null);
      kc.textContent='keeper-'+(i+1); kc.style.opacity=REPL?'1':'0.45';
      placeChip(kc,G.x+i*(kw+16)+kw/2,G.yk+24); }
    if(kPulse>0) kPulse=Math.max(0,kPulse-0.012);
    // 調整の線(node ↔ Keeper)
    const kc2=G.x+G.w/2;
    gL.moveTo(kc2,G.y1+G.h1).lineTo(kc2,G.yk).stroke({width:1.5,color:REPL?(kPulse>0?0xb49ae0:0x6b5f80):0x3a3a35});
    if(REPL){
      gL.moveTo(kc2,G.yk+46).lineTo(kc2,G.y2).stroke({width:1.5,color:kPulse>0?0xb49ae0:0x6b5f80});
      // データ経路(レプリカ間の fetch)は左を迂回
      const fx=G.x+30, col=fPulse>0?0x7048c8:0xc7bcd8;
      gL.moveTo(fx,G.y1+G.h1).lineTo(fx,G.y2-8).stroke({width:fPulse>0?2.5:1.2,color:col});
      gL.poly([fx,G.y2,fx-4.5,G.y2-8,fx+4.5,G.y2-8]).fill(col);
      if(fPulse>0) fPulse=Math.max(0,fPulse-0.01);
      drawNode(G.y2,G.h2,n2Parts.map(id=>parts.find(x=>x.id===id)).filter(Boolean),n2Pulse);
      n2t.x=G.x+14; n2t.y=G.y2+11; n2s.x=G.x+14; n2s.y=G.y2+26; n2t.alpha=n2s.alpha=1;
      n2s.text='ローカル storage — 同じ Part を1コピー持つ('+n2Parts.length+' / '+act.length+' 同期済み)';
      n2Parts.slice(0,8).forEach((id,i)=>{ const q=parts.find(x=>x.id===id); if(!q) return;
        const cw2=Math.min(120,(G.w-70)/4), bx=G.x+24+(i%4)*(cw2+8), by=G.y2+52+Math.floor(i/4)*34;
        const c=chip('n2p'+id,'',null); c.textContent=q.name.slice(9)+' · '+q.granules.length+'g';
        placeChip(c,bx+cw2/2,by+1); });
      if(n2Pulse>0) n2Pulse=Math.max(0,n2Pulse-0.015);
      hit.visible=false;
      const lc=chip('s3lb','mv',null);
      lc.textContent='調整 = Keeper 経由 / データ = レプリカ間の fetch';
      placeChip(lc,kc2,G.yk+70);
      note.text='ノード同士は「相談」しない。書いた事実は Keeper のログに載り、それを見たレプリカが本体を取りに行く。'
        +'Cloud の SharedMergeTree では、この本体が共有ストレージ1コピーに置き換わる。';
    } else {
      hit.visible=true;
      hg.clear(); hg.roundRect(G.x,G.y2,G.w,G.h2,10).fill(0x201f18).stroke({width:2,color:0x5a5a2a});
      addT.x=G.x+20; addT.y=G.y2+G.h2/2-8;
      n2t.alpha=n2s.alpha=0;
      note.text='いまは単一ノード(MergeTree)。Keeper は暗いまま — 台帳に載せるものが無い。';
    }
    note.x=G.x; note.y=G.y2+G.h2+22;
    CONTENT_H=G.y2+G.h2+120;
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
  SCROLL=0;
  curName=name; SCENE=name; cur=scenes[name];
  world.addChild(cur.cont);
  world.addChild(flyC); // 最前面へ
  cur.enter(); renderCrumb();
}
function zoomTo(name){ if(busy&&name!==curName) return toast('実行中です。終わってから','warn'); switchTo(name); }
const crumbEl=document.getElementById('crumb');
function renderCrumb(){
  const seg=(lbl,on,fn)=>'<span class="cr'+(on?' on':'')+'" data-go="'+(fn||'')+'">'+lbl+'</span>';
  let html=seg('cluster',curName==='S3',curName==='S3'?'':'S3')+'<span class="sep">›</span>'+seg('node-1',false,'S0')+'<span class="sep">›</span>';
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
  if(STMT==='del') return (SVCF?'':'-- ServiceName 未指定 → 既定の checkout を対象にする\n')+"DELETE FROM otel_traces WHERE ServiceName = '"+(SVCF||'checkout')+"';";
  if(STMT==='upd') return (SVCF?'':'-- ServiceName 未指定 → 既定の frontend を対象にする\n')+"ALTER TABLE otel_traces UPDATE SpanAttributes['tier'] = 'vip' WHERE ServiceName = '"+(SVCF||'frontend')+"';";
  return 'ALTER TABLE otel_traces ADD PROJECTION by_service (SELECT * ORDER BY ServiceName);';
}
const wbEl=document.getElementById('wbsql');
function updWb(){ wbEl.textContent=wbSQL(); }
wbEl.onclick=()=>openInsp('<h2>🛠 Workbench</h2><div class="sub">実行される文(パラメータ連動)</div><pre>'+wbSQL().replace(/&/g,'&amp;').replace(/</g,'&lt;')+'</pre>');
document.getElementById('stmtSel').onchange=e=>{ STMT=e.target.value; updWb(); };
document.getElementById('bRun').onclick=()=>{ ({sel:doSelect,tid:doTraceSelect,del:doDelete,upd:doUpdate,proj:doProj})[STMT](); };
hUpdWb=updWb; hOpenInsp=openInsp; hZoomS1=()=>{ S1T='otel_traces'; zoomTo('S1'); };
document.getElementById('dataSel').onchange=e=>{
  RQ.on=(e.target.value==='real');
  if(RQ.on){ realBoot().then(()=>{ hZoomS1();
      toast('実データモード: '+RQ.db+'.'+RQ.tbl+' を system.parts から読む。20 秒ごとに更新し、生まれた/消えた Part を報告する'); }); }
  else { rst(''); RQ.seen=new Set(); toast('縮尺シミュレータに戻した'); }
};
if(rstEl()) rstEl().onclick=()=>{ if(RQ.on) realInsp(); };
setInterval(()=>{ if(RQ.on) realParts().catch(e=>rst('取得失敗: '+e.message,true)); },20000);
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
  parts=[]; seq=0; mvH={}; mvD={}; mvT={}; trSeq=0; mvHParts=[]; hpSeq=0; S1T='otel_traces'; REPL=false; n2Parts=[]; projOn=false; mutSeq=6;
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
  rescEl.style.display=resShown?'block':'none';
  if(resShown){ rescEl.style.top=y+'px'; y+=rescEl.offsetHeight+8; }
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
  if(cur){ try{ cur.tick(); }catch(e){ window.__v6tick=e.message; } }
  tickFly();
  chips.forEach((c,k)=>{ if(c.__seen!==frame){ c.remove(); chips.delete(k); } });
});
window.__v6={ get parts(){return parts;}, get scene(){return curName;}, EVLOG, zoomTo };
})().catch(e=>{ document.title='PXERR: '+(e&&e.message||e); console.error(e); });

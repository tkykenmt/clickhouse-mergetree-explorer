// 英語版スモークテスト: ロードエラーなし・操作可能・日本語が視えないこと
const fs=require('fs'),path=require('path');
const { JSDOM, VirtualConsole } = require('jsdom');
const html=fs.readFileSync(path.join(__dirname,'../en/index.html'),'utf8');
const vc=new VirtualConsole(); const errors=[];
vc.on('jsdomError',e=>errors.push(e.message));
const dom=new JSDOM(html,{runScripts:'dangerously',virtualConsole:vc,pretendToBeVisual:true,url:'https://localhost/'});
setTimeout(()=>{
  const d=dom.window.document;
  const fails=[];
  if(errors.length) fails.push('load errors: '+errors.join(' / '));
  if(!d.body.classList.contains('L1')) fails.push('not L1');
  d.getElementById('btnInsert').dispatchEvent(new dom.window.MouseEvent('click',{bubbles:true}));
  if(errors.length) fails.push('insert errors');
  const clone=d.body.cloneNode(true);
  clone.querySelectorAll('script,style').forEach(e=>e.remove());
  const visible=clone.textContent.match(/[぀-ヿ㐀-鿿][^\n]{0,30}/g)||[];
  const allowed=visible.filter(s=>!s.startsWith('日本語'));
  if(allowed.length) fails.push('Japanese visible: '+JSON.stringify(allowed.slice(0,5)));
  console.log(fails.length?('EN FAILED: '+fails.join(' | ')):'EN ALL PASS');
  process.exit(fails.length?1:0);
},700);

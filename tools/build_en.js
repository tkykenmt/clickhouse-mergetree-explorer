// 英語版ビルド: ja-en.json の対訳を長い順に適用して en/index.html を生成
const fs=require('fs'),path=require('path');
const root=path.join(__dirname,'..');
let h=fs.readFileSync(path.join(root,'index.html'),'utf8');
const map=JSON.parse(fs.readFileSync(path.join(__dirname,'ja-en.json'),'utf8'));
const keys=Object.keys(map).sort((a,b)=>b.length-a.length);
keys.forEach(k=>{ if(map[k]!=null) h=h.split(k).join(map[k]); });
h=h.replace('<html lang="ja">','<html lang="en">');
h=h.replace('href="en/" title="English version"','href="../" title="日本語版"');
h=h.replace('href="../" title="日本語版" style="text-decoration:none;display:inline-flex;align-items:center;justify-content:center"',
            'href="../" title="日本語版" style="text-decoration:none;display:inline-flex;align-items:center;justify-content:center;width:auto;padding:0 12px;white-space:nowrap;border-radius:99px"');
h=h.replace('>EN</a>','>日本語</a>');
h=h.split(' ・ ').join(' · ');
// コメントを除いた残存日本語の検査
let t=h.replace(/\/\*[\s\S]*?\*\//g,'').replace(/(^|[^:])\/\/[^\n]*/g,'$1').replace(/<!--[\s\S]*?-->/g,'');
const JA=/[぀-ヿ㐀-鿿]/;
const bad=[];
t.split('\n').forEach((l,i)=>{ if(JA.test(l)) bad.push((i+1)+': '+l.trim().slice(0,100)); });
// 「日本語」リンクラベルは意図的な残存
const real=bad.filter(l=>!l.includes('>日本語</a>')&&!l.includes('id="langBtn"'));
if(real.length){ console.error('未翻訳の日本語が残っています:'); real.slice(0,30).forEach(l=>console.error(' '+l)); process.exit(1); }
fs.mkdirSync(path.join(root,'en'),{recursive:true});
fs.writeFileSync(path.join(root,'en/index.html'),h);
console.log('en/index.html built:', h.length, 'bytes');

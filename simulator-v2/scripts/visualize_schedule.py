#!/usr/bin/env python3
"""Build a self-contained interactive HTML viewer for a GPU plan and schedule."""
import argparse
import html
import json
from pathlib import Path

PAGE = r'''<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>GPU schedule explorer</title>
<style>
:root{font-family:Inter,system-ui,-apple-system,sans-serif;color:#e7edf8;background:#0e1422}*{box-sizing:border-box}
body{margin:0;padding:30px 20px}main{max-width:1100px;margin:auto}h1{font-size:27px;margin:0 0 7px}h2{font-size:17px;margin:0 0 14px}.muted{color:#99a9c1}.lead{margin:0 0 24px}
.panel{background:#172235;border:1px solid #2a3951;border-radius:16px;padding:20px;margin:15px 0}
.legend{display:flex;gap:17px;flex-wrap:wrap;font-size:13px;margin-bottom:12px}.dot{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:6px}
svg{width:100%;height:auto;display:block;overflow:visible}input[type=range]{width:100%;accent-color:#fb7185;cursor:pointer}
.timebar{display:flex;justify-content:space-between;align-items:center;gap:10px}.time{font-size:25px;font-weight:750;font-variant-numeric:tabular-nums}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}.chunk{border:1px solid #385273;background:#203149;border-radius:12px;padding:15px;min-height:110px}.chunk.free{border-style:dashed;background:#141e2d;color:#9aaac0}.chunk strong{display:block;font-size:17px;margin-bottom:6px}.small{font-size:12px;color:#aabbd2}.tags{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}.tag{border-radius:7px;padding:6px 10px;background:#253850;font-size:13px}.tag.compute{background:#1d3e7a}.tag.h2d{background:#155c50}.tag.d2h{background:#70461d}.metrics{display:flex;gap:18px;flex-wrap:wrap;margin:12px 0}.metric{padding:12px 15px;background:#202f46;border-radius:10px}.metric b{display:block;font-size:18px;margin-top:3px}
.details{font-size:13px;color:#d3ddeb;line-height:1.6}code{color:#c8dcff} .hint{font-size:12px;color:#9fb0c9;margin-top:10px}
</style></head><body><main>
<h1>GPU schedule explorer</h1><p class="muted lead">План операций и реальное расписание — от загрузки данных до освобождения памяти</p>
<div class="metrics" id="metrics"></div>
<section class="panel"><h2>1. Временная диаграмма</h2>
<div class="legend"><span><i class="dot" style="background:#5b92f7"></i>Compute</span><span><i class="dot" style="background:#27b99b"></i>H2D</span><span><i class="dot" style="background:#f0a64b"></i>D2H</span><span><i class="dot" style="background:#fb7185"></i>Текущее время</span></div>
<svg id="timeline" viewBox="0 0 1000 240" role="img" aria-label="Диаграмма исполнения операций"></svg>
<div class="timebar"><strong>Момент времени</strong><span class="time" id="timeLabel"></span></div>
<input id="timeSlider" type="range" min="0" max="42" step="0.25" value="12"><div class="hint">Двигай ползунок или нажимай на диаграмму. Полоски показывают занятость линий, а не зависимости.</div>
</section>
<section class="panel"><h2>2. Что происходит сейчас</h2><div class="tags" id="active"></div><div class="details" id="events"></div></section>
<section class="panel"><h2>3. Память GPU</h2><p class="muted" id="usage"></p><div class="grid" id="chunks"></div><p class="hint">Чанк резервирует память с начала H2D/ALLOC до DROP. Версия после Compute обновляется в конце вычисления; CPU-копия обновляется после D2H.</p></section>
<section class="panel"><h2>4. Зависимости выбранной операции</h2><p class="muted">Нажми на прямоугольник операции на диаграмме.</p><div class="details" id="selected">Пока ничего не выбрано.</div></section>
</main><script id="payload" type="application/json">__DATA__</script><script>
'use strict';
const data=JSON.parse(document.getElementById('payload').textContent), plan=data.plan, schedule=data.schedule;
const ops=new Map(plan.operations.map(o=>[o.id,o]));
const events=schedule.operations.map(e=>({...e,op:ops.get(e.id)}));
const chunks=new Map(plan.chunks.map(c=>[c.id,c]));
const maxT=Math.max(1,...events.map(e=>e.end_us));
const cap=plan.capacity_bytes, slot=plan.chunk_slot_bytes || Math.max(...plan.chunks.map(c=>c.reserved_bytes));
const fmtBytes=n=>n>=1048576?`${(n/1048576).toFixed(2)} МиБ`:`${(n/1024).toFixed(0)} КиБ`;
const laneNames=['Compute','H2D','D2H'];
const colors={Compute:'#5b92f7',H2D:'#27b99b',D2H:'#f0a64b'};
const slider=document.getElementById('timeSlider');slider.max=maxT;slider.step=0.25;
const svg=document.getElementById('timeline');
const ns='http://www.w3.org/2000/svg';
function el(name,attrs={},content=''){const e=document.createElementNS(ns,name);Object.entries(attrs).forEach(([k,v])=>e.setAttribute(k,v));if(content)e.textContent=content;return e;}
const left=112,right=20,plotW=1000-left-right,scale=t=>left+(t/maxT)*plotW;
for(let i=0;i<=8;i++){const t=maxT*i/8,x=scale(t);svg.append(el('line',{x1:x,y1:20,x2:x,y2:191,stroke:'#33425a','stroke-dasharray':'4 5'}));svg.append(el('text',{x,y:213,fill:'#a9b9d0','font-size':13,'text-anchor':'middle'},Number(t.toFixed(1))+' мкс'));}
for(let i=0;i<3;i++){const y=43+i*52;svg.append(el('text',{x:7,y:y+20,fill:'#e7edf8','font-size':16},laneNames[i]));svg.append(el('line',{x1:left,y1:y+30,x2:980,y2:y+30,stroke:'#2d3c52'}));}
let chosen=null;
for(const e of events){if(!laneNames.includes(e.lane))continue;const y=43+laneNames.indexOf(e.lane)*52;const x=scale(e.start_us),w=Math.max(2,scale(e.end_us)-x);const rect=el('rect',{x,y,width:w,height:31,rx:5,fill:colors[e.lane],cursor:'pointer'});rect.addEventListener('click',()=>{chosen=e.id;renderSelected();});rect.append(el('title',{},`${e.id}: ${e.start_us}–${e.end_us} мкс`));svg.append(rect);if(w>45)svg.append(el('text',{x:x+w/2,y:y+20,fill:'#091322','font-size':Math.min(14,Math.max(10,w/8)),'font-weight':700,'text-anchor':'middle','pointer-events':'none'},e.op.task||e.id));}
const cursor=el('line',{y1:19,y2:194,stroke:'#fb7185','stroke-width':3});svg.append(cursor);
svg.addEventListener('click',e=>{if(e.target.tagName.toLowerCase()==='rect')return;const r=svg.getBoundingClientRect();const x=(e.clientX-r.left)/r.width*1000;slider.value=Math.max(0,Math.min(maxT,(x-left)/plotW*maxT));render();});
function simulate(t){
 const cpu={...(plan.initial_cpu_versions||{})},gpu=new Map(),active=new Map(),history=[];
 let peak=0;
 const timeline=[];
 for(const e of events){const type=e.op.type;if(type==='DROP')timeline.push({t:e.start_us,phase:1,e,kind:'instant'});else if(type==='ALLOC')timeline.push({t:e.start_us,phase:2,e,kind:'instant'});else{timeline.push({t:e.start_us,phase:type==='Compute'?4:3,e,kind:'start'});timeline.push({t:e.end_us,phase:0,e,kind:'end'});}}
 timeline.sort((a,b)=>a.t-b.t||a.phase-b.phase||a.e.id.localeCompare(b.e.id));
 function log(s){history.push(s);}
 for(const a of timeline){if(a.t>t)break;const {e,kind}=a,op=e.op,c=op.chunk;
  if(kind==='instant'){
   if(op.type==='ALLOC'){gpu.set(c,{version:null,status:'зарезервирован'});log(`${e.id}: выделена память под ${c}`);}
   else{gpu.delete(c);log(`${e.id}: ${c} удалён из GPU`);}
  }else if(kind==='start'){
   active.set(e.id,e);
   if(op.type==='H2D'){gpu.set(c,{version:null,status:'загрузка H2D'});log(`${e.id}: началась загрузка ${c}@${op.version}`);}
   if(op.type==='D2H')log(`${e.id}: началось сохранение ${c}@${op.version} на CPU`);
   if(op.type==='Compute')log(`${e.id}: начались вычисления`);
  }else{
   active.delete(e.id);
   if(op.type==='H2D'){if(gpu.has(c))gpu.set(c,{version:op.version,status:'готов'});log(`${e.id}: загружен ${c}@${op.version}`);}
   if(op.type==='D2H'){cpu[c]=op.version;log(`${e.id}: CPU получил ${c}@${op.version}`);}
   if(op.type==='Compute'){for(const w of op.writes||[]){if(gpu.has(w.chunk))gpu.set(w.chunk,{version:w.version,status:'готов'});}log(`${e.id}: вычисления завершены`);}
  }
  peak=Math.max(peak,[...gpu.keys()].reduce((n,k)=>n+chunks.get(k).reserved_bytes,0));
 }
 return {gpu,cpu,active:[...active.values()],history,peak};
}
function renderSelected(){const target=document.getElementById('selected');if(!chosen){target.textContent='Пока ничего не выбрано.';return;}const op=ops.get(chosen),e=events.find(x=>x.id===chosen);target.replaceChildren();const p=document.createElement('p');p.textContent=`${chosen} · ${op.type} · ${e.start_us}–${e.end_us} мкс`;target.append(p);const d=document.createElement('p');d.textContent=`Зависит от: ${(op.depends_on||[]).join(', ')||'нет'} · Читает: ${(op.reads||[]).map(x=>x.chunk+'@'+x.version).join(', ')||'—'} · Записывает: ${(op.writes||[]).map(x=>x.chunk+'@'+x.version).join(', ')||'—'}`;target.append(d);}
function render(){const t=Number(slider.value);document.getElementById('timeLabel').textContent=t.toFixed(t%1?2:0)+' мкс';cursor.setAttribute('x1',scale(t));cursor.setAttribute('x2',scale(t));const s=simulate(t),used=[...s.gpu.keys()].reduce((n,k)=>n+chunks.get(k).reserved_bytes,0);
 const active=document.getElementById('active');active.replaceChildren();if(!s.active.length)active.textContent='Сейчас нет выполняющихся операций';for(const e of s.active){const tag=document.createElement('span');tag.className='tag '+e.lane.toLowerCase();tag.textContent=`${e.lane}: ${e.id}`;active.append(tag);}
 document.getElementById('events').textContent='Последнее событие: '+(s.history.at(-1)||'—');
 document.getElementById('usage').textContent=`Занято ${fmtBytes(used)} из ${fmtBytes(cap)} (${(100*used/cap).toFixed(0)}%) · CPU: ${Object.entries(s.cpu).map(([c,v])=>c+'@'+v).join(', ')||'нет сохранённых версий'}`;
 const area=document.getElementById('chunks');area.replaceChildren();for(const [id,state] of s.gpu){const node=document.createElement('div');node.className='chunk';const title=document.createElement('strong');title.textContent=id;const info=document.createElement('div');info.textContent=`Версия: ${state.version===null?'ещё не готова':state.version}`;const status=document.createElement('div');status.className='small';status.textContent=`${state.status} · ${fmtBytes(chunks.get(id).reserved_bytes)}`;node.append(title,info,status);area.append(node);}
 const free=Math.max(0,cap-used);if(free>0){const node=document.createElement('div');node.className='chunk free';const title=document.createElement('strong');title.textContent='Свободная память';const info=document.createElement('div');info.textContent=fmtBytes(free);node.append(title,info);area.append(node);}
}
document.getElementById('metrics').innerHTML=`<div class="metric">Операций<b>${events.length}</b></div><div class="metric">Makespan<b>${maxT} мкс</b></div><div class="metric">Ёмкость GPU<b>${fmtBytes(cap)}</b></div><div class="metric">Чанков<b>${chunks.size}</b></div>`;
slider.addEventListener('input',render);render();
</script></body></html>'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parents[1]
    parser.add_argument('--plan', type=Path, default=root / 'docs/scheduling/example-plan.json')
    parser.add_argument('--schedule', type=Path, default=root / 'docs/scheduling/example-schedule.json')
    parser.add_argument('--output', type=Path, default=root / 'docs/scheduling/schedule-viewer.html')
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text(encoding='utf-8'))
    schedule = json.loads(args.schedule.read_text(encoding='utf-8'))
    payload = json.dumps({'plan': plan, 'schedule': schedule}, ensure_ascii=False).replace('<', '\\u003c')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(PAGE.replace('__DATA__', payload), encoding='utf-8')
    print(f'Created: {args.output}')

if __name__ == '__main__':
    main()

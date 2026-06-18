"""
Data Suite — Execute python 3.13 distroless
Step #9 do DAG: roda apos os 7 nodes DASH_* concluirem.

Credentials to Include:
  - adqtc (GCP/BQ)
  - TOKEN_GITHUB_DASH_UTILIZACAO_LIMITE (Token)
"""
import json, base64, urllib.request
from decimal import Decimal
from datetime import date

# ── Credenciais via connections (sem hardcode) ────────────────────────────────
GITHUB_TOKEN = connections['TOKEN_GITHUB_DASH_UTILIZACAO_LIMITE'].get_secret()
client       = connections['adqtc'].bigquery_client

GITHUB_OWNER = "oliveiraeric-mlb"
GITHUB_REPO  = "dash-utilizacao-limite"
BQ_DATASET   = "meli-bi-data.SBOX_CREDITSTC"

# ── Helpers BQ ───────────────────────────────────────────────────────────────
def _q(sql):
    def py(v): return float(v) if isinstance(v, Decimal) else v
    return [{k: py(v) for k, v in row.items()} for row in client.query(sql).result()]

# ── Helpers GitHub API ────────────────────────────────────────────────────────
def _gh_get(path):
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    })
    with urllib.request.urlopen(req) as r:
        d = json.loads(r.read())
    sha = d["sha"]
    # Arquivos > 1MB: GitHub nao retorna content via Contents API, usa download_url
    if d.get("content"):
        content = base64.b64decode(d["content"].replace("\n","")).decode("utf-8")
    else:
        dl_req = urllib.request.Request(d["download_url"], headers={
            "Authorization": f"token {GITHUB_TOKEN}"
        })
        with urllib.request.urlopen(dl_req) as r:
            content = r.read().decode("utf-8")
    return content, sha

def _gh_put(path, content_str, sha):
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{path}"
    body = json.dumps({
        "message": f"chore: rebuild {date.today()}",
        "content": base64.b64encode(content_str.encode("utf-8")).decode("utf-8"),
        "sha": sha,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }, method="PUT")
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

# ── Builders de dados ─────────────────────────────────────────────────────────
def partial_safras():
    t = date.today()
    p = {t.strftime('%Y-%m')}
    if t.day < 15:
        m = t.month - 1 or 12
        y = t.year if t.month > 1 else t.year - 1
        p.add(f"{y}-{m:02d}")
    return p

def build_pol(rows):
    partial = partial_safras()
    return [{"s": r["seg"], "p": r["pol"], "sf": r["safra"], "cl": r["cl"],
             "d0": r["d0"], "d3": r["d3"], "d7": r["d7"], "d14": r["d14"], "d30": r["d30"],
             "u0": r["u0"], "u7": r["u7"], "u30": r["u30"],
             "pix0": r["pix0"], "pix30": r["pix30"],
             "o6": r["o6"], "o15": r["o15"], "over30": r["over30"],
             "parcial": r["safra"] in partial} for r in rows]

def build_cluster(rows):
    cd = {}
    for r in rows:
        k = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        cd.setdefault(k, {}).setdefault(r["dim_nome"], []).append(
            {"val": r["dim_val"], "cl": r["cl"], "d0": r["d0"], "d7": r["d7"],
             "d30": r["d30"], "u0": r["u0"], "u30": r["u30"],
             "over6": r["over6"], "over15": r["over15"], "over30": r["over30"]})
    return cd

def build_cross(rows):
    xd = {}
    for r in rows:
        k  = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        p  = f"{r['dim1_nome']}||{r['dim2_nome']}"
        rp = f"{r['dim2_nome']}||{r['dim1_nome']}"
        e  = [r["dim1_val"], r["dim2_val"], r["cl"],
              r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
              r["over6"], r["over15"], r["over30"]]
        xd.setdefault(k, {}).setdefault(p,  []).append(e)
        xd.setdefault(k, {}).setdefault(rp, []).append([r["dim2_val"], r["dim1_val"]] + e[2:])
    return xd

def build_canal_cluster(rows):
    cc = {}
    for r in rows:
        k = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        cc.setdefault(k, []).append(
            {"val": r["canal"], "cl": r["cl"], "d0": r["d0"], "d7": r["d7"],
             "d30": r["d30"], "u0": r["u0"], "u30": r["u30"],
             "over6": r["over6"], "over15": r["over15"], "over30": r["over30"]})
    return cc

def build_canal_dim(rows):
    cd2 = {}
    for r in rows:
        k = f"{r['canal']}||{r['safra']}||{r['segmento']}"
        cd2.setdefault(k, {}).setdefault(r["dim_nome"], []).append(
            {"val": r["dim_val"], "cl": r["cl"], "d0": r["d0"], "d7": r["d7"],
             "d30": r["d30"], "u0": r["u0"], "u30": r["u30"],
             "over6": r["over6"], "over15": r["over15"], "over30": r["over30"]})
    return cd2

def build_canal_cross(rows):
    cx = {}
    for r in rows:
        k  = f"{r['canal']}||{r['safra']}||{r['segmento']}"
        p  = f"{r['dim1']}||{r['dim2']}"
        rp = f"{r['dim2']}||{r['dim1']}"
        e  = [r["val1"], r["val2"], r["cl"],
              r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
              r["over6"], r["over15"], r["over30"]]
        cx.setdefault(k, {}).setdefault(p,  []).append(e)
        cx.setdefault(k, {}).setdefault(rp, []).append([r["val2"], r["val1"]] + e[2:])
    return cx

def build_macro(rows):
    m = {}
    for r in rows:
        m.setdefault(r["segmento"], {})[r["safra"]] = {
            "cl": r["cl"], "d0": r["d0"], "d3": r["d3"], "d7": r["d7"],
            "d14": r["d14"], "d30": r["d30"],
            "u80d0": r["u80d0"], "u80d7": r["u80d7"], "u80d30": r["u80d30"],
            "over30": r["over30"]}
    return m

# ── Main ──────────────────────────────────────────────────────────────────────
ds = BQ_DATASET
print("Consultando BQ...")
pol_data       = build_pol(_q(f"SELECT * FROM `{ds}.DASH_POL_DATA` ORDER BY safra,seg,pol"))
cluster_data   = build_cluster(_q(f"SELECT * FROM `{ds}.DASH_CLUSTER_DATA`"))
cross_data     = build_cross(_q(f"SELECT * FROM `{ds}.DASH_CROSS_DATA`"))
canal_cluster  = build_canal_cluster(_q(f"SELECT * FROM `{ds}.DASH_CANAL_CLUSTER`"))
canal_dim_data = build_canal_dim(_q(f"SELECT * FROM `{ds}.DASH_CANAL_DIM_DATA`"))
canal_cross    = build_canal_cross(_q(f"SELECT * FROM `{ds}.DASH_CANAL_CROSS_DATA`"))
macro          = build_macro(_q(f"SELECT * FROM `{ds}.DASH_MACRO` ORDER BY safra,segmento"))
print(f"pol={len(pol_data)} cluster={len(cluster_data)} cross={len(cross_data)}")

cc_js = (
    f"const CANAL_CLUSTER={json.dumps(canal_cluster, ensure_ascii=False)};\n"
    "Object.keys(CANAL_CLUSTER).forEach(k=>{"
    "if(!CLUSTER_DATA[k])CLUSTER_DATA[k]={};"
    "CLUSTER_DATA[k]['FLAG_CANAL_AQUISICAO_SIMP']=CANAL_CLUSTER[k];});\n"
)
data_block = (
    f"const MACRO={json.dumps(macro, ensure_ascii=False)};\n"
    f"const POL_DATA={json.dumps(pol_data, ensure_ascii=False)};\n"
    f"const ALL_SAFRAS=[...new Set(POL_DATA.map(d=>d.sf))].sort();\n\n"
    f"const CROSS_DATA={json.dumps(cross_data, ensure_ascii=False)};\n"
    f"const CLUSTER_DATA={json.dumps(cluster_data, ensure_ascii=False)};\n"
    f"{cc_js}\n"
    f"const CANAL_DIM_DATA={json.dumps(canal_dim_data, ensure_ascii=False)};\n"
    f"const CANAL_CROSS_DATA={json.dumps(canal_cross, ensure_ascii=False)};\n"
)

# ── Funções JS (renderKPIs, renderTabela, SF_LABELS etc.) embutidas ──────────
FUNCTIONS_BLOCK = 'const SF_LABELS = {\'all\':\'Todas as safras\',\'2025-04\':\'Abr/25\',\'2025-05\':\'Mai/25\',\'2025-06\':\'Jun/25\',\'2025-07\':\'Jul/25\',\'2025-08\':\'Ago/25\',\'2025-09\':\'Set/25\',\'2025-10\':\'Out/25\',\'2025-11\':\'Nov/25\',\'2025-12\':\'Dez/25\',\'2026-01\':\'Jan/26\',\'2026-02\':\'Fev/26\',\'2026-03\':\'Mar/26\',\'2026-04\':\'Abr/26*\',\'2026-05\':\'Mai/26*\',\'2026-06\':\'Jun/26*\'};\n// Gera label automaticamente para qualquer safra YYYY-MM não mapeada\nconst _MESES=[\'Jan\',\'Fev\',\'Mar\',\'Abr\',\'Mai\',\'Jun\',\'Jul\',\'Ago\',\'Set\',\'Out\',\'Nov\',\'Dez\'];\nfunction sfLabel(sf){ if(SF_LABELS[sf]) return SF_LABELS[sf];\n  const p=sf.match(/^(\\d{4})-(\\d{2})$/); return p?_MESES[+p[2]-1]+\'/\'+p[1].slice(2)+\'*\':sf; }\n\nconst COMP_SAFRAS = [\'2025-10\',\'2025-11\',\'2025-12\',\'2026-01\',\'2026-02\',\'2026-03\'];\nconst COMP_COLORS = [\'rgba(99,102,241,.85)\',\'rgba(59,130,246,.85)\',\'rgba(16,185,129,.85)\',\'rgba(245,158,11,.85)\',\'rgba(239,68,68,.85)\',\'rgba(168,85,247,.85)\'];\nconst COLORS = [\'#6366f1\',\'#3b82f6\',\'#10b981\',\'#f59e0b\',\'#ef4444\',\'#a855f7\',\'#ec4899\',\'#14b8a6\'];\nconst LC = \'#9ca3af\';\nconst GC = \'#1f2937\';\nconst baseOpts = (ylabel) => ({\n  responsive:true, maintainAspectRatio:true,\n  plugins:{legend:{labels:{color:LC,font:{size:10}}},tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y!=null?c.parsed.y.toFixed(1)+\'%\':\'—\'}`}}},\n  scales:{\n    x:{ticks:{color:LC,font:{size:10}},grid:{color:\'#1f2937\'}},\n    y:{ticks:{color:LC,font:{size:10},callback:v=>v+\'%\'},grid:{color:\'#1f2937\'},title:{display:true,text:ylabel,color:\'#6b7280\',font:{size:9}}}\n  }\n});\n\nlet segState=\'all\', sortCol=\'sf\', sortAsc=true;\nlet chartCM=null, chartCU=null, chartCP=null, chartSM=null, chartSF=null;\n\nfunction fmtN(n){\n  if(n==null) return \'—\';\n  if(n>=1e6) return (n/1e6).toFixed(1)+\'M\';\n  if(n>=1e3) return (n/1e3).toFixed(1)+\'k\';\n  return String(n);\n}\nfunction pill(v,thr1=30,thr2=20){\n  if(v==null) return \'<span style="color:#4b5563">—</span>\';\n  if(v>=thr1) return `<span class="pill p-hi">${v}%</span>`;\n  if(v>=thr2) return `<span class="pill p-md">${v}%</span>`;\n  return `<span class="pill p-lo">${v}%</span>`;\n}\nfunction miniBar(v,max=100,color=\'#3b82f6\'){\n  const w=Math.min(v||0,max)/max*80;\n  return `<div class="mini-bar"><div class="mini-fill" style="width:${w}px;background:${color}"></div></div>`;\n}\nfunction mkChart(id,type,data,opts){\n  const el=document.getElementById(id); if(!el) return null;\n  const ex=Chart.getChart(el); if(ex) ex.destroy();\n  return new Chart(el.getContext(\'2d\'),{type,data,options:opts});\n}\nfunction mobCell(val,sf,metric){\n  if(val!=null){\n    const cls=val>=5?\'p-hi\':val>=3.5?\'p-md\':\'p-lo\';\n    return `<span class="pill ${cls}">${val.toFixed?val.toFixed(1):val}%</span>`;\n  }\n  const pending={\n    over6:  {\'2026-04\':\'Jun/26\',\'2026-05\':\'Jul/26\'},\n    over15: {\'2026-03\':\'Jun/26\',\'2026-04\':\'Jul/26\',\'2026-05\':\'Ago/26\'},\n    over30: {\'2026-02\':\'Jun/26\',\'2026-03\':\'Jul/26\',\'2026-04\':\'Ago/26\',\'2026-05\':\'Set/26\'},\n  };\n  const when=(pending[metric]||{})[sf];\n  return when?`<span style="color:#4b5563;font-size:11px">⏳ ${when}</span>`:`<span style="color:#4b5563;font-size:11px">—</span>`;\n}\nfunction over30Cell(v,sf){ return mobCell(v,sf,\'over30\'); }\nfunction safraColor(seg,idx,total){\n  const isPartial=idx===total-1;\n  if(isPartial) return \'rgba(251,191,36,0.85)\';\n  const alpha=(0.15+(idx/(Math.max(total-2,1)))*0.85).toFixed(2);\n  return seg===\'micro_tc\'?`rgba(239,68,68,${alpha})`:`rgba(59,130,246,${alpha})`;\n}\nfunction canalCell(seg,pol,sf){\n  const arr=(CLUSTER_DATA[`${seg}||${pol}||${sf}`]||{})[\'FLAG_CANAL_AQUISICAO_SIMP\'];\n  if(!arr||!arr.length) return \'<span style="color:#6b7280;font-size:10px">—</span>\';\n  const total=arr.reduce((s,r)=>s+(r.cl||0),0);\n  if(!total) return \'<span style="color:#6b7280;font-size:10px">—</span>\';\n  const colors={\'ML\':\'#15803d\',\'MP\':\'#1d4ed8\',\'EA - MP\':\'#b45309\',\'N/D\':\'#9ca3af\'};\n  return arr.filter(r=>r.val!==\'N/D\'&&r.cl>0).map(r=>{\n    const pct=Math.round(r.cl/total*100);\n    const c=colors[r.val]||\'#6b7280\';\n    const lbl=r.val===\'EA - MP\'?\'EA\':r.val;\n    return `<span style="background:${c};color:#fff;border-radius:3px;padding:1px 5px;font-size:9px;font-weight:700;display:inline-block;margin:1px">${lbl} ${pct}%</span>`;\n  }).join(\'\')||\'<span style="color:#6b7280;font-size:10px">—</span>\';\n}\n\n// ─── KPIs ─────────────────────────────────────────────────────────────────────\nfunction renderKPIs(){\n  const segs=segState===\'all\'?[\'micro_tc\',\'tc_full\']:segState===\'micro_tc\'?[\'micro_tc\']:[\'tc_full\'];\n  const complete=ALL_SAFRAS.filter(sf=>sf!==\'2026-04\'&&sf!==\'2026-05\');\n  let totCl=0,sumD0=0,sumU80=0,sumO30=0,cntO30=0;\n  segs.forEach(s=>{\n    complete.forEach(sf=>{\n      const d=MACRO[s]?.[sf]; if(!d) return;\n      totCl+=d.cl; sumD0+=d.d0*d.cl; sumU80+=d.u80d0*d.cl;\n      if(d.over30!=null){sumO30+=d.over30*d.cl;cntO30+=d.cl;}\n    });\n  });\n  if(!totCl) return;\n  document.getElementById(\'kpi-cl\').textContent=fmtN(totCl);\n  document.getElementById(\'kpi-d0\').textContent=(sumD0/totCl).toFixed(1)+\'%\';\n  document.getElementById(\'kpi-u80\').textContent=(sumU80/totCl).toFixed(1)+\'%\';\n  document.getElementById(\'kpi-over30\').textContent=cntO30>0?(sumO30/cntO30).toFixed(2)+\'%\':\'—\';\n  const micro12=complete.reduce((a,sf)=>{const d=MACRO.micro_tc?.[sf];return d?a+d.cl:a;},0);\n  const full12=complete.reduce((a,sf)=>{const d=MACRO.tc_full?.[sf];return d?a+d.cl:a;},0);\n  const sub=document.getElementById(\'kpi-cl-sub\');\n  if(sub) sub.textContent=`micro: ${fmtN(micro12)} · full: ${fmtN(full12)}`;\n}\n\n// ─── CURVAS GLOBAIS ───────────────────────────────────────────────────────────\nfunction renderCurvasGlobais(){\n  const segs=segState===\'all\'?[\'micro_tc\',\'tc_full\']:segState===\'micro_tc\'?[\'micro_tc\']:[\'tc_full\'];\n  const labM=[\'D0\',\'D3\',\'D7\',\'D14\',\'D30\'], labU=[\'D0\',\'D7\',\'D30\'];\n  const dsM=[], dsU=[];\n  const N=ALL_SAFRAS.length;\n  segs.forEach(s=>{\n    ALL_SAFRAS.forEach((sf,i)=>{\n      const d=MACRO[s]?.[sf]; if(!d) return;\n      const isP=d.parcial||sf===\'2026-04\'||sf===\'2026-05\';\n      const col=safraColor(s,i,N);\n      const lbl=`${s===\'micro_tc\'?\'μ\':\'F\'} ${SF_LABELS[sf]||sf}`;\n      const thick=i>=N-3?2:1;\n      const dash=isP?[4,3]:(i<N-5?[2,2]:undefined);\n      const pr=i>=N-3?3:1;\n      dsM.push({label:lbl,data:[d.d0,d.d3,d.d7,d.d14,d.d30],borderColor:col,backgroundColor:\'transparent\',borderDash:dash,tension:.3,pointRadius:pr,borderWidth:thick,spanGaps:true});\n      dsU.push({label:lbl,data:[d.u80d0,d.u80d7,d.u80d30],borderColor:col,backgroundColor:\'transparent\',borderDash:dash,tension:.3,pointRadius:pr,borderWidth:thick});\n    });\n  });\n  const noLegend={plugins:{legend:{display:false}}};\n  if(chartCM){chartCM.destroy();chartCM=null;}\n  if(chartCU){chartCU.destroy();chartCU=null;}\n  chartCM=mkChart(\'chartCurvaMedia\',\'line\',{labels:labM,datasets:dsM},{...baseOpts(\'%\'),...noLegend,interaction:{mode:\'index\',intersect:false}});\n  chartCU=mkChart(\'chartCurvaUsuarios\',\'line\',{labels:labU,datasets:dsU},{...baseOpts(\'%\'),...noLegend,interaction:{mode:\'index\',intersect:false}});\n}\n\n// ─── TABELA ───────────────────────────────────────────────────────────────────\nfunction renderTabela(){\n  const sf=document.getElementById(\'safraFilter\').value;\n  const q=(document.getElementById(\'searchPol\').value||\'\').toLowerCase();\n  let rows=POL_DATA.filter(d=>{\n    if(segState!==\'all\'&&d.s!==segState) return false;\n    if(sf!==\'all\'&&d.sf!==sf) return false;\n    if(q&&!d.p.toLowerCase().includes(q)) return false;\n    return true;\n  });\n  rows.sort((a,b)=>{\n    const va=a[sortCol]??-Infinity, vb=b[sortCol]??-Infinity;\n    if(typeof va===\'string\') return sortAsc?va.localeCompare(vb):vb.localeCompare(va);\n    return sortAsc?va-vb:vb-va;\n  });\n  const tbody=document.getElementById(\'tabelaBody\');\n  tbody.innerHTML=rows.map(d=>{\n    const sc=d.s===\'micro_tc\'?\'#ef4444\':\'#3b82f6\';\n    const rowStyle=d.parcial?\'background:rgba(251,191,36,.04);\':\'\';\n    const sfCell=d.parcial\n      ?`<td style="color:#fbbf24;font-style:italic">${sfLabel(d.sf)||d.sf} <span style="font-size:10px;background:#451a03;color:#fbbf24;padding:1px 5px;border-radius:10px;font-style:normal">parcial</span></td>`\n      :`<td>${sfLabel(d.sf)||d.sf}</td>`;\n    return `<tr style="${rowStyle}">\n      <td><span style="color:${sc};font-weight:700;font-size:11px">${d.s}</span></td>\n      <td style="max-width:240px;white-space:normal;font-size:11px"><span class="pol-link" onclick="openDrilldown(\'${d.s}\',\'${d.p.replace(/\'/g,"\\\\\'")}\',\'${d.sf}\')">${d.p}</span></td>\n      ${sfCell}\n      <td>${fmtN(d.cl)}</td>\n      <td>${d.d0??\'—\'}% ${d.d0?miniBar(d.d0,120,sc):\'\'}</td>\n      <td>${d.d7??\'—\'}%</td>\n      <td>${d.d30??\'—\'}%</td>\n      <td style="color:#f59e0b;font-weight:600">${d.pix0!=null?d.pix0+\'%\':\'—\'}</td>\n      <td style="color:#f59e0b;font-weight:600">${d.pix30!=null?d.pix30+\'%\':\'—\'}</td>\n      <td>${pill(d.u0,30,15)}</td>\n      <td>${pill(d.u30,70,40)}</td>\n      <td>${mobCell(d.o6,  d.sf,\'over6\')}</td>\n      <td>${mobCell(d.o15, d.sf,\'over15\')}</td>\n      <td>${over30Cell(d.over30,d.sf)}</td>\n    </tr>`;\n  }).join(\'\');\n}\nfunction sortTable(col,thEl){\n  if(sortCol===col) sortAsc=!sortAsc; else{sortCol=col;sortAsc=false;}\n  document.querySelectorAll(\'#mainTable th\').forEach(th=>{\n    th.classList.remove(\'sorted\');\n    const base=th.dataset.col?th.textContent.replace(/ [↑↓]$/,\'\'):th.textContent;\n    th.textContent=base;\n  });\n  if(thEl){thEl.classList.add(\'sorted\');thEl.textContent=thEl.textContent.replace(/ [↑↓]$/,\'\')+(sortAsc?\' ↑\':\' ↓\');}\n  renderTabela();\n}\n\n// ─── CURVAS POR POLÍTICA ──────────────────────────────────────────────────────\nfunction populatePolSelect(){\n  const seg=document.getElementById(\'polSeg\').value;\n  const pols=[...new Set(POL_DATA.filter(d=>d.s===seg).map(d=>d.p))];\n  const sel=document.getElementById(\'polSelect\');\n  sel.innerHTML=pols.map(p=>`<option value="${p}">${p}</option>`).join(\'\');\n  [...sel.options].slice(0,4).forEach(o=>o.selected=true);\n  renderCurvaPol();\n}\nfunction renderCurvaPol(){\n  const seg=document.getElementById(\'polSeg\').value;\n  const sf=document.getElementById(\'polSafra\').value;\n  const sel=[...document.getElementById(\'polSelect\').selectedOptions].map(o=>o.value);\n  const labels=[\'D0\',\'D3\',\'D7\',\'D14\',\'D30\'];\n  const datasets=[];\n  sel.forEach((p,i)=>{\n    let r=POL_DATA.filter(d=>d.s===seg&&d.p===p&&d.d0!=null);\n    if(sf!==\'all\') r=r.filter(d=>d.sf===sf);\n    if(!r.length) return;\n    const totCl=r.reduce((a,d)=>a+(d.cl||0),0);\n    const avg=k=>r.reduce((a,d)=>a+(d[k]||0)*(d.cl||0),0)/totCl;\n    datasets.push({label:p.length>40?p.slice(0,40)+\'…\':p,data:[avg(\'d0\'),avg(\'d3\'),avg(\'d7\'),avg(\'d14\'),avg(\'d30\')],borderColor:COLORS[i%COLORS.length],backgroundColor:\'transparent\',tension:.3,pointRadius:5,borderWidth:2});\n  });\n  if(chartCP){chartCP.destroy();chartCP=null;}\n  chartCP=mkChart(\'chartCurvaPol\',\'line\',{labels,datasets},{...baseOpts(\'%\'),interaction:{mode:\'index\',intersect:false}});\n}\n\n// ─── COMPARATIVO SAFRAS ───────────────────────────────────────────────────────\nfunction renderSafraComp(){\n  const topMicro=[\'BAU-MTC SEGMENTOS PREFERENCIAIS\',\'BAU-MTC BAU\',\'Mar Aberto RTS\',\'Mar Aberto Async\',\'BAU-6A SELLERS\',\'BAU-5A SELLERS\'];\n  const topFull =[\'Mar Aberto RTS\',\'BAU-CARD TO CARD OPF\',\'BAU-BAU GRUPO 1y2 SIN CAMBIOS\',\'BAU-HEAVY USERS\',\'BAU-USUARIOS MP VIP\',\'TEST REACH-TEST NO ECOSISTEMATICOS\'];\n  function buildDs(pols,seg){\n    return COMP_SAFRAS.map((sf,i)=>({\n      label:SF_LABELS[sf]||sf,\n      data:pols.map(p=>{const r=POL_DATA.find(d=>d.s===seg&&d.p===p&&d.sf===sf);return r?r.u0:null;}),\n      backgroundColor:COMP_COLORS[i],borderColor:COMP_COLORS[i].replace(\'.85\',\'.95\'),borderWidth:1\n    }));\n  }\n  const sm=topMicro.map(p=>p.replace(\'BAU-MTC \',\'\').replace(\'BAU-\',\'\').replace(\' SELLERS\',\'S\'));\n  const sf=topFull.map(p=>p.replace(\'BAU-\',\'\').replace(\'TEST REACH-\',\'TR-\').replace(\' USUARIOS MP VIP\',\'VIP\'));\n  const barOpts=(y)=>({...baseOpts(y),plugins:{legend:{labels:{color:LC,font:{size:10}}}}});\n  if(chartSM){chartSM.destroy();chartSM=null;}\n  if(chartSF){chartSF.destroy();chartSF=null;}\n  chartSM=mkChart(\'chartSafraMicro\',\'bar\',{labels:sm,datasets:buildDs(topMicro,\'micro_tc\')},barOpts(\'% usr ≥80% D0\'));\n  chartSF=mkChart(\'chartSafraFull\',\'bar\',{labels:sf,datasets:buildDs(topFull,\'tc_full\')},barOpts(\'% usr ≥80% D0\'));\n}\nfunction setSegState(seg,btn){\n  segState=seg;\n  document.querySelectorAll(\'.seg-btn\').forEach(b=>b.className=\'seg-btn\');\n  if(seg===\'all\') btn.className=\'seg-btn act-all\';\n  else if(seg===\'micro_tc\') btn.className=\'seg-btn act-micro\';\n  else btn.className=\'seg-btn act-full\';\n  renderKPIs(); renderCurvasGlobais(); renderTabela();\n}\n'

print("Buscando docs/index.html do GitHub...")
current_html, current_sha = _gh_get("docs/index.html")

data_start  = current_html.find("const MACRO=")
if data_start == -1:
    data_start = current_html.find("const MACRO =")
drill_state = current_html.find("\n// ─── DRILL-DOWN STATE")

if data_start < 0 or drill_state < 0:
    raise RuntimeError(f"Marcadores nao encontrados: data_start={data_start}, drill_state={drill_state}")

new_html = current_html[:data_start] + data_block + FUNCTIONS_BLOCK + current_html[drill_state:]

print(f"HTML: {len(new_html)/1024/1024:.1f} MB — publicando...")
result = _gh_put("docs/index.html", new_html, current_sha)
print("Concluido! SHA:", result.get("content", {}).get("sha", "ok")[:12])

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
    return base64.b64decode(d["content"].replace("\n","")).decode("utf-8"), d["sha"]

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

print("Buscando template do GitHub...")
template, _    = _gh_get("static/index.html")
_, current_sha = _gh_get("docs/index.html")

fetch_start     = template.find("// -- DATA loaded dynamically")
if fetch_start == -1:
    fetch_start = template.find("// ── DATA loaded dynamically")
fn_start        = template.find("\nconst SF_LABELS", fetch_start)
drill_state     = template.find("\n// ─── DRILL-DOWN STATE")
functions_block = template[fn_start:drill_state] if fn_start > 0 else ""
new_html        = template[:fetch_start] + data_block + functions_block + template[drill_state:]

print(f"HTML: {len(new_html)/1024/1024:.1f} MB — publicando...")
result = _gh_put("docs/index.html", new_html, current_sha)
print("Concluido! SHA:", result.get("content", {}).get("sha", "ok")[:12])

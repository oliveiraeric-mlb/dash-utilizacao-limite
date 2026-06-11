"""
Rebuild dashboard HTML from BigQuery DASH_* tables.
Runs locally or via GitHub Actions.
Output: docs/index.html
"""
import json, os, re
from datetime import date
from decimal import Decimal
from google.cloud import bigquery

BQ_PROJECT = os.getenv("BQ_PROJECT", "ddme000725-g9rtvpqr28z-furyid")
BQ_DATASET = os.getenv("BQ_DATASET", "meli-bi-data.SBOX_CREDITSTC")
client = bigquery.Client(project=BQ_PROJECT)


def _q(sql):
    def py(v): return float(v) if isinstance(v, Decimal) else v
    return [{k: py(v) for k, v in row.items()} for row in client.query(sql).result()]


def _partial_safras():
    today = date.today()
    current = today.strftime('%Y-%m')
    # mark current month and previous if before 15th as partial
    partial = {current}
    if today.day < 15:
        m = today.month - 1 or 12
        y = today.year if today.month > 1 else today.year - 1
        partial.add(f"{y}-{m:02d}")
    return partial


def build_pol_data(rows):
    partial = _partial_safras()
    result = []
    for r in rows:
        result.append({
            "s": r["seg"], "p": r["pol"], "sf": r["safra"], "cl": r["cl"],
            "d0": r["d0"], "d3": r["d3"], "d7": r["d7"], "d14": r["d14"], "d30": r["d30"],
            "u0": r["u0"], "u7": r["u7"], "u30": r["u30"],
            "pix0": r["pix0"], "pix30": r["pix30"],
            "o6": r["o6"], "o15": r["o15"], "over30": r["over30"],
            "parcial": r["safra"] in partial,
        })
    return result


def build_cluster(rows):
    cd = {}
    for r in rows:
        key = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        cd.setdefault(key, {}).setdefault(r["dim_nome"], []).append({
            "val": r["dim_val"], "cl": r["cl"],
            "d0": r["d0"], "d7": r["d7"], "d30": r["d30"],
            "u0": r["u0"], "u30": r["u30"],
            "over6": r["over6"], "over15": r["over15"], "over30": r["over30"],
        })
    return cd


def build_cross(rows):
    xd = {}
    for r in rows:
        key   = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        pair  = f"{r['dim1_nome']}||{r['dim2_nome']}"
        rpair = f"{r['dim2_nome']}||{r['dim1_nome']}"
        e     = [r["dim1_val"], r["dim2_val"], r["cl"],
                 r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
                 r["over6"], r["over15"], r["over30"]]
        xd.setdefault(key, {}).setdefault(pair,  []).append(e)
        xd.setdefault(key, {}).setdefault(rpair, []).append([r["dim2_val"], r["dim1_val"]] + e[2:])
    return xd


def build_canal_cluster(rows):
    cc = {}
    for r in rows:
        key = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        cc.setdefault(key, []).append({
            "val": r["canal"], "cl": r["cl"],
            "d0": r["d0"], "d7": r["d7"], "d30": r["d30"],
            "u0": r["u0"], "u30": r["u30"],
            "over6": r["over6"], "over15": r["over15"], "over30": r["over30"],
        })
    return cc


def build_canal_dim(rows):
    cd2 = {}
    for r in rows:
        key = f"{r['canal']}||{r['safra']}||{r['segmento']}"
        cd2.setdefault(key, {}).setdefault(r["dim_nome"], []).append({
            "val": r["dim_val"], "cl": r["cl"],
            "d0": r["d0"], "d7": r["d7"], "d30": r["d30"],
            "u0": r["u0"], "u30": r["u30"],
            "over6": r["over6"], "over15": r["over15"], "over30": r["over30"],
        })
    return cd2


def build_canal_cross(rows):
    cx = {}
    for r in rows:
        key  = f"{r['canal']}||{r['safra']}||{r['segmento']}"
        pair = f"{r['dim1']}||{r['dim2']}"
        rp   = f"{r['dim2']}||{r['dim1']}"
        e    = [r["val1"], r["val2"], r["cl"],
                r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
                r["over6"], r["over15"], r["over30"]]
        cx.setdefault(key, {}).setdefault(pair, []).append(e)
        cx.setdefault(key, {}).setdefault(rp,   []).append([r["val2"], r["val1"]] + e[2:])
    return cx


def build_macro(rows):
    macro = {}
    for r in rows:
        macro.setdefault(r["segmento"], {})[r["safra"]] = {
            "cl": r["cl"],
            "d0": r["d0"], "d3": r["d3"], "d7": r["d7"],
            "d14": r["d14"], "d30": r["d30"],
            "u80d0": r["u80d0"], "u80d7": r["u80d7"], "u80d30": r["u80d30"],
            "over30": r["over30"],
        }
    return macro


def main():
    ds = BQ_DATASET
    print("Querying BQ...")
    pol_data        = build_pol_data(_q(f"SELECT * FROM `{ds}.DASH_POL_DATA` ORDER BY safra,seg,pol"))
    cluster_data    = build_cluster(_q(f"SELECT * FROM `{ds}.DASH_CLUSTER_DATA`"))
    cross_data      = build_cross(_q(f"SELECT * FROM `{ds}.DASH_CROSS_DATA`"))
    canal_cluster   = build_canal_cluster(_q(f"SELECT * FROM `{ds}.DASH_CANAL_CLUSTER`"))
    canal_dim_data  = build_canal_dim(_q(f"SELECT * FROM `{ds}.DASH_CANAL_DIM_DATA`"))
    canal_cross     = build_canal_cross(_q(f"SELECT * FROM `{ds}.DASH_CANAL_CROSS_DATA`"))
    macro           = build_macro(_q(f"SELECT * FROM `{ds}.DASH_MACRO` ORDER BY safra,segmento"))
    print(f"pol_data={len(pol_data)} cluster={len(cluster_data)} cross={len(cross_data)}")

    # Read template HTML (static/index.html with fetch bootstrap)
    template_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(template_path, encoding="utf-8") as f:
        html = f.read()

    # Inject data blocks as JS constants (fallback mode: embed data for offline use)
    cc_js = (f"const CANAL_CLUSTER = {json.dumps(canal_cluster, ensure_ascii=False)};\n"
             "Object.keys(CANAL_CLUSTER).forEach(k => {\n"
             "  if (!CLUSTER_DATA[k]) CLUSTER_DATA[k] = {};\n"
             "  CLUSTER_DATA[k]['FLAG_CANAL_AQUISICAO_SIMP'] = CANAL_CLUSTER[k];\n"
             "});\n")

    data_block = (
        f"const MACRO = {json.dumps(macro, ensure_ascii=False)};\n"
        f"// ─── POL_DATA ──────────────────────────────────────────────────────────────\n"
        f"const POL_DATA = {json.dumps(pol_data, ensure_ascii=False)};\n"
        f"const ALL_SAFRAS = [...new Set(POL_DATA.map(d=>d.sf))].sort();\n\n"
        f"const CROSS_DATA = {json.dumps(cross_data, ensure_ascii=False)};\n"
        f"const CLUSTER_DATA = {json.dumps(cluster_data, ensure_ascii=False)};\n"
        f"{cc_js}\n"
        f"const CANAL_DIM_DATA = {json.dumps(canal_dim_data, ensure_ascii=False)};\n"
        f"const CANAL_CROSS_DATA = {json.dumps(canal_cross, ensure_ascii=False)};\n"
    )

    # Replace the fetch() bootstrap with embedded data (static HTML, no server needed)
    fetch_start = html.find("// ── DATA loaded dynamically")
    drill_state = html.find("\n// ─── DRILL-DOWN STATE", fetch_start)
    if fetch_start > 0 and drill_state > fetch_start:
        html = html[:fetch_start] + data_block + html[drill_state:]

    os.makedirs("docs", exist_ok=True)
    out_path = os.path.join("docs", "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    size = os.path.getsize(out_path) / 1024 / 1024
    print(f"Done: {out_path} ({size:.1f} MB)")


if __name__ == "__main__":
    main()

import os
import json
from decimal import Decimal
from google.cloud import bigquery
from cachetools import TTLCache, cached
from threading import Lock


def _to_py(v):
    """Convert BQ Decimal/int64 to plain Python float/int."""
    if isinstance(v, Decimal):
        return float(v)
    return v

BQ_PROJECT = os.getenv("BQ_PROJECT", "ddme000725-g9rtvpqr28z-furyid")
BQ_DATASET = os.getenv("BQ_DATASET", "meli-bi-data.SBOX_CREDITSTC")
client = bigquery.Client(project=BQ_PROJECT)

_cache: TTLCache = TTLCache(maxsize=1, ttl=3600)  # 1h cache
_lock = Lock()


def _q(sql: str) -> list:
    return [{k: _to_py(v) for k, v in row.items()} for row in client.query(sql).result()]


def _build_pol_data(rows: list) -> list:
    partial_safras = {"2026-04", "2026-05"}
    result = []
    for r in rows:
        result.append({
            "s": r["seg"], "p": r["pol"], "sf": r["safra"],
            "cl": r["cl"],
            "d0": r["d0"], "d3": r["d3"], "d7": r["d7"],
            "d14": r["d14"], "d30": r["d30"],
            "u0": r["u0"], "u7": r["u7"], "u30": r["u30"],
            "pix0": r["pix0"], "pix30": r["pix30"],
            "o6": r["o6"], "o15": r["o15"], "over30": r["over30"],
            "parcial": r["safra"] in partial_safras,
        })
    return result


def _build_cluster_data(rows: list) -> dict:
    cd = {}
    for r in rows:
        key = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        dim = r["dim_nome"]
        cd.setdefault(key, {}).setdefault(dim, []).append({
            "val": r["dim_val"], "cl": r["cl"],
            "d0": r["d0"], "d7": r["d7"], "d30": r["d30"],
            "u0": r["u0"], "u30": r["u30"],
            "over6": r["over6"], "over15": r["over15"], "over30": r["over30"],
        })
    return cd


def _build_cross_data(rows: list) -> dict:
    xd = {}
    for r in rows:
        key = f"{r['segmento']}||{r['politica']}||{r['safra']}"
        pair  = f"{r['dim1_nome']}||{r['dim2_nome']}"
        rpair = f"{r['dim2_nome']}||{r['dim1_nome']}"
        entry = [r["dim1_val"], r["dim2_val"], r["cl"],
                 r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
                 r["over6"], r["over15"], r["over30"]]
        rentryv = [r["dim2_val"], r["dim1_val"]] + entry[2:]
        xd.setdefault(key, {}).setdefault(pair,  []).append(entry)
        xd.setdefault(key, {}).setdefault(rpair, []).append(rentryv)
    return xd


def _build_canal_cluster(rows: list) -> dict:
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


def _build_canal_dim(rows: list) -> dict:
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


def _build_canal_cross(rows: list) -> dict:
    cx = {}
    for r in rows:
        key  = f"{r['canal']}||{r['safra']}||{r['segmento']}"
        pair = f"{r['dim1']}||{r['dim2']}"
        rp   = f"{r['dim2']}||{r['dim1']}"
        e    = [r["val1"], r["val2"], r["cl"],
                r["d0"], r["d7"], r["d30"], r["u0"], r["u30"],
                r["over6"], r["over15"], r["over30"]]
        re_  = [r["val2"], r["val1"]] + e[2:]
        cx.setdefault(key, {}).setdefault(pair, []).append(e)
        cx.setdefault(key, {}).setdefault(rp,   []).append(re_)
    return cx


def _build_macro(rows: list) -> dict:
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


def get_dashboard_data() -> dict:
    with _lock:
        if "data" in _cache:
            return _cache["data"]

        ds = BQ_DATASET
        pol_rows     = _q(f"SELECT * FROM `{ds}.DASH_POL_DATA`     ORDER BY safra, seg, pol")
        cluster_rows = _q(f"SELECT * FROM `{ds}.DASH_CLUSTER_DATA` ORDER BY safra, segmento, politica, dim_nome, dim_val")
        cross_rows   = _q(f"SELECT * FROM `{ds}.DASH_CROSS_DATA`   ORDER BY safra, segmento, politica")
        cc_rows      = _q(f"SELECT * FROM `{ds}.DASH_CANAL_CLUSTER` ORDER BY safra, segmento, politica, canal")
        cd2_rows     = _q(f"SELECT * FROM `{ds}.DASH_CANAL_DIM_DATA` ORDER BY safra, segmento, canal, dim_nome, dim_val")
        cx_rows      = _q(f"SELECT * FROM `{ds}.DASH_CANAL_CROSS_DATA`")
        macro_rows   = _q(f"SELECT * FROM `{ds}.DASH_MACRO`        ORDER BY safra, segmento")

        payload = {
            "pol_data":         _build_pol_data(pol_rows),
            "cluster_data":     _build_cluster_data(cluster_rows),
            "cross_data":       _build_cross_data(cross_rows),
            "canal_cluster":    _build_canal_cluster(cc_rows),
            "canal_dim_data":   _build_canal_dim(cd2_rows),
            "canal_cross_data": _build_canal_cross(cx_rows),
            "macro":            _build_macro(macro_rows),
        }
        _cache["data"] = payload
        return payload

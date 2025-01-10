import polars as pl
import math
from datetime import datetime

def _calc_npv(cashflows, rate):
    """
    NPV clássico:
      NPV = sum( CF[t] / (1+rate)^t ), t=0..(n-1)
    Aqui assumimos 'cashflows' em ordem temporal (t=0,1,2...).
    """
    npv_value = 0.0
    for i, cf in enumerate(cashflows):
        npv_value += cf / ((1 + rate)**i)
    return npv_value

def _calc_irr(cashflows, guess=0.1, tol=1e-7, max_iter=100):
    """
    IRR = taxa que zera o NPV.
    Implementação simplificada via método de Newton ou secante.
    cashflows[0..n-1], assumindo t=0,1,2,... (um por período).
    """
    def npv_derivative(cf, r):
        d = 0.0
        for i, c in enumerate(cf):
            d -= (i * c) / ((1 + r)**(i + 1))
        return d

    def npv_val(cf, r):
        return _calc_npv(cf, r)

    r = guess
    for _ in range(max_iter):
        val = npv_val(cashflows, r)
        if abs(val) < tol:
            return r
        dval = npv_derivative(cashflows, r)
        if abs(dval) < 1e-12:
            r_up = r + 0.01
            val_up = npv_val(cashflows, r_up)
            r_new = r - val * (r_up - r) / (val_up - val + 1e-14)
        else:
            r_new = r - val/dval
        if abs(r_new - r) < tol:
            return r_new
        r = r_new
    return r

def _calc_xnpv(cashflows, dates, rate):
    """
    XNPV = sum( CF[i] / (1 + rate)^( (dates[i] - dates[0]) / 365 ) )
    datas devem estar ordenadas no mesmo índice que cashflows.
    """
    if not cashflows or not dates:
        return 0.0
    dt0 = dates[0]
    if isinstance(dt0, str):
        dt0 = datetime.fromisoformat(dt0)
    total = 0.0
    for i, cf in enumerate(cashflows):
        dti = dates[i]
        if isinstance(dti, str):
            dti = datetime.fromisoformat(dti)
        days = (dti - dt0).days
        total += cf / ((1 + rate)**(days/365.0))
    return total

def _calc_xirr(cashflows, dates, guess=0.1, tol=1e-7, max_iter=100):
    """
    XIRR = taxa que zera o XNPV(cashflows, dates, rate).
    Usamos um método iterativo (similar a IRR).
    """
    def xnpv_val(cf, ds, r):
        return _calc_xnpv(cf, ds, r)

    def xnpv_derivative(cf, ds, r):
        """
        Derivada aproximada via secante ou, se quiser, derivada analítica.
        Aqui, para simplificar, faço aproximação numérica.
        """
        h = 1e-6
        x1 = xnpv_val(cf, ds, r)
        x2 = xnpv_val(cf, ds, r + h)
        return (x2 - x1)/h

    r = guess
    for _ in range(max_iter):
        val = xnpv_val(cashflows, dates, r)
        if abs(val) < tol:
            return r
        dval = xnpv_derivative(cashflows, dates, r)
        if abs(dval) < 1e-12:
            r = r + 0.01
        else:
            r_new = r - val/dval
            if abs(r_new - r) < tol:
                return r_new
            r = r_new
    return r

def _calc_mirr(cashflows, finance_rate, reinvest_rate):
    """
    MIRR: Modified Internal Rate of Return.
    Precisamos separar os CF negativos e positivos, etc.
    Fórmula clássica:
       MIRR = ( (-FV(positive CF, reinvest_rate) / PV(negative CF, finance_rate)) ^(1/(n-1)) ) - 1
    """
    n = len(cashflows)
    if n < 2:
        return 0.0
    neg_parts = []
    pos_parts = []
    for i, cf in enumerate(cashflows):
        if cf < 0:
            neg_parts.append(cf / ((1 + finance_rate)**i))
        else:
            pos_parts.append(cf * ((1 + reinvest_rate)**(n-1 - i)))

    pv_neg = sum(neg_parts)
    fv_pos = sum(pos_parts)
    if pv_neg == 0 or fv_pos == 0:
        return 0.0
    mirr_val = ( -fv_pos / pv_neg ) ** (1/(n-1)) - 1
    return mirr_val

def _calc_xmirr(cashflows, dates, finance_rate, reinvest_rate):
    """
    XMIRR: MIRR com datas específicas. 
    Abordagem simplificada:
      1) Definir tempo 0 = data min
      2) separar CF<0 e CF>0, descontar / capitalizar para data final
      3) resolver eq do MIRR mas considerando (days/365.0) como potencia
    """
    if not cashflows or not dates:
        return 0.0
    dt0 = dates[0]
    if isinstance(dt0, str):
        dt0 = datetime.fromisoformat(dt0)

    dtF = dates[-1]
    if isinstance(dtF, str):
        dtF = datetime.fromisoformat(dtF)

    pv_neg = 0.0
    fv_pos = 0.0
    for i, cf in enumerate(cashflows):
        dti = dates[i]
        if isinstance(dti, str):
            dti = datetime.fromisoformat(dti)
        days_0 = (dti - dt0).days
        days_f = (dtF - dti).days
        if cf < 0:
            pv_neg += cf / ((1 + finance_rate) ** (days_0/365.0))
        else:
            fv_pos += cf * ((1 + reinvest_rate) ** (days_f/365.0))

    if pv_neg >= 0.0 or fv_pos <= 0.0:
        return 0.0

    total_years = (dtF - dt0).days / 365.0
    if total_years <= 0:
        return 0.0
    ratio = -fv_pos / pv_neg
    xmirr_val = ratio ** (1.0 / total_years) - 1.0
    return xmirr_val

def _agg_sum(vals):
    return float(sum(vals))

def _agg_count(vals):
    return float(len(vals))

def _agg_count_non_null(vals):
    return float(sum(x is not None for x in vals))

def _agg_count_null(vals):
    return float(sum(x is None for x in vals))

def _agg_min(vals):
    try:
        return float(min(v for v in vals if v is not None))
    except:
        return None

def _agg_max(vals):
    try:
        return float(max(v for v in vals if v is not None))
    except:
        return None

def _agg_first(vals):
    return vals[0] if len(vals) > 0 else None

def _agg_last(vals):
    return vals[-1] if len(vals) > 0 else None

def _agg_avg(vals):
    tmp = [v for v in vals if (v is not None)]
    if len(tmp) == 0:
        return None
    return float(sum(tmp)) / len(tmp)

def _agg_median(vals):
    tmp = sorted(v for v in vals if v is not None)
    n = len(tmp)
    if n == 0:
        return None
    mid = n // 2
    if n % 2 == 1:
        return float(tmp[mid])
    else:
        return float( (tmp[mid-1] + tmp[mid]) / 2 )

def _agg_stddev(vals, ignore_zeros=False):
    tmp = [v for v in vals if v is not None]
    if ignore_zeros:
        tmp = [x for x in tmp if x != 0]
    n = len(tmp)
    if n < 2:
        return 0.0
    mean_ = sum(tmp)/n
    var_ = sum((x - mean_)**2 for x in tmp)/(n-1)
    return math.sqrt(var_)

def _agg_variance(vals, ignore_zeros=False):
    tmp = [v for v in vals if v is not None]
    if ignore_zeros:
        tmp = [x for x in tmp if x != 0]
    n = len(tmp)
    if n < 2:
        return 0.0
    mean_ = sum(tmp)/n
    var_ = sum((x - mean_)**2 for x in tmp)/(n-1)
    return var_

def _agg_product(vals):
    prod = 1.0
    for v in vals:
        if v is not None:
            prod *= v
    return prod

def _agg_count_distinct(vals):
    return float(len(set(vals)))

def _agg_count_distinct_non_null(vals):
    return float(len({x for x in vals if x is not None}))

def _agg_mode(vals):
    """
    Retorna um valor. Se houver empate, retorna um deles (por ex. o menor).
    """
    from collections import Counter
    c = Counter(vals)
    freq = c.most_common()
    if not freq:
        return None
    max_count = freq[0][1]
    empates = [x for x, cnt in freq if cnt == max_count]
    return min(empates) 

def _agg_percentile(vals, percentile_value=50):
    """
    percentile_value = 0..100 (ex. 50 => mediana)
    """
    tmp = [v for v in vals if v is not None]
    tmp.sort()
    if not tmp:
        return None
    p = percentile_value/100.0
    idx = p*(len(tmp)-1)
    if idx.is_integer():
        return tmp[int(idx)]
    else:
        lower_i = int(math.floor(idx))
        upper_i = int(math.ceil(idx))
        frac = idx - lower_i
        return tmp[lower_i]*(1-frac) + tmp[upper_i]*frac

def _agg_concat(vals, start="", sep=",", end=""):
    """
    Junta as strings do grupo. Se tiver ints/floats, converte pra str.
    """
    str_vals = []
    for v in vals:
        if v is None:
            continue
        str_vals.append(str(v))
    return start + sep.join(str_vals) + end

def _agg_count_blank(vals):
    """
    Conta quantos são string em branco.
    """
    count_b = 0
    for v in vals:
        if isinstance(v, str) and v.strip() == "":
            count_b += 1
    return float(count_b)

def _agg_count_non_blank(vals):
    """
    Conta quantos são string não-branco.
    """
    count_nb = 0
    for v in vals:
        if isinstance(v, str) and v.strip() != "":
            count_nb += 1
    return float(count_nb)

def _agg_longest(vals):
    """
    Retorna a string mais longa.
    Se empate, retorna uma delas.
    """
    str_only = [v for v in vals if isinstance(v, str)]
    if not str_only:
        return None
    return max(str_only, key=lambda s: len(s))

def _agg_shortest(vals):
    str_only = [v for v in vals if isinstance(v, str)]
    if not str_only:
        return None
    return min(str_only, key=lambda s: len(s))

def do_agg_operation(col_vals, op_name, params, sub_df):
    """
    Executa a operação (op_name) em col_vals (lista de valores da coluna).
    params = dict com financeRate, dateField, etc.
    sub_df = polars df do grupo completo (usado p/ XNPV, XIRR, etc. quando precisamos de col de datas).
    Retorna valor único (float|str|None).
    """
    op = op_name.upper().strip()

    if op == "SUM": return _agg_sum(col_vals)
    if op == "COUNT": return _agg_count(col_vals)
    if op == "COUNT_NON_NULL": return _agg_count_non_null(col_vals)
    if op == "COUNT_NULL": return _agg_count_null(col_vals)
    if op == "MIN": return _agg_min(col_vals)
    if op == "MAX": return _agg_max(col_vals)
    if op == "FIRST": return _agg_first(col_vals)
    if op == "LAST": return _agg_last(col_vals)
    if op == "AVG": return _agg_avg(col_vals)
    if op == "MEDIAN": return _agg_median(col_vals)
    if op == "MODE": return _agg_mode(col_vals)
    if op == "PRODUCT": return _agg_product(col_vals)
    if op == "COUNT_DISTINCT": return _agg_count_distinct(col_vals)
    if op == "COUNT_DISTINCT_NON_NULL": return _agg_count_distinct_non_null(col_vals)

    if op == "STDDEV":
        ignore_zeros = bool(params.get("ignoreZeros", False))
        return _agg_stddev(col_vals, ignore_zeros)

    if op == "VARIANCE":
        ignore_zeros = bool(params.get("ignoreZeros", False))
        return _agg_variance(col_vals, ignore_zeros)

    if op == "PERCENTILE":
        percentile_value = float(params.get("percentileValue", 50))
        return _agg_percentile(col_vals, percentile_value)

    if op == "CONCATENATE":
        start = params.get("concatStart", "")
        sep = params.get("concatSeparator", ",")
        end = params.get("concatEnd", "")
        return _agg_concat(col_vals, start, sep, end)

    if op == "COUNT_BLANK":
        return _agg_count_blank(col_vals)

    if op == "COUNT_NON_BLANK":
        return _agg_count_non_blank(col_vals)

    if op == "LONGEST":
        return _agg_longest(col_vals)

    if op == "SHORTEST":
        return _agg_shortest(col_vals)

    if op == "NPV":
        finance_rate = float(params.get("financeRate", 0.0))
        return _calc_npv(col_vals, finance_rate)

    if op == "IRR":
        guess = float(params.get("financeRate", 0.1))
        return _calc_irr(col_vals, guess)

    if op == "XNPV":
        date_field = params.get("dateField", None)
        finance_rate = float(params.get("financeRate", 0.0))
        if not date_field:
            return None
        date_list = sub_df[date_field].to_list()
        return _calc_xnpv(col_vals, date_list, finance_rate)

    if op == "XIRR":
        date_field = params.get("dateField", None)
        guess = float(params.get("financeRate", 0.1))
        if not date_field:
            return None
        date_list = sub_df[date_field].to_list()
        return _calc_xirr(col_vals, date_list, guess)

    if op == "MIRR":
        finance_rate = float(params.get("financeRate", 0.0))
        reinvest_rate = float(params.get("reinvestRate", 0.0))
        return _calc_mirr(col_vals, finance_rate, reinvest_rate)

    if op == "XMIRR":
        date_field = params.get("dateField", None)
        finance_rate = float(params.get("financeRate", 0.0))
        reinvest_rate = float(params.get("reinvestRate", 0.0))
        if not date_field:
            return None
        date_list = sub_df[date_field].to_list()
        return _calc_xmirr(col_vals, date_list, finance_rate, reinvest_rate)

    raise ValueError(f"Operação '{op_name}' não suportada.")


def sumarizar(global_df: pl.DataFrame, request):
    """
    Esperamos no request.json algo como:
    {
      "agrupamento": ["colA", "colB"],
      "operacoes": {
        "Valor": [
          { "operation": "SUM" },
          { "operation": "XNPV", "params": { "financeRate": 0.1, "dateField": "Data" } }
        ],
        "Quantidade": [
          { "operation": "IRR", "params": { "financeRate": 0.0 } }
        ]
      }
    }

    Retornamos (updated_df, None) ou (None, {"error": ...}).
    """
    data = request.json
    agrupamento = data.get("agrupamento", [])
    operacoes_dict = data.get("operacoes", {})

    if not agrupamento:
        return None, {"error": "Nenhuma coluna de agrupamento fornecida."}

    try:
        agg_instructions = []
        for col_name, lista_ops in operacoes_dict.items():
            for op_def in lista_ops:
                op_name = op_def.get("operation")
                params = op_def.get("params", {})
                agg_instructions.append((col_name, op_name, params))

        def group_aggregations(subdf: pl.DataFrame) -> pl.DataFrame:
            """
            Recebe um subdf (apenas as linhas de um grupo).
            Retorna um DF de 1 linha com as colunas de agrupamento + cada aggreg.
            """
            result_dict = {}
            for gcol in agrupamento:
                if len(subdf) == 0:
                    return pl.DataFrame([])
                group_val = subdf[gcol][0]
                result_dict[gcol] = group_val

            for (col_name, op_name, params) in agg_instructions:
                if col_name not in subdf.columns:
                    col_vals = [None]*len(subdf)
                else:
                    col_vals = subdf[col_name].to_list()

                val = do_agg_operation(col_vals, op_name, params, subdf)
                alias_name = f"{col_name}_{op_name.lower()}"
                result_dict[alias_name] = val

            return pl.DataFrame([result_dict])

        grouped = global_df.groupby(agrupamento, maintain_order=True)
        updated_polars_df = grouped.apply(group_aggregations)

        return updated_polars_df, None

    except Exception as e:
        return None, {"error": str(e)}

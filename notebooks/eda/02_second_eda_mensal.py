"""EDA temporal em frequência mensal (espelho de 01_second_eda.py).

Usa `agg_evolucao_temporal` (ANO, MES) e `agg_evolucao_mensal_regiao`.
Indicadores relativos: participação regional % por mês; variação % mês a mês.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

plt.style.use("seaborn-v0_8-whitegrid")
sns.set_context("notebook", font_scale=1.1)

_ANO_INI = 2012
_ANO_FIM = 2024
_PANDEMIA_INI = 2020
_PANDEMIA_FIM = 2022
# Gráfico de variação % mês a mês: eixo X a partir deste ano
_VAR_PCT_MES_ANO_INI = 2015


def _filtro_anos(df: pd.DataFrame, col: str = "ANO") -> pd.DataFrame:
    out = df[(df[col] >= _ANO_INI) & (df[col] <= _ANO_FIM)].copy()
    if out.empty:
        logger.warning(
            "Nenhum dado entre %s e %s em %s; usando série completa.",
            _ANO_INI,
            _ANO_FIM,
            col,
        )
        return df.copy()
    return out


def _add_data_ref(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["DATA"] = pd.to_datetime(
        {"year": out["ANO"], "month": out["MES"], "day": 1}
    )
    return out.sort_values(["ANO", "MES"]).reset_index(drop=True)


def _serie_nacional_mensal(df_tempo: pd.DataFrame) -> pd.DataFrame:
    d = _filtro_anos(df_tempo)
    return _add_data_ref(d)


def _participacao_mensal(df_mes_reg: pd.DataFrame) -> pd.DataFrame:
    d = _filtro_anos(df_mes_reg)
    br = d.groupby(["ANO", "MES"], as_index=False)["TOTAL"].sum()
    br = br.rename(columns={"TOTAL": "TOTAL_BR"})
    m = d.merge(br, on=["ANO", "MES"], how="left")
    m["PCT_NACIONAL"] = np.where(
        m["TOTAL_BR"] > 0, 100.0 * m["TOTAL"] / m["TOTAL_BR"], np.nan
    )
    return _add_data_ref(m)


def _classifica_macro_ano(ano: float) -> str:
    if pd.isna(ano):
        return "Desconhecido"
    if ano < _PANDEMIA_INI:
        return "Pré-pandemia (até 2019)"
    if ano <= _PANDEMIA_FIM:
        return "Durante pandemia (2020–2022)"
    return "Pós-pandemia (2023+)"


def _medias_mensais_por_macro(df_tempo: pd.DataFrame) -> pd.DataFrame:
    """Média de internações/mês por macro-período (só meses com dado)."""
    d = _serie_nacional_mensal(df_tempo)
    d["MACRO"] = d["ANO"].apply(_classifica_macro_ano)
    rows = []
    for macro, g in d.groupby("MACRO"):
        if macro == "Desconhecido":
            continue
        n_meses = len(g)
        soma = g["TOTAL"].sum()
        rows.append(
            {
                "MACRO": macro,
                "N_MESES": n_meses,
                "TOTAL_PERIODO": soma,
                "MEDIA_MENSAL": soma / n_meses if n_meses else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _plot_pandemia_strip_mes(ax: plt.Axes) -> None:
    t0 = pd.Timestamp(year=_PANDEMIA_INI, month=1, day=1)
    t1 = pd.Timestamp(year=_PANDEMIA_FIM, month=12, day=31)
    ax.axvspan(
        t0,
        t1,
        color="crimson",
        alpha=0.12,
        label="Pandemia (2020–2022)",
    )


def main() -> None:  # noqa: PLR0915, PLR0914
    base_dir = Path(__file__).resolve().parents[2]
    interim_dir = base_dir / "data" / "interim"
    fig_dir = base_dir / "reports" / "figures" / "eda" / "mensal"
    fig_dir.mkdir(parents=True, exist_ok=True)

    req = [
        interim_dir / "agg_evolucao_temporal.parquet",
        interim_dir / "agg_evolucao_mensal_regiao.parquet",
        interim_dir / "agg_pandemia_regiao.parquet",
    ]
    for p in req:
        if not p.exists():
            logger.error("Arquivo ausente: %s", p)
            logger.error("Execute notebooks/processing/02_aggregate_data.py.")
            return

    df_tempo = pd.read_parquet(interim_dir / "agg_evolucao_temporal.parquet")
    df_mes_reg = pd.read_parquet(
        interim_dir / "agg_evolucao_mensal_regiao.parquet"
    )
    df_pand_reg = pd.read_parquet(interim_dir / "agg_pandemia_regiao.parquet")

    path_nac = interim_dir / "agg_pandemia_nacional.parquet"
    if path_nac.exists():
        df_pand_nac = pd.read_parquet(path_nac)
    else:
        logger.warning(
            "agg_pandemia_nacional.parquet ausente; derivando do regional."
        )
        df_pand_nac = df_pand_reg.groupby("PERIODO_PANDEMIA", as_index=False)[
            "TOTAL"
        ].sum()

    nacional_mes = _serie_nacional_mensal(df_tempo)
    logger.info(
        "Meses na série (%s–%s): %s a %s",
        _ANO_INI,
        _ANO_FIM,
        nacional_mes["DATA"].min(),
        nacional_mes["DATA"].max(),
    )

    # 1) Evolução nacional mensal
    fig, ax = plt.subplots(figsize=(14, 5))
    sns.lineplot(
        data=nacional_mes,
        x="DATA",
        y="TOTAL",
        linewidth=1.2,
        ax=ax,
    )
    _plot_pandemia_strip_mes(ax)
    ax.set_title(
        "Internações renais (Sul+Sudeste) — frequência mensal (%s–%s)"
        % (_ANO_INI, _ANO_FIM)
    )
    ax.set_ylabel("Internações no mês")
    ax.set_xlabel("Mês")
    ax.legend(loc="upper left")
    fig.autofmt_xdate()
    plt.tight_layout()
    f1 = fig_dir / "02_evolucao_nacional_mensal.png"
    fig.savefig(f1, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f1)
    plt.show()

    # 2) Evolução por região (mensal)
    dreg = _add_data_ref(_filtro_anos(df_mes_reg))
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.lineplot(
        data=dreg,
        x="DATA",
        y="TOTAL",
        hue="REGIAO",
        linewidth=1.0,
        ax=ax,
    )
    _plot_pandemia_strip_mes(ax)
    ax.set_title("Internações por região — série mensal")
    ax.set_ylabel("Internações no mês")
    ax.set_xlabel("Mês")
    ax.legend(title="Região", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.autofmt_xdate()
    plt.tight_layout()
    f2 = fig_dir / "03_evolucao_regiao_mensal.png"
    fig.savefig(f2, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f2)
    plt.show()

    # 3) Pandemia — totais (iguais ao EDA anual; não são taxa mensal)
    fig, ax = plt.subplots(figsize=(8, 5))
    order = sorted(df_pand_nac["PERIODO_PANDEMIA"].unique())
    sns.barplot(
        data=df_pand_nac,
        x="PERIODO_PANDEMIA",
        y="TOTAL",
        order=order,
        hue="PERIODO_PANDEMIA",
        hue_order=order,
        palette="Set2",
        legend=False,
        ax=ax,
    )
    ax.set_title(
        "Volume acumulado por período pandêmico (Sul+Sudeste, sem região)"
    )
    ax.set_xlabel("Período")
    ax.set_ylabel("Total de internações")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    f3 = fig_dir / "04_pandemia_nacional.png"
    fig.savefig(f3, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f3)
    plt.show()

    # 4) Pandemia por região (totais)
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.barplot(
        data=df_pand_reg,
        x="REGIAO",
        y="TOTAL",
        hue="PERIODO_PANDEMIA",
        palette="muted",
        ax=ax,
    )
    ax.set_title("Internações por região e período (pré / durante / pós)")
    ax.set_ylabel("Total de internações")
    ax.set_xlabel("Região")
    ax.legend(title="Período", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    f4 = fig_dir / "05_pandemia_por_regiao.png"
    fig.savefig(f4, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f4)
    plt.show()

    # 5) Participação regional % por mês
    part = _participacao_mensal(df_mes_reg)
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.lineplot(
        data=part,
        x="DATA",
        y="PCT_NACIONAL",
        hue="REGIAO",
        linewidth=1.0,
        ax=ax,
    )
    _plot_pandemia_strip_mes(ax)
    ax.set_title("Participação regional (% do total mensal Sul+Sudeste)")
    ax.set_ylabel("% do total no mês")
    ax.set_xlabel("Mês")
    ax.legend(title="Região", bbox_to_anchor=(1.02, 1), loc="upper left")
    fig.autofmt_xdate()
    plt.tight_layout()
    f5 = fig_dir / "06_participacao_regional_mensal_pct.png"
    fig.savefig(f5, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f5)
    plt.show()

    # 6) Variação % mês a mês (nacional), série exibida a partir de 2015
    mens = nacional_mes.sort_values(["ANO", "MES"]).copy()
    mens["VAR_PCT_MES"] = mens["TOTAL"].pct_change() * 100.0
    mens_var = mens.dropna(subset=["VAR_PCT_MES"])
    mens_var = mens_var[mens_var["ANO"] >= _VAR_PCT_MES_ANO_INI]
    fig, ax = plt.subplots(figsize=(14, 5))
    sns.lineplot(
        data=mens_var,
        x="DATA",
        y="VAR_PCT_MES",
        linewidth=0.9,
        ax=ax,
    )
    ax.axhline(0, color="black", linewidth=0.8)
    _plot_pandemia_strip_mes(ax)
    ax.set_title(
        "Variação %% do total nacional vs mês anterior (desde %s) — "
        "não é taxa por 100 mil hab."
        % _VAR_PCT_MES_ANO_INI
    )
    ax.set_xlabel("Mês")
    ax.set_ylabel("Δ % vs mês anterior")
    fig.autofmt_xdate()
    plt.tight_layout()
    f6 = fig_dir / "07_variacao_mensal_pct_nacional.png"
    fig.savefig(f6, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f6)
    plt.show()

    # 7) Média mensal por macro-período
    med = _medias_mensais_por_macro(df_tempo)
    tbl = med.to_string(index=False)
    logger.info("Médias mensais por macro-período:\n%s", tbl)
    macro_order = [
        "Pré-pandemia (até 2019)",
        "Durante pandemia (2020–2022)",
        "Pós-pandemia (2023+)",
    ]
    med_plot = med.set_index("MACRO").reindex(macro_order).reset_index()
    fig, ax = plt.subplots(figsize=(9, 5))
    med_bars = med_plot.dropna(subset=["MEDIA_MENSAL"])
    present = set(med_bars["MACRO"].tolist())
    hue_order = [m for m in macro_order if m in present]
    sns.barplot(
        data=med_bars,
        x="MACRO",
        y="MEDIA_MENSAL",
        hue="MACRO",
        hue_order=hue_order,
        palette=["steelblue", "indianred", "seagreen"],
        legend=False,
        ax=ax,
    )
    ax.set_title("Média de internações por mês — pré / durante / pós pandemia")
    ax.set_ylabel("Média mensal (internações/mês no período)")
    plt.xticks(rotation=12, ha="right")
    plt.tight_layout()
    f7 = fig_dir / "08_medias_mensais_pre_durante_pos.png"
    fig.savefig(f7, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f7)
    plt.show()


if __name__ == "__main__":
    main()

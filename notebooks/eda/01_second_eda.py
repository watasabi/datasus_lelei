"""EDA temporal: pandemia, região, evolução 2012–2024 e indicadores relativos.

Sem projeção populacional (IBGE), a 'taxa' aqui é:
- participação regional (% do total nacional por ano);
- variação percentual ano a ano no total nacional (ritmo de mudança).
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

# Janela de análise solicitada (recorta o que existir na base)
_ANO_INI = 2012
_ANO_FIM = 2024
_PANDEMIA_INI = 2020
_PANDEMIA_FIM = 2022


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


def _totais_anuais(df_tempo: pd.DataFrame) -> pd.DataFrame:
    """Soma mensal → total por ano."""
    d = _filtro_anos(df_tempo)
    return d.groupby("ANO", as_index=False)["TOTAL"].sum()


def _participacao_regional(df_regiao: pd.DataFrame) -> pd.DataFrame:
    """% do total nacional de internações, por ano e região."""
    d = _filtro_anos(df_regiao)
    br = d.groupby("ANO")["TOTAL"].sum().rename("TOTAL_BR")
    m = d.merge(br, on="ANO", how="left")
    m["PCT_NACIONAL"] = np.where(
        m["TOTAL_BR"] > 0, 100.0 * m["TOTAL"] / m["TOTAL_BR"], np.nan
    )
    return m


def _classifica_macro_ano(ano: float) -> str:
    if pd.isna(ano):
        return "Desconhecido"
    if ano < _PANDEMIA_INI:
        return "Pré-pandemia (até 2019)"
    if ano <= _PANDEMIA_FIM:
        return "Durante pandemia (2020–2022)"
    return "Pós-pandemia (2023+)"


def _medias_anuais_por_macro(df_tempo: pd.DataFrame) -> pd.DataFrame:
    """Média de internações/ano por macro-período (só anos com dado)."""
    anu = _totais_anuais(df_tempo)
    anu["MACRO"] = anu["ANO"].apply(_classifica_macro_ano)
    rows = []
    for macro, g in anu.groupby("MACRO"):
        if macro == "Desconhecido":
            continue
        n_anos = g["ANO"].nunique()
        soma = g["TOTAL"].sum()
        rows.append(
            {
                "MACRO": macro,
                "N_ANOS": n_anos,
                "TOTAL_PERIODO": soma,
                "MEDIA_ANUAL": soma / n_anos if n_anos else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _plot_pandemia_strip(ax: plt.Axes) -> None:
    ax.axvspan(
        _PANDEMIA_INI - 0.5,
        _PANDEMIA_FIM + 0.5,
        color="crimson",
        alpha=0.12,
        label="Pandemia (2020–2022)",
    )


def main() -> None:  # noqa: PLR0915, PLR0914
    base_dir = Path(__file__).resolve().parents[2]
    interim_dir = base_dir / "data" / "interim"
    fig_dir = base_dir / "reports" / "figures" / "eda"
    fig_dir.mkdir(parents=True, exist_ok=True)

    req = [
        interim_dir / "agg_evolucao_temporal.parquet",
        interim_dir / "agg_evolucao_regiao.parquet",
        interim_dir / "agg_pandemia_regiao.parquet",
    ]
    for p in req:
        if not p.exists():
            logger.error("Arquivo ausente: %s", p)
            logger.error("Execute notebooks/processing/02_aggregate_data.py.")
            return

    df_tempo = pd.read_parquet(interim_dir / "agg_evolucao_temporal.parquet")
    df_regiao = pd.read_parquet(interim_dir / "agg_evolucao_regiao.parquet")
    df_pand_reg = pd.read_parquet(interim_dir / "agg_pandemia_regiao.parquet")

    path_nac = interim_dir / "agg_pandemia_nacional.parquet"
    if path_nac.exists():
        df_pand_nac = pd.read_parquet(path_nac)
    else:
        logger.warning(
            "agg_pandemia_nacional.parquet ausente; derivando do regional."
        )
        df_pand_nac = (
            df_pand_reg.groupby("PERIODO_PANDEMIA", as_index=False)["TOTAL"]
            .sum()
        )

    evolucao = _totais_anuais(df_tempo)
    logger.info(
        "Anos na série (após filtro %s–%s): %s",
        _ANO_INI,
        _ANO_FIM,
        sorted(evolucao["ANO"].unique().tolist()),
    )

    # 1) Evolução nacional 2012–2024
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(
        data=evolucao, x="ANO", y="TOTAL", marker="o", linewidth=2.5, ax=ax
    )
    _plot_pandemia_strip(ax)
    ax.set_title(
        "Evolução das internações renais (Sul+Sudeste) — janela %s–%s"
        % (_ANO_INI, _ANO_FIM)
    )
    ax.set_ylabel("Total de internações (ano)")
    ax.set_xlabel("Ano")
    ax.legend(loc="upper left")
    plt.tight_layout()
    f1 = fig_dir / "02_evolucao_nacional_2012_2024.png"
    fig.savefig(f1, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f1)
    plt.show()

    # 2) Evolução segregada por região
    dreg = _filtro_anos(df_regiao)
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.lineplot(
        data=dreg,
        x="ANO",
        y="TOTAL",
        hue="REGIAO",
        marker="o",
        linewidth=2,
        ax=ax,
    )
    _plot_pandemia_strip(ax)
    ax.set_title("Internações por região — mesma janela temporal")
    ax.set_ylabel("Total de internações (ano)")
    ax.set_xlabel("Ano")
    ax.legend(title="Região", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    f2 = fig_dir / "03_evolucao_por_regiao.png"
    fig.savefig(f2, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f2)
    plt.show()

    # 3) Pandemia — não segregado (Brasil)
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
        "Volume total por período pandêmico (Sul+Sudeste, sem região)"
    )
    ax.set_xlabel("Período")
    ax.set_ylabel("Total de internações")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    f3 = fig_dir / "04_pandemia_nacional.png"
    fig.savefig(f3, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f3)
    plt.show()

    # 4) Pandemia segregada por região
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

    # 5) "Taxa" relativa: participação % da região no total nacional/ano
    part = _participacao_regional(df_regiao)
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.lineplot(
        data=part,
        x="ANO",
        y="PCT_NACIONAL",
        hue="REGIAO",
        marker="o",
        ax=ax,
    )
    _plot_pandemia_strip(ax)
    ax.set_title(
        "Participação regional (% do total nacional de internações/ano)"
    )
    ax.set_ylabel("% do total Sul+Sudeste")
    ax.set_xlabel("Ano")
    ax.legend(title="Região", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    f5 = fig_dir / "06_participacao_regional_pct.png"
    fig.savefig(f5, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f5)
    plt.show()

    # 6) Variação % ano a ano (nacional) — evidência de mudança pós-pandemia
    ev = _totais_anuais(df_tempo).sort_values("ANO")
    ev["VAR_PCT_ANO"] = ev["TOTAL"].pct_change() * 100.0
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.barplot(
        data=ev.dropna(subset=["VAR_PCT_ANO"]),
        x="ANO",
        y="VAR_PCT_ANO",
        ax=ax,
    )
    ax.axhline(0, color="black", linewidth=0.8)
    _plot_pandemia_strip(ax)
    ax.set_title(
        "Variação % do total nacional em relação ao ano anterior "
        "(mudança de ritmo; não é taxa por 100 mil hab.)"
    )
    ax.set_xlabel("Ano")
    ax.set_ylabel("Δ % vs ano anterior")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    f6 = fig_dir / "07_variacao_anual_pct_nacional.png"
    fig.savefig(f6, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f6)
    plt.show()

    # 7) Comparativo de média anual: pré vs durante vs pós (ênfase alteração)
    med = _medias_anuais_por_macro(df_tempo)
    tbl = med.to_string(index=False)
    logger.info("Médias anuais por macro-período:\n%s", tbl)
    macro_order = [
        "Pré-pandemia (até 2019)",
        "Durante pandemia (2020–2022)",
        "Pós-pandemia (2023+)",
    ]
    med_plot = med.set_index("MACRO").reindex(macro_order).reset_index()
    fig, ax = plt.subplots(figsize=(9, 5))
    med_bars = med_plot.dropna(subset=["MEDIA_ANUAL"])
    present = set(med_bars["MACRO"].tolist())
    hue_order = [m for m in macro_order if m in present]
    sns.barplot(
        data=med_bars,
        x="MACRO",
        y="MEDIA_ANUAL",
        hue="MACRO",
        hue_order=hue_order,
        palette=["steelblue", "indianred", "seagreen"],
        legend=False,
        ax=ax,
    )
    ax.set_title(
        "Média de internações por ano — pré / durante / pós pandemia "
        "(alteração de patamar)"
    )
    ax.set_ylabel("Média anual (internações/ano no período)")
    plt.xticks(rotation=12, ha="right")
    plt.tight_layout()
    f7 = fig_dir / "08_medias_pre_durante_pos.png"
    fig.savefig(f7, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f7)
    plt.show()


if __name__ == "__main__":
    main()

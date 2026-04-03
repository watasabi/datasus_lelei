"""Taxa de internações renais por 100 mil hab. (Sul e Sudeste, pop. IBGE)."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

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


def _plot_pandemia_strip(ax: plt.Axes) -> None:
    ax.axvspan(
        _PANDEMIA_INI - 0.5,
        _PANDEMIA_FIM + 0.5,
        color="crimson",
        alpha=0.12,
        label="Pandemia (2020–2022)",
    )


def _pop_long(pop: pd.DataFrame) -> pd.DataFrame:
    pl = pop.melt(
        id_vars=["ANO"],
        value_vars=["POP_SUL", "POP_SUDESTE"],
        var_name="_c",
        value_name="POP",
    )
    pl["REGIAO"] = pl["_c"].map({"POP_SUL": "Sul", "POP_SUDESTE": "Sudeste"})
    return pl.drop(columns=["_c"])


def _taxa_por_regiao(
    dreg: pd.DataFrame, pop_long: pd.DataFrame
) -> pd.DataFrame:
    m = dreg.merge(pop_long, on=["ANO", "REGIAO"], how="inner")
    m["TAXA_100K"] = m["TOTAL"] / m["POP"] * 100_000.0
    return m


def _taxa_escopo_somado(dreg: pd.DataFrame, pop: pd.DataFrame) -> pd.DataFrame:
    tot = dreg.groupby("ANO", as_index=False)["TOTAL"].sum()
    tot = tot.rename(columns={"TOTAL": "INTERNACOES_ESCOPO"})
    out = tot.merge(
        pop[["ANO", "POP_SUL_SUDESTE"]],
        on="ANO",
        how="inner",
    )
    out["TAXA_100K"] = (
        out["INTERNACOES_ESCOPO"] / out["POP_SUL_SUDESTE"] * 100_000.0
    )
    return out


def _export_tabela_resumo(
    m_reg: pd.DataFrame,
    comb: pd.DataFrame,
    path: Path,
) -> None:
    sul = m_reg[m_reg["REGIAO"] == "Sul"][
        ["ANO", "TOTAL", "POP", "TAXA_100K"]
    ].rename(
        columns={
            "TOTAL": "INTERNACOES_SUL",
            "POP": "POP_SUL",
            "TAXA_100K": "TAXA_100K_SUL",
        }
    )
    sud = m_reg[m_reg["REGIAO"] == "Sudeste"][
        ["ANO", "TOTAL", "POP", "TAXA_100K"]
    ].rename(
        columns={
            "TOTAL": "INTERNACOES_SUDESTE",
            "POP": "POP_SUDESTE",
            "TAXA_100K": "TAXA_100K_SUDESTE",
        }
    )
    wide = sul.merge(sud, on="ANO", how="outer")
    wide = wide.merge(
        comb.rename(
            columns={
                "INTERNACOES_ESCOPO": "INTERNACOES_SUL_MAIS_SUDESTE",
                "POP_SUL_SUDESTE": "POP_SUL_MAIS_SUDESTE",
                "TAXA_100K": "TAXA_100K_SUL_MAIS_SUDESTE",
            }
        ),
        on="ANO",
        how="outer",
    )
    wide = wide.sort_values("ANO")
    wide.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info("Tabela: %s", path)


def main() -> None:
    base = Path(__file__).resolve().parents[2]
    interim = base / "data" / "interim"
    pop_path = base / "data" / "external" / "ibge_populacao_sul_sudeste.csv"
    fig_dir = base / "reports" / "figures" / "eda"
    tbl_dir = base / "reports" / "tables"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tbl_dir.mkdir(parents=True, exist_ok=True)

    if not pop_path.exists():
        logger.error("Ficheiro de população ausente: %s", pop_path)
        return

    reg_path = interim / "agg_evolucao_regiao.parquet"
    if not reg_path.exists():
        logger.error(
            "Execute notebooks/processing/02_aggregate_data.py primeiro."
        )
        return

    pop = pd.read_csv(pop_path)
    pop["POP_SUL_SUDESTE"] = pop["POP_SUL"] + pop["POP_SUDESTE"]

    dreg = pd.read_parquet(reg_path)
    dreg = dreg[dreg["REGIAO"].isin(["Sul", "Sudeste"])].copy()
    dreg = dreg[(dreg["ANO"] >= _ANO_INI) & (dreg["ANO"] <= _ANO_FIM)]

    pop_long = _pop_long(pop)
    m_reg = _taxa_por_regiao(dreg, pop_long)
    comb = _taxa_escopo_somado(dreg, pop)

    _export_tabela_resumo(
        m_reg,
        comb,
        tbl_dir / "taxa_internacoes_100k_sul_sudeste.csv",
    )

    # Gráfico 1: taxa por região (denominador = pop. da própria região)
    fig, ax = plt.subplots(figsize=(12, 5.5))
    sns.lineplot(
        data=m_reg,
        x="ANO",
        y="TAXA_100K",
        hue="REGIAO",
        marker="o",
        linewidth=2.2,
        ax=ax,
    )
    _plot_pandemia_strip(ax)
    ax.set_title(
        "Taxa de internações renais por 100 mil habitantes — Sul e Sudeste"
    )
    ax.set_ylabel("Internações / 100 mil hab. (ano)")
    ax.set_xlabel("Ano")
    ax.legend(title="Região", loc="upper left")
    fig.text(
        0.5,
        0.01,
        "Denominador: população residente IBGE da respectiva região "
        "(ficheiro data/external/ibge_populacao_sul_sudeste.csv).",
        ha="center",
        fontsize=9,
        style="italic",
    )
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.14)
    f1 = fig_dir / "11_taxa_internacoes_100k_por_regiao.png"
    fig.savefig(f1, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f1)
    plt.close(fig)

    # Gráfico 2: escopo Sul+Sudeste (internações somadas / pop. somada)
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(
        data=comb,
        x="ANO",
        y="TAXA_100K",
        marker="o",
        linewidth=2.5,
        color="#2c3e50",
        label="Taxa (escopo Sul+Sudeste)",
        ax=ax,
    )
    _plot_pandemia_strip(ax)
    ax.set_title(
        "Taxa no escopo Sul + Sudeste "
        "(soma das internações / soma das populações)"
    )
    ax.set_ylabel("Internações / 100 mil hab. (ano)")
    ax.set_xlabel("Ano")
    ax.legend(loc="upper left")
    fig.text(
        0.5,
        0.01,
        "Numerador: internações Sul + Sudeste. "
        "Denominador: POP_SUL + POP_SUDESTE (IBGE).",
        ha="center",
        fontsize=9,
        style="italic",
    )
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.14)
    f2 = fig_dir / "12_taxa_internacoes_100k_escopo_sul_mais_sudeste.png"
    fig.savefig(f2, dpi=150, bbox_inches="tight")
    logger.info("Salvo: %s", f2)
    plt.close(fig)


if __name__ == "__main__":
    main()

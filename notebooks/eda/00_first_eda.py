"""EDA inicial: demografia, volume por região e CIDs renais (N17–N19)."""

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

_PANDEMIA_INI = 2020
_PANDEMIA_FIM = 2022

_PERIODO_LABELS: tuple[tuple[int, int, str], ...] = (
    (-10_000, _PANDEMIA_INI - 1, "Pré-pandemia (até 2019)"),
    (_PANDEMIA_INI, _PANDEMIA_FIM, "Durante pandemia (2020–2022)"),
    (_PANDEMIA_FIM + 1, 10_000, "Pós-pandemia (2023+)"),
)

_PERIODO_ORDER = [t[2] for t in _PERIODO_LABELS]
_PERIODO_PALETTE = {
    "Pré-pandemia (até 2019)": "#4C72B0",
    "Durante pandemia (2020–2022)": "#C44E52",
    "Pós-pandemia (2023+)": "#55A868",
}

# Capítulo renal CID-10 (doenças renais) — alinhado ao filtro em 00_get_data.py
_PREFIXOS_CID_RENAL = ("N17", "N18", "N19")


def _periodo_pandemia_por_ano(ano: float) -> str:
    if pd.isna(ano):
        return "Sem ano"
    y = int(ano)
    for lo, hi, lab in _PERIODO_LABELS:
        if lo <= y <= hi:
            return lab
    return "Fora da faixa"


def _resolve_cleaned_path(base_dir: Path) -> Path:
    """Prefere dados processados; fallback para raw."""
    processed = base_dir / "data" / "processed" / "renais_cleaned.parquet"
    raw = base_dir / "data" / "raw" / "renais_cleaned.parquet"
    if processed.exists():
        return processed
    return raw


def _cid_series_normalizado(df: pd.DataFrame) -> pd.Series:
    s = (
        df["CID_RENAL"]
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": ""})
    )
    return s.mask(s.str.len() == 0, "Não informado")


def _mascara_capitulo_renal(cid: pd.Series) -> pd.Series:
    """True se o código começa por N17, N18 ou N19 (CID-10 renal)."""
    cod = cid.astype(str).str.strip().str.upper()
    return cod.str.startswith(_PREFIXOS_CID_RENAL)


def _plot_cids_renais_contagem(fig_dir: Path, cid: pd.Series) -> None:
    vc = cid.value_counts()
    plt.figure(figsize=(10, max(5, 0.35 * len(vc))))
    sns.barplot(
        x=vc.values,
        y=vc.index,
        hue=vc.index,
        palette="viridis",
        legend=False,
    )
    plt.title(
        "CIDs renais (N17–N19): diagnóstico principal — frequência na base"
    )
    plt.xlabel("Número de internações")
    plt.ylabel("CID renal")
    plt.tight_layout()
    out_c = fig_dir / "09_cids_renais_contagem.png"
    plt.savefig(out_c, dpi=150, bbox_inches="tight")
    logger.info("Figura salva: %s", out_c)
    plt.show()


def _plot_cids_renais_por_periodo(
    fig_dir: Path, df: pd.DataFrame, cid: pd.Series
) -> None:
    """Barras empilhadas por período pandêmico (só capítulo renal)."""
    if "DATA_INTERNACAO" not in df.columns:
        logger.warning("DATA_INTERNACAO ausente; ignorando CIDs × período.")
        return

    dfc = df.assign(
        CID_RENAL=cid,
        ANO_INT=df["DATA_INTERNACAO"].dt.year,
    )
    dfc = dfc.dropna(subset=["ANO_INT"])
    dfc["PERIODO_PANDEMIA"] = dfc["ANO_INT"].apply(_periodo_pandemia_por_ano)
    dfc = dfc[dfc["PERIODO_PANDEMIA"].isin(_PERIODO_ORDER)].copy()
    if dfc.empty:
        logger.warning(
            "Sem linhas com período pandêmico definido; skip CIDs × período."
        )
        return

    order_cid = cid.value_counts().index.tolist()
    ct = pd.crosstab(dfc["CID_RENAL"], dfc["PERIODO_PANDEMIA"])
    for col in _PERIODO_ORDER:
        if col not in ct.columns:
            ct[col] = 0
    ct = ct[_PERIODO_ORDER]
    ct = ct.reindex(order_cid).fillna(0).astype(int)

    fig, ax = plt.subplots(figsize=(11, max(5, 0.35 * len(ct))))
    left = np.zeros(len(ct))
    for lab in _PERIODO_ORDER:
        vals = ct[lab].to_numpy()
        ax.barh(
            ct.index.astype(str),
            vals,
            left=left,
            label=lab,
            color=_PERIODO_PALETTE[lab],
            alpha=0.9,
        )
        left += vals
    ax.set_title(
        "CIDs renais (N17–N19): contagem por período (ano da internação)"
    )
    ax.set_xlabel("Número de internações")
    ax.set_ylabel("CID renal")
    ax.legend(title="Período", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    out_d = fig_dir / "10_cids_renais_por_periodo_pandemia.png"
    plt.savefig(out_d, dpi=150, bbox_inches="tight")
    logger.info("Figura salva: %s", out_d)
    plt.show()


def main() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    fig_dir = base_dir / "reports" / "figures" / "eda"
    fig_dir.mkdir(parents=True, exist_ok=True)

    path = _resolve_cleaned_path(base_dir)
    if not path.exists():
        logger.error("Arquivo não encontrado: %s", path)
        logger.error("Execute o 01_cleaning_cols.py antes do EDA.")
        return

    logger.info("Carregando %s", path)
    df = pd.read_parquet(path)

    # --- Idade por região (distribuição) ---
    plt.figure(figsize=(11, 5))
    sns.histplot(data=df, x="IDADE", bins=24, hue="REGIAO", multiple="stack")
    plt.title("Distribuição de idade nas internações renais, por região")
    plt.xlabel("Idade (anos)")
    plt.ylabel("Contagem")
    plt.tight_layout()
    out_a = fig_dir / "00_idade_por_regiao.png"
    plt.savefig(out_a, dpi=150, bbox_inches="tight")
    logger.info("Figura salva: %s", out_a)
    plt.show()

    # --- Volume absoluto por região (amostra agregada) ---
    plt.figure(figsize=(9, 5))
    order = df["REGIAO"].value_counts().index
    sns.countplot(
        data=df,
        y="REGIAO",
        order=order,
        hue="REGIAO",
        hue_order=order,
        palette="viridis",
        legend=False,
    )
    plt.title("Volume de registros por região")
    plt.xlabel("Número de internações")
    plt.ylabel("Região")
    plt.tight_layout()
    out_b = fig_dir / "01_volume_por_regiao.png"
    plt.savefig(out_b, dpi=150, bbox_inches="tight")
    logger.info("Figura salva: %s", out_b)
    plt.show()

    if "CID_RENAL" not in df.columns:
        logger.warning("Coluna CID_RENAL ausente; ignorando gráficos de CID.")
        return

    cid = _cid_series_normalizado(df)
    m_renal = _mascara_capitulo_renal(cid)
    n_out = int((~m_renal).sum())
    if n_out:
        logger.info(
            "Fora do capítulo renal N17–N19 (ou vazio): %s linhas excluídas "
            "dos gráficos de CID.",
            n_out,
        )
    cid = cid[m_renal]
    df = df.loc[m_renal]
    if cid.empty:
        logger.warning("Sem códigos N17–N19; skip gráficos de CIDs renais.")
        return

    _plot_cids_renais_contagem(fig_dir, cid)
    _plot_cids_renais_por_periodo(fig_dir, df, cid)


if __name__ == "__main__":
    main()

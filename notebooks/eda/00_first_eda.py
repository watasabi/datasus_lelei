"""EDA inicial: distribuição demográfica e volume por região."""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
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


def _resolve_cleaned_path(base_dir: Path) -> Path:
    """Prefere dados processados; fallback para raw."""
    processed = base_dir / "data" / "processed" / "renais_cleaned.parquet"
    raw = base_dir / "data" / "raw" / "renais_cleaned.parquet"
    if processed.exists():
        return processed
    return raw


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


if __name__ == "__main__":
    main()

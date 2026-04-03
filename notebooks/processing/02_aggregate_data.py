import logging
from pathlib import Path

import pandas as pd

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Cortes temporais para classificação pandemia COVID-19 (SIH)
_YEAR_PRE_PANDEMIA_LIMITE = 2020  # exclusivo: ano < limite = pré-pandemia
_YEAR_PANDEMIA_INI = 2020
_YEAR_PANDEMIA_FIM = 2022


def _classify_pandemic(year: float) -> str:
    if pd.isna(year):
        return "Desconhecido"
    if year < _YEAR_PRE_PANDEMIA_LIMITE:
        return "1. Pré-Pandemia (até 2019)"
    if _YEAR_PANDEMIA_INI <= year <= _YEAR_PANDEMIA_FIM:
        return "2. Durante Pandemia (2020-2022)"
    return "3. Pós-Pandemia (2023+)"


def create_aggregations(input_path: Path, out_dir: Path) -> None:
    """Agrupa dados renais para análises temporais e de pandemia.

    Args:
        input_path: Parquet limpo (renais_cleaned.parquet).
        out_dir: Diretório interim para os agregados.
    """
    logger.info("Carregando dados limpos de %s", input_path)
    df = pd.read_parquet(input_path)

    # Garante que temos as colunas temporais
    df["ANO"] = df["DATA_INTERNACAO"].dt.year
    df["MES"] = df["DATA_INTERNACAO"].dt.month

    df["PERIODO_PANDEMIA"] = df["ANO"].apply(_classify_pandemic)

    # 1. Evolução Temporal Geral (Ano e Mês)
    logger.info("Gerando agregações temporais...")
    path_tempo = out_dir / "agg_evolucao_temporal.parquet"
    agg_tempo = df.groupby(["ANO", "MES"]).size().reset_index(name="TOTAL")
    agg_tempo.to_parquet(path_tempo)
    logger.info("Salvo %s: %s lin.", path_tempo.name, len(agg_tempo))

    # 2. Internações por Região ao longo dos anos
    logger.info("Gerando agregações por região...")
    path_regiao = out_dir / "agg_evolucao_regiao.parquet"
    agg_regiao = df.groupby(["ANO", "REGIAO"]).size().reset_index(name="TOTAL")
    agg_regiao.to_parquet(path_regiao)
    logger.info("Salvo %s: %s lin.", path_regiao.name, len(agg_regiao))

    # 2b. Mensal por região (para EDA de frequência mensal)
    path_mes_reg = out_dir / "agg_evolucao_mensal_regiao.parquet"
    agg_mes_reg = (
        df.groupby(["ANO", "MES", "REGIAO"]).size().reset_index(name="TOTAL")
    )
    agg_mes_reg.to_parquet(path_mes_reg)
    logger.info("Salvo %s: %s lin.", path_mes_reg.name, len(agg_mes_reg))

    # 3. Impacto da Pandemia (Geral e por Região)
    logger.info("Gerando agregações de pandemia...")
    path_pandemia = out_dir / "agg_pandemia_regiao.parquet"
    agg_pandemia = (
        df.groupby(["PERIODO_PANDEMIA", "REGIAO"])
        .size()
        .reset_index(name="TOTAL")
    )
    agg_pandemia.to_parquet(path_pandemia)
    logger.info("Salvo %s: %s lin.", path_pandemia.name, len(agg_pandemia))

    # 4. Pandemia agregada só Brasil (sem segregação por região)
    path_pandemia_br = out_dir / "agg_pandemia_nacional.parquet"
    agg_pandemia_nacional = (
        df.groupby("PERIODO_PANDEMIA").size().reset_index(name="TOTAL")
    )
    agg_pandemia_nacional.to_parquet(path_pandemia_br)
    logger.info(
        "Salvo %s: %s lin.",
        path_pandemia_br.name,
        len(agg_pandemia_nacional),
    )

    logger.info(
        "Todas as agregações salvas em %s com sucesso.", out_dir.resolve()
    )


def main() -> None:
    """Pipeline principal de agregação."""
    base_dir = Path(__file__).resolve().parents[2]
    input_file = base_dir / "data" / "raw" / "renais_cleaned.parquet"
    interim_dir = base_dir / "data" / "interim"

    interim_dir.mkdir(parents=True, exist_ok=True)

    if not input_file.exists():
        logger.error("Arquivo não encontrado: %s", input_file)
        logger.error("Execute o 01_cleaning_cols.py primeiro.")
        return

    create_aggregations(input_file, interim_dir)


if __name__ == "__main__":
    main()

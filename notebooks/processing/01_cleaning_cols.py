import logging
from pathlib import Path

import pandas as pd

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Escopo alinhado ao 00_get_data.py (Sul + Sudeste)
_UFS_ESCOPO = frozenset({"PR", "RS", "SC", "SP", "MG", "RJ", "ES"})
_SIGLA_PARA_REGIAO = {
    "PR": "Sul",
    "RS": "Sul",
    "SC": "Sul",
    "SP": "Sudeste",
    "MG": "Sudeste",
    "RJ": "Sudeste",
    "ES": "Sudeste",
}


def _export_csv_xlsx(df: pd.DataFrame, parquet_path: Path) -> None:
    """Gera `mesmo_nome.csv` e `mesmo_nome.xlsx` ao lado do parquet.

    O XLSX respeita o limite do Excel (~1,04 M linhas); bases maiores
    falham — use Parquet/CSV.
    """
    base = parquet_path.with_suffix("")
    csv_path = base.with_suffix(".csv")
    xlsx_path = base.with_suffix(".xlsx")
    # Excel não lida bem com Categorical em alguns cenários
    out = df.copy()
    if "FAIXA_ETARIA" in out.columns:
        out["FAIXA_ETARIA"] = (
            out["FAIXA_ETARIA"].astype(str).replace("nan", "")
        )
    out.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info("CSV: %s", csv_path)
    out.to_excel(xlsx_path, index=False, engine="openpyxl")
    logger.info("XLSX: %s", xlsx_path)


def clean_data(input_path: Path, output_path: Path) -> None:
    """Limpa e formata os dados renais extraídos.

    Args:
        input_path: Caminho do arquivo bruto (renais.parquet).
        output_path: Caminho de destino (renais_cleaned.parquet).
    """
    logger.info("Lendo dados de %s", input_path)
    df = pd.read_parquet(input_path)

    if "SIGLA_UF" not in df.columns:
        logger.error(
            "Coluna SIGLA_UF ausente. Reconsolide com "
            "notebooks/processing/00_get_data.py (usa os batches)."
        )
        return

    n_antes = len(df)
    df = df[df["SIGLA_UF"].isin(_UFS_ESCOPO)].copy()
    logger.info(
        "Escopo Sul+Sudeste: %s registros (excluídos %s fora do escopo)",
        len(df),
        n_antes - len(df),
    )

    # 1. Conversão de Datas
    logger.info("Convertendo datas...")
    df["DT_INTER"] = pd.to_datetime(
        df["DT_INTER"], format="%Y%m%d", errors="coerce"
    )
    df["DT_SAIDA"] = pd.to_datetime(
        df["DT_SAIDA"], format="%Y%m%d", errors="coerce"
    )

    # 2. Tradução de Raça/Cor
    logger.info("Mapeando demografia...")
    dic_raca = {
        "01": "Branca",
        "02": "Preta",
        "03": "Parda",
        "04": "Amarela",
        "05": "Indígena",
        "99": "Sem Informação",
    }
    df["RACA_COR"] = df["RACA_COR"].map(dic_raca).fillna("Não Preenchido")

    # 3. Região a partir da sigla da UF (UF_ZI no SIH não é macro-região)
    df["REGIAO"] = df["SIGLA_UF"].map(_SIGLA_PARA_REGIAO)
    if df["REGIAO"].isna().any():
        n = int(df["REGIAO"].isna().sum())
        logger.warning("REGIAO indefinida em %s linhas; removendo.", n)
        df = df.dropna(subset=["REGIAO"])

    # 4. Idade e Faixa Etária
    logger.info("Calculando faixas etárias...")
    df["IDADE"] = pd.to_numeric(df["IDADE"], errors="coerce")

    cortes = [0, 18, 40, 60, 120]
    rotulos = ["0-17 anos", "18-39 anos", "40-59 anos", "60+ anos"]
    df["FAIXA_ETARIA"] = pd.cut(
        df["IDADE"], bins=cortes, labels=rotulos, right=False
    )

    # 5. Renomear Colunas
    logger.info("Renomeando colunas...")
    df = df.rename(
        columns={
            "DT_INTER": "DATA_INTERNACAO",
            "DT_SAIDA": "DATA_ALTA",
            "UF_ZI": "COD_ESTADO",
            "DIAG_PRINC": "CID_RENAL",
        }
    )

    # 6. Salvar Resultado (raw + processed)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path)
    logger.info("Dados limpos salvos em: %s", output_path)
    _export_csv_xlsx(df, output_path)

    proc_dir = output_path.parents[1] / "processed"
    proc_dir.mkdir(parents=True, exist_ok=True)
    proc_file = proc_dir / output_path.name
    df.to_parquet(proc_file)
    logger.info("Cópia em: %s", proc_file)
    _export_csv_xlsx(df, proc_file)

    logger.info("Total de registros processados: %s", len(df))


def main() -> None:
    """Pipeline principal de limpeza."""
    # Resolução de caminhos dinâmica (a partir de notebooks/processing)
    base_dir = Path(__file__).resolve().parents[2]

    input_file = base_dir / "data" / "raw" / "renais.parquet"
    output_file = base_dir / "data" / "raw" / "renais_cleaned.parquet"

    if not input_file.exists():
        logger.error("Arquivo não encontrado: %s", input_file)
        logger.error("Execute o 00_get_data.py primeiro.")
        return

    clean_data(input_file, output_file)


if __name__ == "__main__":
    main()

import gc
import logging
from pathlib import Path

import pandas as pd
from pysus import SIH

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Nome do lote: {UF}_{ANO}_{MES}.parquet
_MIN_PARTS_NOME_LOTE = 3


def process_batch(  # noqa: PLR0913, PLR0917
    sih: SIH,
    uf: str,
    year: int,
    month: int,
    cids: tuple[str, ...],
    cols: list[str],
    out_dir: Path,
) -> None:
    """Faz o download e filtra um lote específico do SIH.

    Args:
        sih: Instância do catálogo SIH.
        uf: Sigla do estado.
        year: Ano da extração.
        month: Mês da extração.
        cids: Tupla com CIDs para filtro.
        cols: Lista de colunas para manter na memória.
        out_dir: Diretório para salvar o lote.
    """
    batch_file = out_dir / f"{uf}_{year}_{month:02d}.parquet"

    if batch_file.exists():
        logger.info(f"Lote {batch_file.name} já existe. Pulando.")
        return

    try:
        logger.info(f"Buscando {uf} - {year}/{month:02d}")
        files = sih.get_files("RD", uf=uf, year=year, month=month)

        if not files:
            return

        downloaded = sih.download(files)
        if not isinstance(downloaded, list):
            downloaded = [downloaded]

        dfs: list[pd.DataFrame] = []

        for pq_file in downloaded:
            df = pq_file.to_dataframe()

            valid_cols = [c for c in cols if c in df.columns]
            df = df[valid_cols]

            if "DIAG_PRINC" in df.columns:
                mask = df["DIAG_PRINC"].str.startswith(cids, na=False)
                dfs.append(df[mask].copy())

        if dfs:
            df_final = pd.concat(dfs, ignore_index=True)
            if not df_final.empty:
                # UF do arquivo (SIH RD: UF_ZI não é macro-região)
                df_final["SIGLA_UF"] = uf
                df_final.astype(str).to_parquet(batch_file)
                msg = f"Salvo {batch_file.name}: {len(df_final)} lin."
                logger.info(msg)
            del df_final

        del downloaded, dfs, df
        gc.collect()

    except Exception as e:
        logger.error(f"Erro no lote {uf} {year}/{month:02d}: {e}")


def main() -> None:  # noqa: PLR0914
    """Consolida lotes SIH-RD (escopo Sul + Sudeste)."""
    ufs = [
        "PR",
        "RS",
        "SC",
        "SP",
        "MG",
        "RJ",
        "ES",
    ]
    years = list(range(2016, 2024))
    months = list(range(1, 13))
    cids = ("N17", "N18", "N19")

    cols = [
        "DT_INTER",
        "DT_SAIDA",
        "SEXO",
        "RACA_COR",
        "IDADE",
        "UF_ZI",
        "DIAG_PRINC",
    ]

    base_dir = Path(__file__).resolve().parents[2]
    raw_dir = base_dir / "data" / "raw"
    batch_dir = raw_dir / "batches"
    final_file = raw_dir / "renais.parquet"

    batch_dir.mkdir(parents=True, exist_ok=True)
    sih = SIH().load()

    for uf in ufs:
        for year in years:
            for month in months:
                process_batch(sih, uf, year, month, cids, cols, batch_dir)

    logger.info("Consolidando lotes em %s ...", final_file.name)
    batch_files = sorted(batch_dir.glob("*.parquet"))
    frames: list[pd.DataFrame] = []
    lotes_sem_sigla = 0

    for f in batch_files:
        df_part = pd.read_parquet(f)
        if "SIGLA_UF" not in df_part.columns:
            parts = f.stem.split("_")
            if len(parts) < _MIN_PARTS_NOME_LOTE:
                logger.warning("Nome de lote inesperado, pulando: %s", f.name)
                continue
            df_part = df_part.copy()
            df_part["SIGLA_UF"] = parts[0]
            lotes_sem_sigla += 1
        frames.append(df_part)

    if lotes_sem_sigla:
        logger.info(
            "SIGLA_UF preenchida a partir do nome do arquivo em %s lotes.",
            lotes_sem_sigla,
        )

    if frames:
        df_full = pd.concat(frames, ignore_index=True)
        df_full.to_parquet(final_file)
        logger.info("Finalizado: %s (%s registros)", final_file, len(df_full))


if __name__ == "__main__":
    main()

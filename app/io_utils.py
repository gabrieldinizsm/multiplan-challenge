from __future__ import annotations

from pathlib import Path

import polars as pl


def ensure_output_dir_exists(path: Path) -> None:
    """
    Assegura que determinado diretório existe, caso não, o cria.

    Args:
        path (Path): Path do diretório.
    """
    path.mkdir(parents=True, exist_ok=True)


def write_dataframe_output(
        df: pl.DataFrame,
        path: Path,
        format: str = 'json') -> None:
    """
    Escreve o DataFrame para o path desejado no formato desejado.

    Args:
        df (pl.DataFrame): DataFrame a ser escrito.
        path (Path): Path do local e nome a ser salvo.
        format (str): Formato do arquivo, um dos três (csv, json e parquet).
    """
    if format == 'csv':
        df.write_csv(path)
    elif format == 'json':
        df.write_json(path)
    elif format == 'parquet':
        df.write_parquet(path)
    else:
        raise ValueError(f'Formato não suportado: {format}')

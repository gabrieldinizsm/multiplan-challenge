from pathlib import Path
import polars as pl

def ensure_output_dir_exists(path: Path) -> None:
    """
    Assegura que determinado diretório existe, caso não, o cria.

    Args:
        path (Path): Path do diretório.
    """
    path.mkdir(parents=True, exist_ok=True)



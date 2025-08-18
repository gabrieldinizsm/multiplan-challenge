import polars as pl
import hashlib


def filter_top_n_by_response_time(
    df: pl.DataFrame, 
    target_referrer: str, 
    top_n: int = 10
) -> pl.DataFrame:
    """
    Realiza a filtragem das top N linhas, com base no maior responseTime e
    as seguintes condições:

    request inicia com GET /manual/;
    status code = 200 (sucesso); 
    referrerHeade = target_referrer

    Args:
        df (pl.DataFrame): DataFrame de input a ser filtrado.
        target_referrer (str): Qual referrer filtrar.
        top_n (int): Número de elementos a ser retornado.

    Returns:
        pl.DataFrame: DataFrame contendo as N linhas com maior responseTime de determinado referrer.
    """
    return (
        df.filter(
            pl.col("request").str.starts_with("GET /manual/"),
            pl.col("statusCode") == 200,
            pl.col("referrerHeader") == target_referrer,
        )
        .sort("responseTime", descending=True)
        .head(top_n)
    )


def aggregate_requests_per_day(df: pl.DataFrame) -> pl.DataFrame:
    """
    Realiza a contagem de requests por dia.

    Args:
        df (pl.DataFrame): DataFrame de input contendo os dados de requests.

    Returns:
        pl.DataFrame: Um DataFrame agregado, contendo o total de requests por dia.
    """
    return (
        df.with_columns(pl.col("httpTimestamp").dt.truncate("1d").cast(pl.Date).alias("date"))
          .group_by("date")
          .agg(pl.len().alias("count"))
          .sort("count", descending=True)
    )

def get_last_request_by_ip(df: pl.DataFrame) -> pl.DataFrame:
    """
    Busca a última data de request para cada IP único.

    Args:
        df (pl.DataFrame): DataFrame de input contendo os dados de requests.

    Returns:
        pl.DataFrame: DataFrame contendo listas de IP e suas respectivas últimas datas de requisição.
    """
    return (
        df.group_by("remoteHost")
          .agg(pl.max("httpTimestamp").alias("lastRequest"))
    )

def md5_hash(string: str) -> str:
    """
    Realiza o hash md5 em uma string.

    Args:
        string (str): String a sofrer o hash.

    Returns:
        str: String hasheada com md5.
    """
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def add_extra_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Adiciona as colunas extras desejadas na terceira questão.
    Primeira coluna extra: Timestamp no formato UNIX %Y-%m-%d %H:%M:%S.
    Segunda coluna extra: IP hasheado MD5.

    Args:
        df (pl.DataFrame): DataFrame de input contendo os dados de requests.

    Returns:
        pl.DataFrame: DataFrame com as colunas extras.
    """
    return df.with_columns([
        pl.col("httpTimestamp").dt.strftime("%Y-%m-%d %H:%M:%S").alias("httpTimestampUnixStyle"),
        pl.col("remoteHost").map_elements(md5_hash, return_dtype=str).alias("remoteHostMd5Hashed")
    ])
import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as F

from typing import List, Tuple
from pyspark.sql import DataFrame


def convert_dataframe_to_str(df) -> Tuple[pd.DataFrame, List[str]]:
    new_df = df.copy()
    dt_columns = []
    for col in new_df.columns:
        if str(new_df[col].dtype).startswith("datetime64"):
            new_df[col] = new_df[col].astype(str)
            dt_columns.append(col)

    return new_df, dt_columns


def convert_pd_dataframe_to_spark_dataframe(pdf: pd.DataFrame, index_col="index") -> DataFrame:
    """ convert dataframe of pandas into the one of spark because of pandas dataframe conveting error via pyspark.pandas.from_pandas(...)

    Args:
        pdf (pd.Dataframe): _description_
        index_col (str, optional): _description_. Defaults to "index".

    Returns:
        DataFrame: _description_
    """

    new_pdf, dt_cols = convert_dataframe_to_str(pdf)

    sdf = ps.DataFrame(new_pdf).to_spark(index_col=index_col)

    for dt_col in dt_cols:
        sdf = sdf.withColumn(dt_col, F.to_timestamp(dt_col))

    return sdf


def convert_pandas_datetime64_micro_seconds(pdf: pd.DataFrame) -> pd.DataFrame:
    for col in pdf.columns:
        if str(pdf[col].dtype).startswith("datetime64"):
            pdf[col] = pdf[col].astype("datetime64[us]")
    return pdf

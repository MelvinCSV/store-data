from store_data import Base
from store_data import conf
from store_data import Util

import shutil

import datetime as dt
import numpy as np

import polars as pl

folder = conf.f_test_data
shutil.rmtree(folder)
if not folder.exists():
    folder.mkdir()

schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("volume", pl.Float64)]
schema_encode = [("eliotCode", pl.UInt16), ("date", pl.UInt16)]


class DataStockHistoEncoded(Base):
    """"""

    def __init__(self, schema, schema_encode):
        """"""
        super().__init__(folder / "encoded", schema, schema_encode)

    def read(self, eliot_codes=None, dates=None):
        """"""
        fpath = self.fpath_data

        if not fpath.is_file():
            df = Util.empty_df(self.schema)
            return df

        df = pl.read_parquet(fpath)

        dic_col_values = {"eliotCode": eliot_codes, "date": dates}
        df = self.decode_filter(df, dic_col_values)

        if eliot_codes is not None:
            mask = pl.col("eliotCode").is_in(eliot_codes)
            df = df.filter(mask)
        if dates is not None:
            mask = pl.col("date").is_in(dates)
            df = df.filter(mask)

        return df

    def save(self, df):
        """"""
        fpath = self.fpath_data
        df_to_save = self.encode(df)
        df_to_save.write_parquet(fpath)


d_encoded = DataStockHistoEncoded(schema, schema_encode)

schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("volume", pl.Float64)]
schema_encode = []


class DataStockHisto(Base):
    """"""

    def __init__(self, schema, schema_encode):
        """"""
        super().__init__(folder / "not-encoded", schema, schema_encode)

    def read(self, eliot_codes=None, dates=None):
        """"""
        fpath = self.fpath_data

        if not fpath.is_file():
            df = Util.empty_df(self.schema)
            return df

        df = pl.read_parquet(fpath)
        df = self.decode(df)

        if eliot_codes is not None:
            mask = pl.col("eliotCode").is_in(eliot_codes)
            df = df.filter(mask)
        if dates is not None:
            mask = pl.col("date").is_in(dates)
            df = df.filter(mask)

        return df

    def save(self, df):
        """"""
        fpath = self.fpath_data
        df_to_save = self.encode(df)
        df_to_save.write_parquet(fpath)


d = DataStockHisto(schema, schema_encode)

# Eliot Codes
n_codes = 50000
eliot_codes = np.random.randint(1000, 1000000000, n_codes)
df_codes = pl.DataFrame({"eliotCode": eliot_codes}).unique()

# Dates
n_dates = 1000
start_date = dt.datetime(2000, 1, 1)
end_date = dt.datetime(2023, 1, 1)
dates = pl.date_range(start_date, end_date, "1d")
dates = dates[:n_dates]

# Data
data = {"eliotCode": eliot_codes, "date": [dates] * len(eliot_codes)}
df = pl.DataFrame(data).explode("date")

# Values
n = len(df)
volumes = np.random.rand(n)
df = pl.concat((df, pl.DataFrame({"volume": volumes})), how="horizontal")
df = Util.apply_schema(df, schema)


d.save(df)
d_encoded.save(df)

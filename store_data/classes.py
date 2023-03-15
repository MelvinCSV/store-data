from store_data import Base
from store_data import conf
from store_data import Util

import polars as pl


class DataStockHistoEncoded(Base):
    """"""

    def __init__(self):
        """"""
        folder = conf.f_test_data
        if not folder.exists():
            folder.mkdir()
        schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("field", pl.Utf8), ("value", pl.Float64)]
        schema_encode = [("eliotCode", pl.UInt32), ("date", pl.UInt16), ("field", pl.UInt8)]
        super().__init__(folder / "encoded", schema, schema_encode)

    def read(self, eliot_codes=None, dates=None, fields=None):
        """"""
        fpath = self.fpath_data

        if not fpath.is_file():
            df = Util.empty_df(self.schema)
            return df

        df = pl.read_parquet(fpath)

        dic_col_values = {"eliotCode": eliot_codes, "date": dates, "field": fields}
        df = self.decode_filter(df, dic_col_values)

        return df

    def save(self, df):
        """"""
        fpath = self.fpath_data
        df_to_save = self.encode(df)
        df_to_save.write_parquet(fpath)


class DataStockHisto(Base):
    """"""

    def __init__(self):
        """"""
        folder = conf.f_test_data
        if not folder.exists():
            folder.mkdir()

        schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("field", pl.Utf8), ("value", pl.Float64)]
        schema_encode = []
        super().__init__(folder / "not-encoded", schema, schema_encode)

    def read(self, eliot_codes=None, dates=None, fields=None):
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
        if fields is not None:
            mask = pl.col("field").is_in(fields)
            df = df.filter(mask)

        return df

    def save(self, df):
        """"""
        fpath = self.fpath_data
        df_to_save = self.encode(df)
        df_to_save.write_parquet(fpath)

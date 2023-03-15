from pathlib import Path

import polars as pl

from .encoder import Encoder
from .util import Util


class Base:
    """"""

    def __init__(self, folder, schema, schema_encode):
        """"""
        self.folder = Path(folder)

        if not self.folder.exists():
            self.folder.mkdir()

        self.fn_data = "data.parquet"
        self.fn_data_new = "data_{}.parquet"

        self.schema = schema
        self.schema_encode = schema_encode

        dic_col_type = self.dic_col_type
        self.encoders = {
            col: Encoder(folder, f"data_static_{col}", col, dic_col_type[col], id_type)
            for (col, id_type) in schema_encode
        }

    @property
    def fpath_data(self):
        """"""
        return self.folder / self.fn_data

    @property
    def fpath_data_new(self):
        """"""
        return self.folder / self.fn_data_new

    @property
    def dic_col_type(self):
        """"""
        return {e: f for (e, f) in self.schema}

    @property
    def dic_id_type(self):
        """"""
        return {e: f for (e, f) in self.schema_encode}

    @property
    def schema_encoded(self):
        """"""
        dic_id_type = self.dic_id_type
        schema_stored = [
            (e, dic_id_type[e]) if e in dic_id_type.keys() else (e, f)
            for e, f in self.schema
        ]

        return schema_stored

    def decode(self, df):
        """"""
        for encoder in self.encoders.values():
            df = encoder.decode(df)

        df = Util.apply_schema(df, self.schema)

        return df

    def decode_filter(self, df, dic_col_values):
        """"""
        for col, encoder in self.encoders.items():
            values = dic_col_values.get(col)

            if values is None:
                df = encoder.decode(df)
            else:
                df = encoder.decode_filter(df, values)

        df = Util.apply_schema(df, self.schema)

        return df

    def encode(self, df):
        """"""
        for encoder in self.encoders.values():
            df = encoder.encode(df)

        df = Util.apply_schema(df, self.schema_encoded)

        return df

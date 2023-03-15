from pathlib import Path

import polars as pl

from .util import Util


class Encoder:
    """"""

    def __init__(self, folder, filename, col_name, col_type, id_type=pl.UInt16):
        """"""
        self.folder = Path(folder)
        self.filename = f"{filename}.parquet"
        self.col_name = col_name
        self.col_type = col_type
        self.id_type = id_type

        self.schema = [("id", id_type)] + [(col_name, col_type)]
        # self.df_map = self.read()

    @property
    def fpath(self):
        """"""
        return self.folder / self.filename

    @property
    def df_map(self):
        """"""
        return self.read()

    def read(self):
        """"""
        fpath = self.fpath

        if not fpath.is_file():
            return Util.empty_df(self.schema)

        df_map = pl.read_parquet(self.fpath)

        return df_map

    def save(self, df_map):
        """"""
        df_map = Util.apply_schema(df_map, self.schema)
        df_map.write_parquet(self.fpath)

    def find_missing(self, df_to_encode, df_map):
        """"""
        col = self.col_name
        df_miss = df_to_encode.select(col).unique().join(df_map, on=col, how="anti")

        return df_miss

    def create_codes(self, df_miss, df_map):
        """"""
        # When no values are missing there is nothing to do
        if df_miss.is_empty():
            return df_map

        # Otherwise missing values with new ids
        n = len(df_miss)

        if df_map.is_empty():
            m = 0
        else:
            m = df_map["id"].max() + 1

        col = self.col_name
        ids = list(range(m, m + n))
        values = df_miss[col].to_list()
        data = {"id": ids, col: values}
        df_in = pl.DataFrame(data, schema=self.schema)

        # Then we update mapping dataframe
        df_map = pl.concat((df_map, df_in))

        # Save it
        self.save(df_map)

        # Store it in attributes
        # self.df_map = df_map

        return df_map

    def _encode(self, df_to_encode, df_map):
        """"""
        col = self.col_name
        df_encoded = (
            df_to_encode.join(df_map, on=col)
            .select(pl.exclude(col))
            .rename({"id": col})
        )

        return df_encoded

    def format_df_encoded(self, df_encoded, df_to_encode):
        """"""
        # Apply schema to ensure data types
        col = self.col_name
        schema = [
            (col, self.id_type) if e == col else (e, f)
            for (e, f) in df_to_encode.schema.items()
        ]
        df_encoded = Util.apply_schema(df_encoded, schema)

        return df_encoded

    def encode(self, df_to_encode):
        """"""
        # We get mapping dataframe
        df_map = self.df_map

        # Search for unknown values in df_to_encode
        df_miss = self.find_missing(df_to_encode, df_map)

        # Build new ids in df_map if necesary
        df_map = self.create_codes(df_miss, df_map)

        # Replace column col_name with id
        df_encoded = self._encode(df_to_encode, df_map)

        # Sanity check
        msg = "ERROR initial and encoded dataframe should have same size"
        assert len(df_encoded) == len(df_to_encode), msg

        # Apply schema to ensure data types
        df_encoded = self.format_df_encoded(df_encoded, df_to_encode)

        return df_encoded

    def decode(self, df_to_decode):
        """"""
        # We get mapping dataframe
        df_map = self.df_map

        # We use mapping to decode
        col = self.col_name
        df_decoded = (
            df_to_decode.rename({col: "id"})
            .join(df_map, on="id")
            .select(pl.exclude("id"))
        )

        # Sanity check
        msg = "ERROR initial and decoded dataframe should have same size"
        assert len(df_decoded) == len(df_to_decode), msg

        # Apply schema to ensure data types
        schema = [
            (col, self.col_type) if e == col else (e, f)
            for (e, f) in df_to_decode.schema.items()
        ]
        df_decoded = Util.apply_schema(df_decoded, schema)

        return df_decoded

    def filter(self, df_encoded, values):
        """"""
        # We get mapping dataframe
        df_map = self.df_map

        # We get id corresponding to values
        col_name = self.col_name
        ids = df_map.filter(pl.col(col_name).is_in(values))["id"].unique().to_list()

        # We then filter
        df_filtered = df_encoded.filter(pl.col(col_name).is_in(ids))

        return df_filtered

    def decode_filter(self, df_to_decode, values):
        """"""
        df_filtered = self.filter(df_to_decode, values)
        df_decoded = self.decode(df_filtered)

        return df_decoded

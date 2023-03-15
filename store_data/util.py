import polars as pl


class Util:
    """"""

    def __init__(self):
        """"""
        pass

    @staticmethod
    def empty_df(schema):
        """"""
        return pl.DataFrame(schema=schema)

    @staticmethod
    def apply_schema(df, schema):
        """"""

        if df.is_empty():
            return Util.empty_df(schema)

        exprs = []
        for (col, dtype) in schema:

            if col in df.columns:
                expr = pl.col(col).cast(dtype)
            else:
                expr = pl.lit(None).cast(dtype)
            exprs.append(expr)

        df = df.select(exprs)

        return df

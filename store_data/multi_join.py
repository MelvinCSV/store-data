import ipyparallel as ipp
import polars as pl
import gc


class MultiJoin:
    """"""

    def __init__(self):
        """"""
        pass

    @staticmethod
    def join(df1: pl.DataFrame, df2, batch_size, *args, **kwargs):
        """"""
        dfs = list(df1.iter_slices(batch_size))
        li_params = [(dfi, df2, args, kwargs) for dfi in dfs]

        with ipp.Client(cluster_id="work") as rc:
            view = rc.load_balanced_view()
            async_res = view.map_async(lambda x: MultiJoin._join(*x), li_params)
            rc.wait(async_res)

        df = pl.concat(async_res.get())

        return df

    @staticmethod
    def _join(df1: pl.DataFrame, df2, args, kwargs):
        """"""

        df = df1.join(df2, *args, **kwargs)

        del df1
        del df2
        del args
        del kwargs
        gc.collect()

        return df

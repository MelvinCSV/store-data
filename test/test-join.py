import time

from store_data.memory_profile import MemoryProfile
from store_data.multi_join import MultiJoin
from store_data.util import Util

from guppy import hpy
import gc

import datetime as dt
import numpy as np
import polars as pl


@MemoryProfile.profile(poll_interval=.1, start_sleep=1, end_sleep=1, alpha=1)
def create_df():
    """"""
    # Eliot Codes
    n_codes = 10000
    eliot_codes = create_eliot_codes(n_codes)

    # Dates
    n_dates = 1000
    start_date = dt.datetime(2000, 1, 1)
    end_date = dt.datetime(2023, 1, 1)
    dates = create_dates(start_date, end_date, n_dates)

    # Fields
    fields = ["price", "volume", "factor", "perf", "sentiment"]

    # Data
    data = build_data(eliot_codes, dates, fields)
    df = build_df(data)
    time.sleep(1)

    # Values
    n = len(df)
    values = create_values(n)
    time.sleep(1)

    df = add_values(df, values)
    time.sleep(1)

    # Remove duplicates
    df = remove_duplicates(df)
    time.sleep(1)

    # Format for data types
    df = format_df(df)
    time.sleep(1)

    # # try partition by
    # dfs = partition_by(df)
    # time.sleep(1)

    # try simple join
    batch_size = 50
    n_batch = (len(eliot_codes) - 1) // batch_size + 1
    batches = [eliot_codes[k*batch_size:(k+1)*batch_size] for k in range(n_batch)]
    data_batch = {"eliotCode": batches, "batch": list(range(n_batch))}
    df_batch = pl.DataFrame(data_batch).explode("eliotCode")
    simple_join(df, df_batch)

    # try mutli join
    init_client()
    time.sleep(1)
    multi_join(df, df_batch)

    # Keep track of one eliotCode
    eliot_code = eliot_codes[0]

    # Delete useless data
    print("START release memory")
    time.sleep(1)
    del eliot_codes
    del dates
    del fields
    del values
    del data

    gc.collect()
    time.sleep(1)
    print("END release memory")

    # Dataframe estimate size
    show_estimated_size(df)

    # Store df in local disk
    store_df(df)

    # Delete useless data
    print("START release memory")
    time.sleep(1)
    del df

    gc.collect()
    time.sleep(1)
    print("END release memory")

    # # Query first line
    # query_1()
    #
    # # Query first line with collect
    # query_2()
    #
    # # Query filter one eliot_code (5,000 rows)
    # query_3(eliot_code)
    #
    # print(hpy().heap())


def create_eliot_codes(n_codes):
    """"""
    eliot_codes = np.random.randint(1000, 1000000000, n_codes)
    eliot_codes = list(set(eliot_codes))
    eliot_codes = convert_eliot_codes(eliot_codes)

    return eliot_codes


def convert_eliot_codes(eliot_codes):
    """"""
    eliot_codes = list(map(str, eliot_codes))

    return eliot_codes


def create_dates(start_date, end_date, n_dates):
    """"""
    dates = pl.date_range(start_date, end_date, "1d")
    dates = dates[:n_dates]

    return dates


def build_data(eliot_codes, dates, fields):
    """"""
    data = {
        "eliotCode": eliot_codes,
        "date": [dates] * len(eliot_codes),
        "field": [fields] * len(eliot_codes)
    }

    return data


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def build_df(data):
    """"""
    df = init_df(data)
    df = explode_date(df)
    df = explode_field(df)

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def init_df(data):
    """"""
    df = pl.DataFrame(data)

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def explode_date(df):
    """"""
    df = df.explode("date")

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def explode_field(df):
    """"""
    df = df.explode("field")

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def create_values(n):
    """"""
    values = np.random.rand(n)

    return values


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def add_values(df, values):
    """"""
    df = pl.concat((df, pl.DataFrame({"value": values})), how="horizontal")

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def remove_duplicates(df):
    """"""
    df = df.unique(subset=["eliotCode", "date", "field"])

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def format_df(df):
    """"""
    schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("field", pl.Utf8), ("value", pl.Float64)]
    df = Util.apply_schema(df, schema)

    return df


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def partition_by(df):
    """"""
    return df.partition_by("eliotCode")


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def simple_join(df, df_batch):
    """"""
    return df.join(df_batch, on="eliotCode")


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def init_client():
    """"""
    import ipyparallel as ipp
    with ipp.Client(cluster_id="work") as rc:
        dv = rc[:]
        dv.execute("from store_data.multi_join import µ")


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def multi_join(df, df_batch):
    """"""
    return MultiJoin.join(df, df_batch, batch_size=1000000, on="eliotCode")


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def show_estimated_size(df):
    """"""
    print(f"Estimated size: {df.estimated_size() / 2 ** 20:,.0f} MiB")


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def store_df(df):
    """"""
    from store_data.conf import conf as c
    from pathlib import Path
    folder = Path(c.f_test_data)
    filename = folder / "data.parquet"
    df.write_parquet(filename)


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def query_1():
    """"""
    from store_data.conf import conf as c
    from pathlib import Path
    folder = Path(c.f_test_data)
    filename = folder / "data.parquet"
    df = pl.scan_parquet(filename)

    return df.fetch(1)


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def query_2():
    """"""
    from store_data.conf import conf as c
    from pathlib import Path
    folder = Path(c.f_test_data)
    filename = folder / "data.parquet"
    df = pl.scan_parquet(filename)

    return df.limit(1).collect()


@MemoryProfile.timer(start_sleep=1, end_sleep=1)
def query_3(eliot_code):
    """"""
    from store_data.conf import conf as c
    from pathlib import Path
    folder = Path(c.f_test_data)
    filename = folder / "data.parquet"
    df = pl.scan_parquet(filename)
    mask = pl.col("eliotCode") == eliot_code

    return df.filter(mask).collect()


def main():
    """"""
    create_df()


if __name__ == "__main__":
    main()

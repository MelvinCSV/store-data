from store_data.conf import conf as c
from store_data.classes import DataStockHisto, DataStockHistoEncoded
from store_data.util import Util

import gc
import shutil

import datetime as dt
import numpy as np
import polars as pl

try:
    shutil.rmtree(c.f_test_data)
except Exception:
    pass

# Eliot Codes
n_codes = 10000
eliot_codes = np.random.randint(1000, 1000000000, n_codes)

# Dates
n_dates = 1000
start_date = dt.datetime(2000, 1, 1)
end_date = dt.datetime(2023, 1, 1)
dates = pl.date_range(start_date, end_date, "1d")
dates = dates[:n_dates]

# Fields
fields = ["price", "volume", "factor", "perf", "sentiment"]

# Data
data = {"eliotCode": eliot_codes, "date": [dates] * len(eliot_codes), "field": [fields] * len(eliot_codes)}
df = pl.DataFrame(data).explode("date").explode("field")

# Values
n = len(df)
values = np.random.rand(n)
df = pl.concat((df, pl.DataFrame({"value": values})), how="horizontal")

schema = [("eliotCode", pl.Utf8), ("date", pl.Datetime), ("field", pl.Utf8), ("value", pl.Float64)]
df = df.unique(subset=["eliotCode", "date", "field"])
df = Util.apply_schema(df, schema)

# Iniatialise data classes
data = DataStockHisto()
data_encoded = DataStockHistoEncoded()

# Store data
data.save(df)
data_encoded.save(df)

del df
gc.collect()

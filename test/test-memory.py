from store_data.classes import DataStockHisto, DataStockHistoEncoded
from store_data.memory_profile import MemoryProfile


@MemoryProfile.profile(poll_interval=.1, start_sleep=1, end_sleep=1)
def data_stock_histo_encoded():
    """"""
    return DataStockHistoEncoded()


# Iniatialise data classes
data = DataStockHisto()
data_encoded = data_stock_histo_encoded()

# Find eliot_codes
eliot_codes = data_encoded.encoders["eliotCode"].df_map["eliotCode"].to_list()


@MemoryProfile.profile(poll_interval=.1, start_sleep=1, end_sleep=1, alpha=.975)
def read_data1():
    """"""
    res = data_encoded.read(eliot_codes=list(map(str, eliot_codes[:100])))
    return res


@MemoryProfile.profile(poll_interval=.1, start_sleep=1, end_sleep=1, alpha=.975)
def read_data2():
    """"""
    res = data.read(eliot_codes=list(map(str, eliot_codes[:100])))
    return res


df1 = read_data1().to_pandas()
df2 = read_data2().to_pandas()

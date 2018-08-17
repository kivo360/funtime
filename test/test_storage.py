import time

from funtime import Store, Converter



store = Store().create_lib("hello.World").get_store()

# Insert dogshit
store['hello.World'].store({
    "type": "price",
    "currency": "ETH_USD",
    "timestamp": time.time(),
    "open": 1234,
    "close": 1234.41,
    "other": "etc",
    "exchange": "binance",
    "period": "minute"
})

runs = store['hello.World'].query_latest({
    "type": "price",
    "exchange": "binance",
    "period": "minute"
})
# runs = store['hello.World'].query_time(time_type="before", start=time.time(), query_type="price")



# returning some shit
print(Converter.to_dataframe(runs))


def create_library():
    pass

def store_items():
    pass

def query_item():
    pass

def access_item():
    pass
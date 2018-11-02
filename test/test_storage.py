import time
from copy import deepcopy
from funtime import Store, Converter



store = Store('localhost').create_lib("hello.World").get_store()
hworld = store['hello.World']

def test_single():
    only_time = time.time()
    for i in range(5):
        hworld.store({
            "type": "price",
            "currency": "ETH_USD",
            "timestamp": only_time,
            "open": 1234,
            "close": 1234.41,
            "other": "etc",
            "exchange": "binance",
            "period": "minute"
        })

    single_record = hworld.query({"type": "price", "timestamp": only_time, "exchange": "binance", "period": "minute"})
    record_frame = Converter.to_dataframe(single_record)
    print(record_frame)
    
    assert record_frame['type'].count() == 1


def create_library():
    pass

def store_items():
    pass

def query_latest():
    #only_time = 
    for i in range(5):
        hworld.store({
            "type": "price",
            "currency": "ETH_USD",
            "timestamp": time.time(),
            "open": 1234,
            "close": 1234.41,
            "other": "etc",
            "exchange": "binance",
            "period": "minute"
        })

    latest = hworld.query_latest({"type": "price", "exchange": "binance", "period": "minute"})
    print(list(latest))
    

def access_item():
    pass

if __name__ == "__main__":
    test_single()
    query_latest()
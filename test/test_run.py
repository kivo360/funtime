import time
from copy import deepcopy
from funtime import Store, Converter


store = Store('localhost').create_lib("global").get_store()
hworld = store['global']


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

    single_record = hworld.query(
        {"type": "price", "timestamp": only_time, "exchange": "binance", "period": "minute"})
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

    latest = hworld.query_latest(
        {"type": "price", "exchange": "binance", "period": "minute"})
    print(list(latest))


def access_item():
    pass


def sequencial_add(mins):
    current_time = round(time.time(), 0)
    for i in range(mins):
        hworld.store({
            "type": "test_add",
            "timestamp": current_time - (i*60),
            "extra": "whore",
            "period": "minute"
        })


def sequencial_get(mins):
    base = {"type": "test_bulk_add", "base": "BTC", "trade": "USDT"}
    bl = []
    current_time = round(time.time(), 0)
    for i in range(mins):
        b = deepcopy(base)
        b['timestamp'] = current_time - (i*60.0)
        bl.append(b)
    
    return bl

if __name__ == "__main__":
    sequence = sequencial_get(10000)
    print(len(sequence))
    hworld.bulk_upsert(sequence, _column_first=["type", "base", "trade", "exchange"])
    # print(sequence[1])
    # current_time = round(time.time(), 0)
    # print(current_time - (44*60))
    close = hworld.query_closest(
        sequence[1]
    )
    # dc = deepcopy(close)
    # dc['shit'] = "dick"

    # print(close)

    
    

    # print(close)
# Social network closer. I can add pagination to my calls now instantly. As long as the api updates everything

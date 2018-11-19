# Funtime - Funguana's Time-series database library for your trading bot

`Funtime` is a mongodb based time series library. It was created because we ran into the problem of inputting data that may have its schema change and querying it quickly.

We found `arctic` to be a good library, yet it lacked straightforward pythonic querying methods. We added a layer on top for our own purposes.

**Both `funtime` and `arctic` use mongodb as the main database**


## What makes `Funtime` better?
The single thing that makes funtime better for timeseries information is that it has more clear querying methods. It should be noted that it still has arctic as the foundation. Meaning the speed of it is extremely fast. This works a lot like tickstore, yet it's easier to get your hands on and use than tickstore

It is a layer on time of arctic. We added the following:

* An easy way to add and filter data with a given timestamp/datetime
* Easy conversions to both a `pandas` and `dask` dataframe
* A choice to get information by a filtration type. This allows the user to be highly flexible with how they want to get information
    * `Window` - A window query is a query that gets information two dates. They must be valid.
    * `Before` - Gets every record before a certain time
    * `After` - Gets every record after a certain time


## How does it work?
All forms of time data is stored as a epoch timestamp. This is to make querying easier and faster than if we were to use a full datetime object. We do conversions within the library from datetime objects into epoch timestamps.

This increases the insert time, yet reduces the querying time. Numbers are easier to filter than date objects. The biggetst tradeoff is in ease of use.

### Example:
---
```python
from funtime import Store, Converter
import mimesis # this is used to seed data for our test
import time

# Create a library and access the store you want
store = Store('localhost').create_lib("hello.World").get_store()

# store the data with a timestamp
store['hello.World'].store({
    "type": "price",
    "currency": "ETH_USD",
    "timestamp": time.time(),
    "candlestick": {
        "open": 1234,
        "close": 1234.41,
        "other": "etc"
    }
})


# Query general information. It returns a generator
runs = store['hello.World'].query({
    "type": "price"
})

# Check for results
for r in runs:
    print(r)


# Even get information with complex time queries
runs2 = store['hello.World'].query_time(time_type="before", start=time.time(), query_type="price")


# Check for results
for r in runs:
    print(r)

```

### To change default store and host
```python
from funtime import set_library_type, set_mongo_host


# Run before you run everything else
set_library_type()

# Run before you run everything else
set_mongo_host()
```


## Using the Pandas/Dask converter

As a data scientist, you may want to handle your data in dataframe format. With `funtime`, you can get your timestamp information in both `pandas.DataFrame` and `dask.DataFrame` format. You would use the `Converter` import. 

```python
from funtime import Store, Converter


runs = store['hello.World'].query({
    "type": "price"
})

# if you want a pandas object
df = Converter.to_dataframe(runs)

# If you want to do parallel processing within dask
ddf = Converter.to_dataframe(runs, "dask")
```


## How to install

Make sure to install mongodb at the very beginning. The instructions are different for different operating systems. Then run:

```
pip install funtime
```

Or you can use `pipenv` for it:

```
pipenv install funtime
```

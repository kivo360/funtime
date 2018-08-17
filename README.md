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
import funtime
import mimesis # this is used to seed data for our test


# access new library

def seed_dict():
    """ Returns a dict that will seed the database"""
    pass

# store data with a datetime
if 'mongo_host' not in globals():
    mongo_host = 'localhost'
store = Arctic(mongo_host)
# store.delete_library('twitter.tweets')

### Initialize the library
try:
    register_library_type(TwitterStore._LIBRARY_TYPE, TwitterStore)
except Exception as e:
    print(str(e))
# store.list_libraries()
store.initialize_library('ttwitter.tweets', TwitterStore._LIBRARY_TYPE)


lib = store['ttwitter.tweets']
# store the data with a timestamp

# query data with a timestamp
# query data with a set of datetimes
# get query as a pandas object
# get query as a dask object
```
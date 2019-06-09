from datetime import datetime as dt
from bson.binary import Binary
from six.moves import cPickle    
from arctic import Arctic, register_library_type
from arctic.decorators import mongo_retry

import maya
import pymongo
from pymongo.errors import BulkWriteError
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from datetime import datetime as dt
import time
from abc import ABCMeta, abstractmethod

from funtime.util.timing import TimeHandler
from funtime.config import LIBRARYTYPE
import funtime.util.ticker_handler as th
from copy import deepcopy

class DataStoreBase(metaclass=ABCMeta):
    def __init__(self):
        pass
    
    @abstractmethod
    def store(self):
        pass
    
    @abstractmethod
    def query(self):
        pass
    
    @abstractmethod
    def stats(self):
        pass
    
    @abstractmethod
    def delete(self):
        pass




class FunStore(DataStoreBase):
    # NOTE: I really want to add dask into this
    # It would make a ton of sense for making
    # It's irrelavant however for the task of analyzing software. Move to NetworkX
    _LIBRARY_TYPE = LIBRARYTYPE
    
    def __init__(self, arctic_lib):
        self._arctic_lib = arctic_lib
        self._collection = arctic_lib.get_top_level_collection()
  
    def _ensure_index(self):
        """
        Make sure the field itself is available
        """
        collection = self._collection
        # collection.add_indexes
        collection.create_index('type')
    
    @classmethod
    def initialize_library(cls, arctic_lib, **kwargs):
        FunStore(arctic_lib)._ensure_index()

    def bulk_upsert(self, items, _column_first=[], _in=['timestamp']):
        deepitems = deepcopy(items)
        assert isinstance(items, list)
        assert isinstance(_column_first, list)
        assert isinstance(_in, list)

        if len(deepitems) == 0:
            raise IndexError("Items should be more than zero")
        
        first_column_search = {}
        first_item = dict(deepitems[0])
        for name in _column_first:
            column = first_item.get(name)
            if column is None:
                continue
            first_column_search[name] = column
        

        
        if len(_in) > 0:
            for __in in _in:
                temp = {
                    f"{__in}": {"$in": [x[__in] for x in deepitems]}
                }
                first_column_search = {**first_column_search, **temp}
        

        self._collection.delete_many(first_column_search)
        time.sleep(0.1) # Temporary Hack
        self._collection.insert_many(deepitems)


    @mongo_retry
    def query_latest(self, *args, **kwargs):
        """
            Get the latest 
        """


        # print(args)
        qitem = th.merge_dicts(*args)
        # print(qitem)
        # If there a 'price' type, check to see if there's a 'period' as well. 
        # This determines which kind of data we need to get.
        query_type = None


        since = "now" # since 'now' means time.time()
        limit = 500
        # Here we assume that we are getting price information
        qtype = {
            "type": "price"
        }


        try:
            qtype["type"] = qitem.pop("type")
        except Exception:
            # Try looking for it in kwargs
            _type = kwargs.get("_type")
            if _type is not None:
                qtype["type"] = _type
        

        try:
            # Since when
            since = qitem.pop('since')
        except:
            pass
        
        try:
            # Since when
            limit = qitem.pop('limit')
        except:
            pass
        
        if qtype["type"] == "price":
            
            try:
                th.price_query_filter(qitem)
            except KeyError:
                raise AttributeError("When querying price information, couldn't find the required variables. Must have: exchange (to select what exchange the data is coming from) and period (to select the time period of the data)")
        time_query = {}
        if since is not "now":
            time_query = TimeHandler.everything_before(since)
        else:
            time_query = TimeHandler.everything_before(time.time())
        final = th.merge_dicts(qtype, qitem, time_query)
        
        # Determine pagination here.

        pagination = kwargs.get("pagination", False)
        page_size = kwargs.get("page_size", 10)
        page_num = kwargs.get("page_num", 1)
        
        # Gets the page if pagination == true
        if pagination == True:
            # Re return something else here
            skips = page_size * (page_num - 1)
            cursor = self._collection.find(final).sort("timestamp",pymongo.DESCENDING).skip(skips).limit(page_size)
            for x in cursor:
                del x["_id"]
                yield x
        
        # Break the function here
        else:
            for x in self._collection.find(final).sort("timestamp",pymongo.DESCENDING).limit(limit):
                del x['_id'] # Remove default unique '_id' field from doc
                # TODO: Create generic cast
                yield x

    def query_sorted(self, *args, **kwargs):
        """ 
            # Query all of the items sorted
            
            Get all of the items and sort by timestamp. Place the limit if you're interested in increasing to more than 500 records
        """
        qitem = th.merge_dicts(*args)
        limit = kwargs.get("limit", 500)

        for x in self._collection.find(qitem).sort("timestamp",pymongo.DESCENDING).limit(limit):
            del x['_id'] # Remove default unique '_id' field from doc
            # TODO: Create generic cast
            yield x

    @mongo_retry
    def query_last(self, *args, **kwargs):
        """
            Get the latest 
        """

        # print(args)
        qitem = th.merge_dicts(*args)
        # print(qitem)
        # If there a 'price' type, check to see if there's a 'period' as well.
        # This determines which kind of data we need to get.
        query_type = None

        since = "now"  # since 'now' means time.time()
        limit = 500
        # Here we assume that we are getting price information
        qtype = {
            "type": "price"
        }

        try:
            qtype["type"] = qitem.pop("type")
        except Exception:
            # Try looking for it in kwargs
            _type = kwargs.get("_type")
            if _type is not None:
                qtype["type"] = _type

        try:
            # Since when
            since = qitem.pop('since')
        except:
            pass

        try:
            # Since when
            limit = qitem.pop('limit')
        except:
            pass

        if qtype["type"] == "price":

            try:
                th.price_query_filter(qitem)
            except KeyError:
                raise AttributeError(
                    "When querying price information, couldn't find the required variables. Must have: exchange (to select what exchange the data is coming from) and period (to select the time period of the data)")
        time_query = {}
        if since is not "now":
            time_query = TimeHandler.everything_before(since)
        else:
            time_query = TimeHandler.everything_before(time.time())
        final = th.merge_dicts(qtype, qitem, time_query)

        # Determine pagination here.

        pagination = kwargs.get("pagination", False)
        page_size = kwargs.get("page_size", 10)
        page_num = kwargs.get("page_num", 1)
        main_list = []
        # Gets the page if pagination == true
        if pagination == True:
            # Re return something else here
            skips = page_size * (page_num - 1)
            cursor = self._collection.find(final).sort(
                "timestamp", pymongo.DESCENDING).skip(skips).limit(page_size)
            for x in cursor:
                del x["_id"]
                main_list.append(x)

        # Break the function here
        else:
            for x in self._collection.find(final).sort("timestamp", pymongo.DESCENDING).limit(limit):
                del x['_id']  # Remove default unique '_id' field from doc
                # TODO: Create generic cast
                main_list.append(x)
        if len(main_list) == 0:
            return None
        return main_list[0]


    @mongo_retry
    def query(self, *args, **kwargs):
        """
        Generic query method.
        In reality, your storage class would have its own query methods,
        Performs a Mongo find on the Marketdata index metadata collection.
        See:
        http://api.mongodb.org/python/current/api/pymongo/collection.html
        """
        qitem = th.merge_dicts(*args)
        # If there a 'price' type, check to see if there's a 'period' as well. 
        # This determines which kind of data we need to get.
        query_type = qitem.get('type')
        _query_type = kwargs.get("_type")
        if query_type is None and _query_type is None:
            raise AttributeError("Must specify a type. It is a selector to help the user sort through data. Can use either kwargs or dict")

        if query_type is None:
            # This means _query_type is specified instead
            query_type = _query_type

        if query_type == "price":
            try:
                th.price_query_filter(qitem)
            except KeyError:
                raise AttributeError("When querying price information, couldn't find the required variables. Must have: exchange (to select what exchange the data is coming from) and period (to select the time period of the data)")
        
        pagination = kwargs.get("pagination", False)
        page_size = kwargs.get("page_size", 1)
        page_num = kwargs.get("page_num", 10)
        # Gets the page if pagination == true
        if pagination:
            # Re return something else here
            skips = page_size * (page_num - 1)
            cursor = self._collection.find(qitem).skip(skips).limit(page_size)
            for x in cursor:
                del x["_id"]
                yield x
        
        # Break the function here
        else:
            # Get absolute latest
            for x in self._collection.find(qitem):
                del x['_id'] # Remove default unique '_id' field from doc
                # TODO: Create generic cast
                yield x

    @mongo_retry
    def query_time(self, *args, **kwargs):
        # Assume the default variables here
        time_type = 'window'
        start = None
        stop = None
        query_type = None
        time_query = None

        available_ttypes = ["window", "before", "after"]
        qitem = th.merge_dicts(args)

        # Find a better way to handle time manipulations

        # Check for things not required
        # Make sure to check for validity inside as well 
        try:
            time_type = kwargs.pop('time_type')
        except Exception:
            print("time_type not added. Skipping")

        if time_type not in available_ttypes:
            raise AttributeError("time_type should one of the following: window, before, after")

        try:
            start = kwargs.pop('start')
            query_type = kwargs.pop('query_type')
        except KeyError:
            raise KeyError('start and query_type are both required arguments')

        if time_type is "window" and stop is None:
            stop = maya.now().datetime().timestamp()
            
        if time_type is "window":
            time_query = TimeHandler.get_window(start, stop)
        
        if time_type is "before":
            time_query = TimeHandler.everything_before(start)
        
        if time_type is "after":
            time_query = TimeHandler.everything_after(start)

        pre = {
            "type": query_type
        }

        main_query = {**pre, **time_query, **kwargs, **qitem}
        for x in self._collection.find(main_query):
            del x['_id'] # Remove default unique '_id' field from doc
            # TODO: Create generic cast
            yield x
    
    @mongo_retry
    def query_closest(self, query_item):
        """
            closest = store.query_closest({"type": "...", "item_1": "...", "timestamp": "..."}) # returns a list of the closest items to a given thing
        """

        if not isinstance(query_item, dict):
            raise TypeError("The query query_item isn't a dictionary")
        
        _type = query_item.get("type")
        _timestamp = query_item.get("timestamp")

        if _type is None:
            raise AttributeError("Please make sure to add a type to the query_item dict")

        if _timestamp is None:
            raise AttributeError("Timestamp doesn't exist. It's necessary to provide closest query")
        
        query_less = deepcopy(query_item)
        query_more = deepcopy(query_item)
        
        query_less["timestamp"] = {"$lte": _timestamp}
        query_more["timestamp"] = {"$gt": _timestamp}
        closestBelow = self._collection.find(query_less).sort(
            "timestamp", pymongo.DESCENDING).limit(1)
        closestAbove = self._collection.find(query_more).sort("timestamp", pymongo.ASCENDING).limit(1)
        
        combined = list(closestAbove) + list(closestBelow)
        for x in combined:
            del x['_id']
        
        # abs()
        if len(combined) >= 2: 
            if abs(combined[0]["timestamp"] - _timestamp) > abs(combined[1]["timestamp"] - _timestamp):
                return combined[1]
            else:
                return combined[0]
        elif combined == 1:
            return combined[0]
        else:
            return None

    @mongo_retry
    def stats(self):
        """
        Database usage statistics. Used by quota.
        """
        res = {}
        db = self._collection.database
        res['dbstats'] = db.command('dbstats')
        res['data'] = db.command('collstats', self._collection.name)
        res['totals'] = {'count': res['data']['count'], 'size': res['data']['size']}
        return res
    
    @mongo_retry
    def store(self, store_item):
        """
            Store for tweets and user information. Must have all required information and types
        """
        required_keys = {"type": str, "timestamp": float}
        
        if not isinstance(store_item, dict):
            raise TypeError("The stored item should be a dict")
           
        for k, v in required_keys.items(): 
            if k not in store_item:
                raise AttributeError("{} is not available. Please add it.".format(k))
            
            if not isinstance(store_item[k], v):
                raise TypeError("{} is not a {}. Please change it. ".format(k, v))
                #       
#         # TODO: CREATE FILTER FOR PERSISTENCE METHOD. Make sure it has the necessary data
#         to_store = {'field1': thing.field1,
#                     'date_field': thing.date_field,
#                     }
#         to_store['stuff'] = Binary(cPickle.dumps(thing.stuff))
        # Respect any soft-quota on write - raises if stats().totals.size > quota 
        self._arctic_lib.check_quota()
        self._collection.update(store_item, store_item, upsert=True)
        # self._collection.insert_one(store_item)

    @mongo_retry
    def delete(self, query):
        """
        Simple delete method
        """
        self._collection.delete_one(query)
    
    @mongo_retry
    def delete_many(self, query):
        """
        Simple delete method
        """
        self._collection.delete_many(query)


from datetime import datetime as dt
from bson.binary import Binary
from six.moves import cPickle    
from arctic import Arctic, register_library_type
from arctic.decorators import mongo_retry

import maya
from datetime import datetime as dt

from abc import ABCMeta, abstractmethod

from funtime.util.timing import TimeHandler
from funtime.config import LIBRARYTYPE

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

    
    @mongo_retry
    def query(self, *args):
        """
        Generic query method.
        In reality, your storage class would have its own query methods,
        Performs a Mongo find on the Marketdata index metadata collection.
        See:
        http://api.mongodb.org/python/current/api/pymongo/collection.html
        """
        all_dicts = []
        for ag in args:
            if isinstance(ag, dict):
                all_dicts.append(ag)
        
        # Get all dictionaries
        # Merge them
        # Check if type is there

        qitem = { k: v for d in all_dicts for k, v in d.items() }

        try:
            qitem['type']
        except KeyError:
            print('need to specify a type')
            return

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
        main_query = {**pre, **time_query, **kwargs}
        print(main_query)
        for x in self._collection.find(main_query):
            del x['_id'] # Remove default unique '_id' field from doc
            # TODO: Create generic cast
            yield x
    
    @mongo_retry
    def stats(self):
        """
        Database usage statistics. Used by quota.
        """
        res = {}
        db = self._collection.database
        res['dbstats'] = db.command('dbstats')
        res['data'] = db.command('collstats', self._collection.name)
        res['totals'] = {'count': res['data']['count'],
                         'size': res['data']['size']
                         }
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
        self._collection.insert_one(store_item)

    @mongo_retry
    def delete(self, query):
        """
        Simple delete method
        """
        self._collection.delete_one(query)


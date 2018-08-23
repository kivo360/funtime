import types
from arctic import Arctic, register_library_type
from arctic.decorators import mongo_retry
from funtime.util.storage import FunStore
from funtime.config import MONGOHOST
import pandas as pd
import dask.dataframe as dd

class Store:
    def __init__(self):
        """Initializes the store here if it hasn't already z"""
        self.store = Arctic(MONGOHOST)
        try:
            register_library_type(FunStore._LIBRARY_TYPE, FunStore)
        except Exception:
            print("The library type already exist")
        
    def create_lib(self, lib_name):
        try:
            self.store.initialize_library(lib_name, FunStore._LIBRARY_TYPE)
        except Exception:
            print("Silent Fail...")
        
        return self
    
    def get_store(self):
        return self.store


class Converter:
    def __init__(self):
        pass
    
    @classmethod
    def to_dataframe(cls, generlist, ctype="pandas"):
        if isinstance(generlist, types.GeneratorType):
            # print(list(generlist) )
            df = pd.DataFrame(generlist)
            if ctype == "pandas":
                return df
            if ctype == "dask":
                return dd.from_pandas(df, npartitions=1)
        elif isinstance(generlist, list):
            df = pd.DataFrame.from_records(generlist)
            if ctype == "pandas":
                return df
            if ctype == "dask":
                return dd.from_pandas(df, npartitions=1)
        else:
            raise TypeError("You didn't enter eiter a list or generator")
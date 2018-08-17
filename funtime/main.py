from arctic import Arctic, register_library_type
from arctic.decorators import mongo_retry
from funtime.util.storage import FunStore
from funtime.config import MONGOHOST
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

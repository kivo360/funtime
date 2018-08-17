import maya
from datetime import datetime as dt

class TimeHandler(object):
    """
        Use this class to handle the timestamp queries. 
        Use with other stuff to create very deep queries very quickly.
        
        
    """
    def __init__(self):
        pass
    
    @classmethod
    def get_window(self, start, stop, return_type="epoch", field_name="timestamp"):
        # both start and stop should be datetime or a string
        start_maya = None
        start_timestamp = None
        stop_maya = None
        stop_timestamp = None
        
        if isinstance(start, str):
            start_maya = maya.when(start)
            start_timestamp = start_maya.datetime().timestamp()
            
        if isinstance(stop, str):
            stop_maya = maya.when(stop)
            stop_timestamp = stop_maya.datetime().timestamp()
            
        if isinstance(start, float):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
            
            
        if isinstance(stop, float):
            stop_maya = maya.MayaDT(stop)
            stop_timestamp = stop_maya.datetime().timestamp()
            
        if isinstance(start, dt):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
            
            
        if isinstance(stop, dt):
            stop_maya = maya.MayaDT(stop)
            stop_timestamp = stop_maya.datetime().timestamp()    
        
        
        assert (start_timestamp < stop_timestamp), "The start time isn't less than the end time. The end should be closest to the present"
        return {'{}'.format(field_name):{'$lt':stop_timestamp, '$gt': start_timestamp}}
    
    @classmethod
    def everything_before(self, start, return_type="epoch", field_name="timestamp"):
        # both start and stop should be datetime or a string
        start_maya = None
        start_timestamp = None
        
        if isinstance(start, str):
            start_maya = maya.when(start)
            start_timestamp = start_maya.datetime().timestamp()
            
        if isinstance(start, float):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
            
            
        if isinstance(start, dt):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
        
        
        return {'{}'.format(field_name):{'$lt': start_timestamp}}
    
    @classmethod
    def everything_after(self, start, return_type="epoch", field_name="timestamp"):
        # both start and stop should be datetime or a string
        start_maya = None
        start_timestamp = None
        
        if isinstance(start, str):
            start_maya = maya.when(start)
            start_timestamp = start_maya.datetime().timestamp()
            
        if isinstance(start, float):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
            
            
        if isinstance(start, dt):
            start_maya = maya.MayaDT(start)
            start_timestamp = start_maya.datetime().timestamp()
        
        
        return {'{}'.format(field_name):{'$gt': start_timestamp}}
    
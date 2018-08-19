# from decouple import config
MONGOHOST = "localhost"

def set_mongo_host(new_host):
    global MONGOHOST
    MONGOHOST = new_host

LIBRARYTYPE = 'funtime.FunStore'

def set_library_type(new_library_type):
    global LIBRARYTYPE
    LIBRARYTYPE = new_library_type

# The mongodb port 
# MONGOPORT = config('MONGOPORT')
# LIBRARYTYPE = config('LIBRARYTYPE') # The name of the library
# ACCESS_TOKEN_SECRET = config('ACCESS_TOKEN_SECRET')
# CONSUMER_KEY = config('CONSUMER_KEY')
# CONSUMER_SECRET = config('CONSUMER_SECRET')


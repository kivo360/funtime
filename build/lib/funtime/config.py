from decouple import config

MONGOHOST = config('MONGOHOST')
# The mongodb port 
MONGOPORT = config('MONGOPORT')
LIBRARYTYPE = config('LIBRARYTYPE') # The name of the library
# ACCESS_TOKEN_SECRET = config('ACCESS_TOKEN_SECRET')
# CONSUMER_KEY = config('CONSUMER_KEY')
# CONSUMER_SECRET = config('CONSUMER_SECRET')


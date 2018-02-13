import logging, os
from os.path import expanduser

lname = "kernels"
lFilename = "edas-python-" + lname
log_file = '/tmp/' + expanduser("~") + '/logs/' + lFilename + "-" + str(os.getpid()) +'.log'
logger = logging.getLogger( lname )
formatter = logging.Formatter(lname + ': %(asctime)s %(levelname)s %(message)s')
handler = logging.FileHandler( log_file )
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
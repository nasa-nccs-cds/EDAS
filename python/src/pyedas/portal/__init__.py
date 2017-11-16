import logging, os, getpass

lname = "edas-python-portal"
log_file = os.path.expanduser( '/tmp/' + getpass.getuser() + '/logs/' + lname + '.log')
try: os.remove(log_file)
except Exception: pass

logger = logging.getLogger( lname )
formatter = logging.Formatter(lname + ': %(asctime)s %(levelname)s %(message)s')
handler = logging.FileHandler( log_file )
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

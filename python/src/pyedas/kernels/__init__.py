import logging, os, getpass

lname = "worker"
lFilename = "edas-python-" + lname
log_path = os.path.expanduser( '/tmp/' + getpass.getuser() + '/logs/' )
if not os.path.exists(log_path): os.makedirs(log_path)
log_file = log_path  + "/" +  lFilename + "-" + str(os.getpid()) +'.log'

try: os.remove(log_file)
except Exception: pass

logger = logging.getLogger( lname )
formatter = logging.Formatter(lname + ': %(asctime)s %(levelname)s %(message)s')
handler = logging.FileHandler( log_file )
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


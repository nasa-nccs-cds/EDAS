import logging, os, getpass
import socket

lname = "worker"
lFilename = "edas-python-" + lname + "-" + socket.gethostname() + "-" + str(os.getpid())
log_file = os.path.expanduser( '~/.edas/logs/' + lFilename  +'.log')
logger = logging.getLogger()
formatter = logging.Formatter(lname + ': %(asctime)s %(levelname)s %(message)s')
handler = logging.FileHandler( log_file )
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

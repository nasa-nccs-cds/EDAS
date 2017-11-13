import subprocess, shlex, os
from psutil import virtual_memory
request_port = 5670
response_port = 5671
MB = 1024 * 1024
mem = virtual_memory()
total_ram = mem.total / MB
EDAS_MAX_MEM = os.environ.get( 'EDAS_MAX_MEM', str( total_ram - 1000 ) + 'M' )

try:
    edas_startup = "edas bind {0} {1} -J-Xmx{2} -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( request_port, response_port, EDAS_MAX_MEM )
    process = subprocess.Popen(shlex.split(edas_startup))
    print "Staring EDAS with command: {0}\n".format(edas_startup)
    process.wait()
except KeyboardInterrupt as ex:
    print "  ----------------- EDAS TERM ----------------- "
    process.kill()
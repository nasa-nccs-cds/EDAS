import subprocess, signal, os
p = subprocess.Popen([ os.path.expanduser( "~/.edas/sbin/shutdown_python_worker.sh" ) ], stdout=subprocess.PIPE)
out, err = p.communicate()

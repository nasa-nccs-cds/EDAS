package nasa.nccs.edas.workers.python;
import nasa.nccs.edas.workers.Worker;
import nasa.nccs.edas.workers.WorkerPortal;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.*;
import java.net.InetAddress;
import java.net.UnknownHostException;


public class PythonWorker extends Worker {
    Process proc;

    public PythonWorker( WorkerPortal portal ) throws Exception {
        super( portal );
        proc = startup();
        _portal.logger.info( " *** Started worker process: " +  proc.toString() );
    }


    protected Process startup() throws Exception {
        try {
            FileSystem fileSystems = FileSystems.getDefault();
            Path log_path = fileSystems.getPath( "/tmp", System.getProperty("user.name"), "logs", String.format("python-worker-%d.log",request_port) );
            Path run_script = fileSystems.getPath( System.getProperty("user.home"), ".edas", "sbin", "startup_python_worker.sh" );
            Map<String, String> sysenv = System.getenv();
//            ProcessBuilder pb = new ProcessBuilder( "python", "-m", "pyedas.worker", String.valueOf(request_port), String.valueOf(result_port) );
            ProcessBuilder pb = new ProcessBuilder( run_script.toString(), String.valueOf(request_port), String.valueOf(result_port) );
            Map<String, String> env = pb.environment();
            for (Map.Entry<String, String> entry : sysenv.entrySet()) { env.put( entry.getKey(), entry.getValue() ); }
            pb.redirectErrorStream( true );
            pb.redirectOutput( ProcessBuilder.Redirect.appendTo( log_path.toFile() ));
            _portal.logger.info( " #PW# ("+ _portal.getProcessorAddress() + ") Starting Python Worker: pyedas.worker.Worker --> request_port = " + String.valueOf(request_port)+ ", result_port = " + String.valueOf(result_port));
            return pb.start();
        } catch ( IOException ex ) {
            throw new Exception( "Error starting Python Worker : " + ex.toString() );
        }
    }

}

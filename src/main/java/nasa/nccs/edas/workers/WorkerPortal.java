package nasa.nccs.edas.workers;
import nasa.nccs.utilities.EDASLogManager;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class WorkerPortal extends Thread {
    protected ZMQ.Context zmqContext = null;
    protected ConcurrentLinkedQueue<Worker> availableWorkers = null;
    protected ConcurrentLinkedQueue<Worker> busyWorkers = null;
    public Logger logger = EDASLogManager.getCurrentLogger();

    protected WorkerPortal(){
        zmqContext = ZMQ.context(1);
        availableWorkers = new ConcurrentLinkedQueue<Worker>();
        busyWorkers = new ConcurrentLinkedQueue<Worker>();
    }

    public Worker getWorker() throws Exception {
        Worker worker = availableWorkers.poll();
        if( worker == null ) { worker =  newWorker(); }
        busyWorkers.add( worker );
        return worker;
    }

    protected abstract Worker newWorker() throws Exception;

    public void releaseWorker( Worker worker ) {
        busyWorkers.remove( worker );
        availableWorkers.add( worker );
    }

    public void killWorker( Worker worker ) {
        busyWorkers.remove( worker );
        availableWorkers.remove( worker );
        worker.quit();
    }

    int getNumWorkers() { return availableWorkers.size() + busyWorkers.size(); }

    public void shutdown() { try { start(); } catch ( Exception ex ) { run(); } }

    public void run() {
        try {
            logger.info( "\t   ***!! WorkerPortal SHUTDOWN !!*** " );
            while( !availableWorkers.isEmpty() ) try { availableWorkers.poll().quit(); } catch ( Exception ex ) {;}
            while( !busyWorkers.isEmpty() ) try { busyWorkers.poll().quit(); } catch ( Exception ex ) {;}
            logger.info( "\t   *** Worker shutdown complete *** " );
        } catch ( Exception ex ) { logger.info( "Error shutting down WorkerPortal: " + ex.toString() ); }
    }

    public void shutdown1() {
        logger.info( "\t   *** WorkerPortal SHUTDOWN *** " );
        while( !availableWorkers.isEmpty() ) try {
            Worker worker = availableWorkers.poll();
            worker.quit();
        } catch ( Exception ex ) {;}
        while( !busyWorkers.isEmpty() ) { busyWorkers.poll().quit(); }
        try { Thread.sleep(2000); } catch ( Exception ex ) {;}
    }
}

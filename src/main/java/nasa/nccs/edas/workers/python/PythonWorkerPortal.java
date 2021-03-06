package nasa.nccs.edas.workers.python;
import nasa.nccs.edas.workers.Worker;
import nasa.nccs.edas.workers.WorkerPortal;

public class PythonWorkerPortal extends WorkerPortal {

    private PythonWorkerPortal() {
        super();
    }

    public String[] getCapabilities() {
        try {
            PythonWorker worker = getPythonWorker();
            String response =  worker.getCapabilities();
            logger.info( "GET CAPABILITIES RESPONSE: " + response );
            releaseWorker(worker);
            return response.split("[|]");
        } catch ( Exception ex ) {
            logger.error( "GET CAPABILITIES ERROR: " + ex.toString() );
            logger.error( ex.getStackTrace()[0].toString() );
            return null;
        }
    }

    private static class SingletonHelper{
        private static final PythonWorkerPortal INSTANCE = new PythonWorkerPortal();
    }

    public static PythonWorkerPortal getInstance(){
        return PythonWorkerPortal.SingletonHelper.INSTANCE;
    }

    protected Worker newWorker() throws Exception { return  new PythonWorker( this ); }

    public PythonWorker getPythonWorker() throws Exception { return (PythonWorker) getWorker(); }

    public void quit() { shutdown(); }
}

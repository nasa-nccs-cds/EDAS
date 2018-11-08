package nasa.nccs.edas.portal;

import nasa.nccs.edas.engine.ExecutionCallback;
import nasa.nccs.edas.workers.TransVar;
import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Loggable;
import nasa.nccs.utilities.Logger;
import org.joda.time.DateTime;
import org.zeromq.ZMQ;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;

class ResponseConnectionType {
    static int PushPull = 0;
    static int PubSub = 1;
}
class HeartbeatManager  {
    Long heartbeatTime = null;
    Long maxHeartbeatPeriod = 3 * 60 * 1000L;
    protected Logger logger = EDASLogManager.getCurrentLogger();

    public HeartbeatManager() { processHeartbeat("init"); }

    public void processHeartbeat(String type) {
        heartbeatTime =  Calendar.getInstance().getTimeInMillis();
        logger.info( "  ################ ProcessHeartbeat-> " + new DateTime(heartbeatTime).toString("hh:mm:ss dd-M-yyyy") + ": " +  type );
    }

    public boolean serverIsDown() {
        Long currentTime = Calendar.getInstance().getTimeInMillis();
        return ( (currentTime - heartbeatTime) > maxHeartbeatPeriod );
    }
}

public class ResponseManager extends Thread {
    String socket_address = "";
    String client_id = "";
    int responseConnectionType = ResponseConnectionType.PushPull;
    protected ZMQ.Context zmqContext = null;
    Boolean active = true;
    Map<String, List<String>> cached_results = null;
    Map<String, List<TransVar>> cached_arrays = null;
    Map<String, List<String>> result_file_paths = null;
    Map<String, ExecutionCallback> callbacks = null;
    String cacheDir = null;
    String publishDir = null;
    String latest_result = "";
    HeartbeatManager heartbeatManager = null;
    SimpleDateFormat timeFormat = new SimpleDateFormat("HH-mm-ss MM-dd-yyyy");
    protected Logger logger = EDASLogManager.getCurrentLogger();
    protected CleanupManager cleanupManager = new CleanupManager();

    public boolean serverIsDown( ) {
        if ( heartbeatManager == null ) { return false; } else { return heartbeatManager.serverIsDown(); }
    }

    public ResponseManager( ZMQ.Context _zmqContext, String _socket_address, String _client_id, Map<String,String> configuration ) {
        socket_address = _socket_address;
        client_id = _client_id;
        zmqContext = _zmqContext;
        cached_results = new HashMap<String, List<String>>();
        cached_arrays = new HashMap<String, List<TransVar>>();
        callbacks = new HashMap<String, ExecutionCallback>();
        result_file_paths = new HashMap<String,List<String>>();
        setName("EDAS ResponseManager");
        setDaemon(true);
        FileSystem fileSystems = FileSystems.getDefault();
        String EDAS_CACHE_DIR = System.getenv( "EDAS_CACHE_DIR" );
        cacheDir = ( EDAS_CACHE_DIR == null ) ? "/tmp/" : EDAS_CACHE_DIR;
        publishDir =  EDASPortalClient.getOrDefault( configuration, "edas.publish.dir", cacheDir );
        logger.info( String.format("@@RM: Starting ResponseManager, publishDir = %s, cacheDir = %s, connecting to %s", publishDir, cacheDir, socket_address ) );
        File[] subdirs = getDirs(publishDir);
        for(int i = 0; i< subdirs.length; i++){ cleanupManager.addFileCleanupTask( subdirs[i].getAbsolutePath(), 1.0f, true, ".*" ); }
        cleanupManager.addFileCleanupTask( publishDir, 1.0f, false, ".*" );
        cleanupManager.addFileCleanupTask( logger.logFileDir().toString(), 2.0f, true, ".*" );

    }

    public File[] getDirs( String baseDir ) {
        return new File(baseDir).listFiles( new FileFilter() {
            @Override
            public boolean accept(File file) { return file.isDirectory(); }
        });
    }

    public void processHeartbeat( String type ) {
        if (heartbeatManager == null) { heartbeatManager = new HeartbeatManager(); }
        heartbeatManager.processHeartbeat(type);
    }

    public void registerCallback( String jobId, ExecutionCallback callback ) {
        callbacks.put( jobId, callback );
    }

    public void cacheResult(String id, String result) {
        if( !result.startsWith("heart") ) {
            logger.info( "@@RM:  #CR# Caching result for id = "+id+", value =  " + result );
            getResults(id).add(result);
        }
    }

    public boolean hasResult( String id ) {
        logger.info( "@@RM: Checking for result '" + id + "', cached results =  " + cached_results.keySet().toString() );
        List<String> cached_result = cached_results.get(id);
        if(cached_result != null) {  logger.info( "@@RM:  #CR# Cached result values =  " + cached_result.toString() );  }
        return (cached_result != null);
    }

    public List<String> getResults(String id) {
        List<String> results = cached_results.get(id);
        if( results == null ) {
            results = new LinkedList<String>();
            cached_results.put( id, results );
        }
        return results;
    }

    private  void cacheArray( String id, TransVar array ) { getArrays(id).add(array); }

    public  List<TransVar> getArrays(String id) {
        List<TransVar> arrays = cached_arrays.get(id);
        if( arrays == null ) {
            arrays = new LinkedList<TransVar>();
            cached_arrays.put( id, arrays );
        }
        return arrays;
    }

    public void run() {
        try {
            ZMQ.Socket socket = null;
            if( responseConnectionType == ResponseConnectionType.PubSub ) {
                socket = zmqContext.socket(ZMQ.SUB);
                socket.connect(socket_address);
                socket.subscribe(client_id);
            } else {
                socket = zmqContext.socket(ZMQ.PULL);
                socket.connect(socket_address);
            }
            logger.info( "@@RM: EDASPortalClient subscribing to EDASServer publisher channel " + client_id );
            while (active) { processNextResponse( socket ); }
            socket.close();
        } catch( Exception err ) {
            logger.error( "@@RM: ResponseManager ERROR: " + err.toString() );
            logger.error( ExceptionUtils.getStackTrace(err) );
        }
    }

    public void term() {
        active = false;
    }

    public String getMessageField( String header, int index) {
        String[] toks = header.split("[|]");
        return toks[index];
    }

    private void processNextResponse( ZMQ.Socket socket ) {
        try {
            String response = new String(socket.recv(0)).trim();
            String[] toks = response.split("[!]");
            String[] toks0 = toks[0].split("[:]");
            String rId = toks0[toks0.length-1];
            int dataOffset = 0;
            if( responseConnectionType == ResponseConnectionType.PubSub ) { dataOffset = 8; }
            String rtype = toks[1];
            String header = toks[2];
            logger.info( "@@RM: Received Response: " + response + ", type = " + rtype + ", rId = " + rId );
            switch ( rtype ) {
                case "array":
                    byte[] bdata = socket.recv(0);
                    cacheArray( rId, new TransVar( header, bdata, dataOffset ) );
                    cacheResult( rId, header );
                    break;
                case "file":
                    try {
                        logger.info( "@@RM: Receiving File Data for header: " + header );
                        byte[] data = socket.recv(0);
                        logger.info( "@@RM: Received File Data" );
                        Path outFilePath = saveFile( header, rId, data, dataOffset );
                        List<String> paths = result_file_paths.getOrDefault(rId, new LinkedList<String>() );
                        paths.add( outFilePath.toString() );
                        result_file_paths.put( rId, paths );
                        cacheResult( rId, header );
                        logger.info( String.format("@@RM: Received file %s for rid %s, saved to: %s", header, rId, outFilePath.toString() ) );
                    } catch( Exception err ) {
                        logger.error(String.format("@@RM: Unable to write to output file: %s", err.getMessage() ) );
                    }
                    break;
                case "response":
                    cacheResult(rId, header );
                    String _currentTime = timeFormat.format(Calendar.getInstance().getTime());
                    logger.info(String.format("@@RM: Received result[%s] (%s): %s", rId, _currentTime, response));
                    break;
                case "error":
                    cacheResult(rId, header );
                    String currentTime_ = timeFormat.format(Calendar.getInstance().getTime());
                    logger.info(String.format("@@RM: Received error[%s] (%s): %s", rId, currentTime_, response ) );
                    break;
                default:
                    logger.error(String.format("@@RM: EDASPortal.ResponseThread-> Received unrecognized message type: %s",rtype));
            }
            processHeartbeat(rtype);

        } catch( Exception err ) {
            logger.error(String.format("@@RM: EDAS error: %s\n%s\n", err, ExceptionUtils.getStackTrace(err) ) );
        }
    }

    private Path saveFile( String header, String response_id, byte[] data, int offset ) throws IOException {
        String[] header_toks = header.split("[|]");
        String id = header_toks[1];
        String role = header_toks[2];
        String fileName = header_toks[3];
//        String fileName = response_id.substring( response_id.lastIndexOf(':') + 1 ) + ".nc";
        Path outFilePath = getPublishFile( role, fileName );
        logger.debug("@@RM:  ##saveFile: role=" + role + " fileName=" + fileName + " id=" + id + " outFilePath=" + outFilePath );
        DataOutputStream os = new DataOutputStream(new FileOutputStream(outFilePath.toFile()));
        os.write(data, offset, data.length-offset );
        os.close();
        return outFilePath;
    }


    private Path getPublishFile( String role, String fileName  ) throws IOException {
        Path pubishDir = Paths.get( publishDir, role );
        if( !pubishDir.toFile().exists() ) {
            Files.createDirectories( pubishDir );
        }
        return Paths.get( publishDir, role, fileName );
    }

    public List<String> getSavedFilePaths( String rId ) throws IOException {
        if ( rId.endsWith(".nc") ) { rId = rId.substring(0,rId.length()-3); }
        logger.debug("@@RM:getSavedFilePaths, rid = " + rId + ", keys = " + result_file_paths.keySet().toString() );
        return result_file_paths.getOrDefault(rId, new LinkedList<String>() );
    }


    public List<String> getResponses( String rId, Boolean wait ) {
        logger.debug("@@RM:#GR rid = " + rId + ", wait = " + wait.toString() );
        while (true) {
            List<String> results = getResults(rId);
            logger.debug("@@RM:#GR results = " + results.toString() );
            if (( results.size() > 0 ) || !wait) { return results; }
            else { try{ sleep(250 ); } catch(Exception err) { ; } }
        }
    }
}

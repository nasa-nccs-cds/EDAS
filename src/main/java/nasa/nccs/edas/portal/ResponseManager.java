package nasa.nccs.edas.portal;

import nasa.nccs.edas.engine.ExecutionCallback;
import nasa.nccs.edas.workers.TransVar;
import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Logger;
import org.zeromq.ZMQ;
import org.apache.commons.lang.exception.ExceptionUtils;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.SimpleDateFormat;
import java.util.*;

public class ResponseManager extends Thread {
    String socket_address = "";
    String client_id = "";
    protected ZMQ.Context zmqContext = null;
    Boolean active = true;
    Map<String, List<String>> cached_results = null;
    Map<String, List<TransVar>> cached_arrays = null;
    Map<String, String> file_paths = null;
    Map<String, ExecutionCallback> callbacks = null;
    String cacheDir = null;
    String publishDir = null;
    String latest_result = "";
    SimpleDateFormat timeFormat = new SimpleDateFormat("HH-mm-ss MM-dd-yyyy");
    protected Logger logger = EDASLogManager.getCurrentLogger();
    protected CleanupManager cleanupManager = new CleanupManager();

    public ResponseManager( ZMQ.Context _zmqContext, String _socket_address, String _client_id, Map<String,String> configuration ) {
        socket_address = _socket_address;
        client_id = _client_id;
        zmqContext = _zmqContext;
        cached_results = new HashMap<String, List<String>>();
        cached_arrays = new HashMap<String, List<TransVar>>();
        callbacks = new HashMap<String, ExecutionCallback>();
        file_paths = new HashMap<String,String>();
        setName("EDAS ResponseManager");
        setDaemon(true);
        String EDAS_CACHE_DIR = System.getenv( "EDAS_CACHE_DIR" );
        cacheDir = ( EDAS_CACHE_DIR == null ) ? "/tmp/" : EDAS_CACHE_DIR;
        publishDir =  EDASPortalClient.getOrDefault( configuration, "edas.publish.dir", cacheDir );
        logger.info( String.format("Starting ResponseManager, publishDir = %s, cacheDir = %s, connecting to %s", publishDir, cacheDir, socket_address ) );
        cleanupManager.addFileCleanupTask( publishDir, 24, true, ".*" );
    }

    public void setFilePermissions( Path directory, String perms ) {
        try {
            Files.setPosixFilePermissions( directory, PosixFilePermissions.fromString(perms));
            File[] listOfFiles = directory.toFile().listFiles();
            for (int i = 0; i < listOfFiles.length; i++) {
                File file = listOfFiles[i];
                Files.setPosixFilePermissions( file.toPath(), PosixFilePermissions.fromString(perms) );
            }
        } catch ( Exception ex ) {
            logger.error("Error setting perms in dir " + directory + ", error = " + ex.getMessage());
        }
    }

    public void registerCallback( String jobId, ExecutionCallback callback ) {
        callbacks.put( jobId, callback );
    }

    public void cacheResult(String id, String result) { getResults(id).add(result); }

    public List<String> getResults(String id) {
        List<String> results = cached_results.get(id);
        if( results == null ) {
            results = new LinkedList<String>();
            cached_results.put( id, results );
        }
        return results;
    }

    public void cacheArray( String id, TransVar array ) { getArrays(id).add(array); }

    public List<TransVar> getArrays(String id) {
        List<TransVar> arrays = cached_arrays.get(id);
        if( arrays == null ) {
            arrays = new LinkedList<TransVar>();
            cached_arrays.put( id, arrays );
        }
        return arrays;
    }

    public void run() {
        try {
            ZMQ.Socket socket = zmqContext.socket(ZMQ.SUB);
            socket.connect(socket_address);
            socket.subscribe(client_id);
            logger.info( "EDASPortalClient subscribing to EDASServer publisher channel " + client_id );
            while (active) { processNextResponse( socket ); }
            socket.close();
        } catch( Exception err ) {
            logger.error( "ResponseManager ERROR: " + err.toString() );
            logger.error( ExceptionUtils.getStackTrace(err) );
        }
    }

    public void term() { active = false; }

    public String getMessageField( String header, int index) {
        String[] toks = header.split("[|]");
        return toks[index];
    }

    public void processNextResponse( ZMQ.Socket socket ) {
        try {
            String response = new String(socket.recv(0)).trim();
            String[] toks = response.split("[!]");
            String rId = toks[0];
            String type = toks[1];
            if ( type.equals("array") ) {
                String header = toks[2];
                byte[] data = socket.recv(0);
                cacheArray(rId, new TransVar( header, data, 8) );
            } else if ( type.equals("file") ) {
                try {
                    String header = toks[2];
                    byte[] data = socket.recv(0);
                    Path outFilePath = saveFile( header, rId, data, 8 );
                    file_paths.put( rId, outFilePath.toString() );
                    logger.info( String.format("Received file %s for rid %s, saved to: %s", header, rId, outFilePath.toString() ) );
                } catch( Exception err ) {
                    logger.error(String.format("Unable to write to output file: %s", err.getMessage() ) );
                }
            } else if ( type.equals("response") ) {
                cacheResult(rId, toks[2]);
                String currentTime = timeFormat.format(Calendar.getInstance().getTime());
                logger.info(String.format("Received result[%s] (%s): %s", rId, currentTime, response));
            } else if ( type.equals("error") ) {
                cacheResult(rId, toks[2]);
                String currentTime = timeFormat.format( Calendar.getInstance().getTime() );
                logger.info(String.format("Received error[%s] (%s): %s", rId, currentTime, response ) );
            } else {
                logger.error(String.format("EDASPortal.ResponseThread-> Received unrecognized message type: %s",type));
            }

        } catch( Exception err ) {
            logger.error(String.format("EDAS error: %s\n%s\n", err, ExceptionUtils.getStackTrace(err) ) );
        }
    }

    Path saveFile( String header, String response_id, byte[] data, int offset ) throws IOException {
        String[] header_toks = header.split("[|]");
        String id = header_toks[1];
        String role = header_toks[2];
        String fileName = header_toks[3];
//        String fileName = response_id.substring( response_id.lastIndexOf(':') + 1 ) + ".nc";
        Path outFilePath = getPublishFile( role, fileName );
        logger.debug(" ##saveFile: role=" + role + " fileName=" + fileName + " id=" + id + " outFilePath=" + outFilePath );
        DataOutputStream os = new DataOutputStream(new FileOutputStream(outFilePath.toFile()));
        os.write(data, offset, data.length-offset );
        setFilePermissions( outFilePath.getParent(), "rwxrwxrwx");
        return outFilePath;
    }


    public Path getPublishFile( String role, String fileName  ) throws IOException {
        java.util.Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwxrwx");
        FileAttribute<Set<PosixFilePermission>> fileAttr = PosixFilePermissions.asFileAttribute(perms);
        Path directory = Paths.get( publishDir, role );
        Path filePath = Paths.get( publishDir, role, fileName );
        Files.createDirectories( directory, fileAttr );
        return  Files.createFile( filePath, fileAttr );
    }

    public List<String> getResponses( String rId, Boolean wait ) {
        while (true) {
            List<String> results = getResults(rId);
            if (( results.size() > 0 ) || !wait) { return results; }
            else { try{ sleep(250 ); } catch(Exception err) { ; } }
        }
    }
}

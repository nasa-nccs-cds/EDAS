package nasa.nccs.edas.portal;

import nasa.nccs.edas.workers.TransVar;
import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Logger;
import org.zeromq.ZMQ;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ResponseManager extends Thread {
    ZMQ.Socket socket = null;
    Boolean active = true;
    Map<String, List<String>> cached_results = null;
    Map<String, List<TransVar>> cached_arrays = null;
    Map<String, String> file_paths = null;
    String cacheDir = null;
    protected Logger logger = EDASLogManager.getCurrentLogger();

    public ResponseManager(EDASPortalClient portalClient) {
        socket = portalClient.response_socket;
        cached_results = new HashMap<String, List<String>>();
        cached_arrays = new HashMap<String, List<TransVar>>();
        file_paths = new HashMap<String,String>();
        setName("EDAS ResponseManager");
        setDaemon(true);
        String EDAS_CACHE_DIR = System.getenv( "EDAS_CACHE_DIR" );
        cacheDir = ( EDAS_CACHE_DIR == null ) ? "/tmp/" : EDAS_CACHE_DIR;
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
        while (active) {
            processNextResponse();
        }
    }

    public void term() {
        active = false;
        try { socket.close(); }
        catch( Exception err ) { ; }
    }


    public String getMessageField( String header, int index) {
        String[] toks = header.split("[|]");
        return toks[index];
    }

    public void processNextResponse() {
        try {
            String response = new String(socket.recv(0)).trim();
            String[] toks = response.split("[!]");
            String rId = toks[0];
            String type = toks[1];
            if ( type.equals("array") ) {
                String header = toks[2];
                byte[] data = socket.recv(0);
                cacheArray(rId, new TransVar( header, data) );
            } else if ( type.equals("file") ) {
                String header = toks[2];
                byte[] data = socket.recv(0);
                File filePath = saveFile( header, data );
                file_paths.put( rId, filePath.toString() );
                logger.info( String.format("Received file %s for rid %s",header,rId) );
            } else if ( type.equals("response") ) {
                cacheResult(rId, toks[2]);
                logger.info(String.format("Received result: %s",toks[2]));
            } else {
                logger.error(String.format("EDASPortal.ResponseThread-> Received unrecognized message type: %s",type));
            }

        } catch( Exception err ) {
            logger.error(String.format("EDAS error: %s\n%s\n", err, err.getStackTrace().toString() ) );
        }
    }

    File saveFile( String header, byte[] data ) {
        String[] header_toks = header.split("|");
        String id = header_toks[1];
        String role = header_toks[2];
        String fileName = header_toks[3];
        Path fileCacheDir = getFileCacheDir(role);
        File outFile = new File( fileCacheDir.toFile(), fileName);
        try {
            DataOutputStream os = new DataOutputStream(new FileOutputStream(outFile));
            os.write(data, 0, data.length);
        } catch( Exception err ) {
            logger.error(String.format("Unable to write to file(%s): %s\n%s\n", id, outFile.toString(), err.getMessage() ) );
        }
        return outFile;
    }


    Path getFileCacheDir( String role ) {
        Path filePath = Paths.get( cacheDir, "transfer", role );
        try {
            Files.createDirectories( filePath );
        } catch( Exception err ) {
            logger.error(String.format("Unable to create directory %s", filePath.toString() ) );
        }
        return filePath;
    }

    public List<String> getResponses( String rId, Boolean wait ) {
        while (true) {
            List<String> results = getResults(rId);
            if (( results.size() > 0 ) || !wait) { return results; }
            else { try{ sleep(250 ); } catch(Exception err) { ; } }
        }
    }
}

package nasa.nccs.edas.workers;
import nasa.nccs.cdapi.data.HeapFltArray;
import org.apache.commons.lang.ArrayUtils;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.Logger;
import ucar.ma2.DataType;
import ucar.ma2.Array;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.StringUtils;

public abstract class Worker {
    int BASE_PORT = 2336;
    ZMQ.Socket request_socket = null;
    ConcurrentLinkedQueue<TransVar> results = null;
    ConcurrentLinkedQueue<String> messages = null;
    Process process = null;
    ResultThread resultThread = null;
    protected WorkerPortal _portal = null;
    protected int result_port = -1;
    protected int request_port = -1;
    private String errorCondition = null;
    private String withData = "1";
    private String withoutData = "0";
    private long requestTime = 0;
    ByteBuffer byteBuffer = java.nio.ByteBuffer.allocate(4);

    static int bindSocket( ZMQ.Socket socket, int init_port ) {
        int test_port = init_port;
        while( true ) {
            try {
                socket.bind("tcp://127.0.0.1:" + String.valueOf(test_port));
                break;
            } catch (Exception err ) {
                test_port = test_port + 1;
            }
        }
        return test_port;
    }

    public int id() { return request_port; }

    private void postInfo( String info ) {
        _portal.logger.info( "Posting info from worker: " + info );
        messages.add( info );
    }

    private void addResult( String result_header, byte[] data ) {
        String elapsedTime = String.valueOf( ( System.currentTimeMillis() - requestTime )/1000.0 );
        _portal.logger.info( "Caching result from worker:   " + result_header+ ", data size = " + data.length  + ", Worker time = " + elapsedTime );
        results.add( new TransVar( result_header, data, 0 ) );
    }

    private void invalidateRequest( String errorMsg ) { errorCondition = errorMsg; }

    public TransVar getResult() throws Exception {
        _portal.logger.debug( "Waiting for result to appear from worker");
        long t0 = System.currentTimeMillis();
        while( true ) {
            if( errorCondition != null ) {
                throw new Exception( errorCondition );
            }
            TransVar result = results.poll();
            if( result == null ) try { Thread.sleep(100); } catch( Exception err ) { return null; }
            else {
                _portal.releaseWorker( this );
                String elapsedTime = String.valueOf( ( System.currentTimeMillis() - t0 )/1000.0 );
                _portal.logger.debug( "Received result from worker after " + elapsedTime + " secs" );
                return result;
            }
        }
    }

    public String getMessage() {
        _portal.logger.debug( "Waiting for message to be posted from worker");
        while( errorCondition == null ) {
            String message = messages.poll();
            if( message == null ) try { Thread.sleep(100); } catch( Exception err ) { return null; }
            else { return message; }
        }
        return null;
    }

    public class ResultThread extends Thread {
        ZMQ.Socket result_socket = null;
        int port = -1;
        boolean active = true;
        public ResultThread( int base_port, ZMQ.Context context ) {
            result_socket = context.socket(ZMQ.PULL);
            port = bindSocket(result_socket,base_port);
        }
        public void run() {
            while( active ) try {
                String result_header = new String(result_socket.recv(0)).trim();
                String[] parts = result_header.split("[|]");
                _portal.logger.info( "Received result header from worker: " + result_header );
                String[] mtypes = parts[0].split("[-]");
                String mtype = mtypes[0];
                int mtlen = parts[0].length();
                String pid = mtypes[1];
                if( mtype.equals("array") ) {
                    _portal.logger.debug("Waiting for result data ");
                    byte[] data = result_socket.recv(0);
                    addResult(result_header, data);
                } else if( mtype.equals("info") ) {
                    postInfo( result_header.substring(mtlen+1) );
                } else if( mtype.equals("error") ) {
                    _portal.logger.error("Python worker {0} signaled error: {1}\n".format( pid, parts[1]) );
                    invalidateRequest(result_header.substring(mtlen+1));
                    quit();
                } else {
                    _portal.logger.info( "Unknown result message type: " + parts[0] );
                }
            } catch ( java.nio.channels.ClosedSelectorException ex ) {
                _portal.logger.info( "Result Socket closed." );
                active = false;
            } catch ( Exception ex ) {
                _portal.logger.error( "Error in ResultThread: " + ex.toString() );
                ex.printStackTrace();
                term();
            }
        }
        public void term() {
            active = false;
            try { result_socket.close(); }  catch ( Exception ex ) { ; }
        }
    }

    public Worker( WorkerPortal portal ) {
        _portal = portal;
        results = new ConcurrentLinkedQueue();
        messages = new ConcurrentLinkedQueue();

        request_socket = _portal.zmqContext.socket(ZMQ.PUSH);
        request_port = bindSocket( request_socket, BASE_PORT );
        resultThread = new ResultThread( request_port + 1, _portal.zmqContext );
        resultThread.setDaemon(true);
        resultThread.start();
        result_port = resultThread.port;
        _portal.logger.info( String.format("Starting Worker, ports: %d %d",  request_port, result_port ) );
        byteBuffer.putFloat( 0, Float.MAX_VALUE );
    }

    @Override
    public void finalize() { quit(); }

    public void sendDataPacket( String header, byte[] data ) {
        _portal.logger.debug(" #PW# " + this.id() + " Sending header: " + header);
        request_socket.send(header.getBytes(), 0 );
        _portal.logger.debug( String.format( " #PW#  " + this.id() + " Sending data, nbytes = %d", data.length ) );
        request_socket.send(data, 0 );
    }

    public void quit() {
        _portal.logger.debug("Worker: Sending Quit request" );
        request_socket.send( "util|quit".getBytes(), 0 );
        _portal.logger.debug("Worker: TERM resultThread" );
        try { resultThread.term();  }  catch ( Exception ex ) { ; }
        _portal.logger.debug("Worker: CLOSE request_socket" );
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        _portal.logger.debug("Worker shutdown complete" );
    }

    public void getCapabilites() {
        request_socket.send("util|capabilities".getBytes(), 0);
    }

    public void sendRequestInput( String id, HeapFltArray array ) {
        if( array.hasData() )  {
            _sendArrayData( id, array.origin(), array.shape(), array.toByteArray(), array.mdata() );
            scala.Option<float[]> weightsOpt = array.weights();
            if( weightsOpt.isDefined() ) {
                float[] weights = weightsOpt.get();
                String[] shapeStr = array.attr("wshape").split(",");
                int[] shape = new int[shapeStr.length];
                _portal.logger.info( " #S# --> SendRequestInput[" + id + "], shape = " + Arrays.toString(shape) );
                for (int i = 0; i < shapeStr.length; i++) {
                    try { shape[i] = Integer.parseInt(shapeStr[i]); }
                    catch ( NumberFormatException nfe ) { _portal.logger.error("Error parsing shape in sendRequestInput: " + array.attr("wshape") ); return; };
                }
                byte[] weight_data = ArrayUtils.addAll( Array.factory(DataType.FLOAT, shape, weights ).getDataAsByteBuffer().array(), byteBuffer.array() );
                String[] idtoks =  id.split("-");
                idtoks[0] = idtoks[0] + "_WEIGHTS_";
                _sendArrayData( StringUtils.join( idtoks, "-" ), array.origin(), shape, weight_data, array.mdata()  );
            }
        }
        else _sendArrayMetadata( id, array.origin(), array.shape(), array.mdata() );
    }

    public void sendArrayMetadata( String id, HeapFltArray array ) {
        _sendArrayMetadata( id, array.origin(), array.shape(), array.mdata() );
    }

    private void _sendArrayData( String id, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        _portal.logger.debug( String.format(">>>---> Kernel: Sending data to worker for input %s, nbytes=%d, origin=%s, shape=%s, metadata=%s", id, data.length, ia2s(origin), ia2s(shape), m2s(metadata) ));
        List<String> slist = Arrays.asList( "array", id, ia2s(origin), ia2s(shape), m2s(metadata), withData );
        String header = StringUtils.join(slist,"|");
        sendDataPacket( header, data );
    }

    private void _sendArrayMetadata( String id, int[] origin, int[] shape, Map<String, String> metadata ) {
        _portal.logger.debug( String.format(" #S# Kernel: Sending metadata to worker for input %s, shape: %s, metadata: %s", id, Arrays.toString(shape), m2s(metadata) ));
        List<String> slist = Arrays.asList( "array", id, ia2s(origin), ia2s(shape), m2s(metadata), withoutData );
        String header = StringUtils.join(slist,"|");
        _portal.logger.debug("Sending header: " + header);
        request_socket.send(header.getBytes(), 0);
    }

    public void sendRequest( String operation, String[] opInputs, Map<String, String> metadata ) {
        List<String> slist = Arrays.asList(  "task", operation, sa2s(opInputs), m2s(metadata)  );
        String header = StringUtils.join(slist,"|");
        _portal.logger.info( "Sending Task Request: " + header );
        requestTime = System.currentTimeMillis();
        request_socket.send(header.getBytes(), 0);
        errorCondition = null;
    }

    public void sendUtility( String request ) {
        List<String> slist = Arrays.asList(  "util", request );
        String header = StringUtils.join(slist,"|");
        _portal.logger.debug( "Sending Utility Request: " + header );
        request_socket.send(header.getBytes(), 0);
        _portal.logger.debug( "Utility Request Sent!" );
    }

    public String getCapabilities() {
        sendUtility("capabilities");
        return getMessage();
    }

    public String ia2s( int[] array ) { return Arrays.toString(array).replaceAll("\\[|\\]|\\s", ""); }
    public String sa2s( String[] array ) { return StringUtils.join(array,","); }
    public String m2s( Map<String, String> metadata ) {
        ArrayList<String> items = new ArrayList<String>();
        for (Map.Entry<String,String> entry : metadata.entrySet() ) {
            items.add( entry.getKey() + ":" + entry.getValue() );
        }
        return StringUtils.join(items,";" );
    }
}

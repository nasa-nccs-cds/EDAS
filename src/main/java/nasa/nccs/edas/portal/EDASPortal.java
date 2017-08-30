package nasa.nccs.edas.portal;
import com.google.common.io.Files;
import nasa.nccs.edas.workers.python.PythonWorkerPortal;
import nasa.nccs.utilities.Logger;
import org.apache.commons.lang.StringUtils;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.EDASLogManager;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

abstract class Response {
    String rtype;
}

class Message extends Response {
    String id = null;
    String message = null;
    public Message( String _id, String _message ) {
        rtype = "message";
        id = _id;
        message = _message;
    }
    public String toString() { return "Message[" + id + "]: " + message; }
}

class DataPacket extends Response {
    String header = null;
    byte[] data = null;
    public DataPacket( String _header, byte[] _data ) {
        rtype = "data";
        header = _header;
        data = _data;
    }
    public String toString() { return "DataPacket[" + header + "]" }
}


class Responder extends Thread {
    ZMQ.Socket response_socket = null;
    Boolean active = true;
    protected int response_port = -1;
    protected Logger logger = EDASLogManager.getCurrentLogger();
    ConcurrentLinkedQueue<Response> response_queue = null;
    HashMap<String,String> status_reports = null;

    public Responder(ZMQ.Context zmqContext, String client_address, int _response_port ) {
        response_socket = zmqContext.socket(ZMQ.PUSH);
        response_port = _response_port;
        response_queue = new ConcurrentLinkedQueue<Response>();
        status_reports = new HashMap<String,String>();
        try{
            response_socket.bind(String.format("tcp://%s:%d", client_address, response_port));
            logger.info( String.format("Bound response socket to client at %s on port: %d", client_address, response_port) );
        } catch (Exception err ) { logger.error( String.format("Error initializing response socket on port %d: %s", response_port, err ) ); }

    }

    public void sendMessage( Message msg ) {
        logger.info( "Post Message to response queue: " + msg.toString() );
        response_queue.add( msg );
    }

    public void sendDataPacket( DataPacket data ) {
        logger.info( "Post DataPacket to response queue: " + data.toString() );
        response_queue.add( data );
    }

    void doSendResponse( Response r ) {
        if( r.rtype == "message") { doSendMessage( (Message)r ); }
        else if( r.rtype == "data") { doSendDataPacket( (DataPacket)r ); }
        else { logger.error( "Error, unrecognized response type: " + r.rtype ); }
    }


    void doSendMessage( Message msg  ) {
        List<String> request_args = Arrays.asList( msg.id, "response", msg.message );
        response_socket.send( StringUtils.join( request_args,  "!" ).getBytes(), 0);
        logger.info( " Sent response: " + msg.id + ", content: " + msg.message.substring( 0, Math.min(300,msg.message.length()) ) );
    }

    void doSendDataPacket( DataPacket dataPacket ) {
        response_socket.send( dataPacket.header.getBytes(), 0 );
        response_socket.send( dataPacket.data, 0 );
    }

    public void setExeStatus( String rid, String status ) {
        status_reports.put(rid,status);
    }

    void heartbeat() {
        Message hb_msg = new Message( "status", status_reports.toString() );
        doSendMessage( hb_msg );
    }

    public void run()  {
        int pause_time = 100;
        int accum_sleep_time = 0;
        int heartbeat_interval = 2000;
        try {
            while (active) {
                Response response = response_queue.poll();
                if (response == null) {
                    Thread.sleep(pause_time);
                    accum_sleep_time += pause_time;
                    if( accum_sleep_time >= heartbeat_interval ) {
                        heartbeat();
                        accum_sleep_time = 0;
                    }
                } else {
                    doSendResponse(response);
                    accum_sleep_time = 0;
                }
            }
        } catch ( InterruptedException ex ) {;}
    }

    public void term() {
        active = false;
        try { response_socket.close(); }
        catch( Exception err ) { ; }
    }

}

public abstract class EDASPortal {
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected int request_port = -1;
    protected Responder responder = null;

    protected Logger logger = EDASLogManager.getCurrentLogger();
    private boolean active = true;

    public abstract void sendErrorReport( String[] taskSpec,  Exception err  );

    protected EDASPortal( String client_address, int _request_port, int _response_port ) {
        try {
            request_port = _request_port;
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.PULL);
            responder = new Responder( zmqContext, client_address, _response_port);
            responder.start();

//                try{
//                    request_socket.connect(String.format("tcp://%s:%d", client_address, request_port));
//                    logger.info(String.format("[1]Connected request socket to client at %s  on port: %d", client_address, request_port));
//                } catch (Exception err ) { logger.error( String.format("Error initializing request socket on port %d: %s", request_port, err ) ); }
//                try{
//                    response_socket.connect(String.format("tcp://%s:%d", client_address, response_port));
//                    logger.info(String.format("Connected response socket to client at %s  on port: %d", client_address, response_port));
//                } catch (Exception err ) { logger.error( String.format("Error initializing response socket on port %d: %s", response_port, err ) ); }

            try{
                request_socket.bind(String.format("tcp://%s:%d", client_address, request_port));
                logger.info(String.format("Bound request socket to client at %s on port: %d", client_address, request_port));
            } catch (Exception err ) { logger.error( String.format("Error initializing request socket on port %d: %s", request_port, err ) ); }

        } catch (Exception err ) {
            logger.error( String.format("\n-------------------------------\nEDAS Init error: %s -------------------------------\n", err ) );
        }
    }

    public void sendResponse(  String id, String msg ) {
        logger.info("-----> SendResponse[" + id + "]" );
        responder.sendMessage( new Message(id,msg) );
    }

    public void setExeStatus( String rid, String status ) {
        responder.setExeStatus(rid,status);
    }

    public void sendArrayData( String rid, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        logger.debug( String.format("Portal: Sending response data to client for rid %s, nbytes=%d", rid, data.length ));
        List<String> array_header_fields = Arrays.asList( "array", rid, ia2s(origin), ia2s(shape), m2s(metadata), "1" );
        String array_header = StringUtils.join(array_header_fields,"|");
        List<String> header_fields = Arrays.asList( rid,"array", array_header );
        String header = StringUtils.join(header_fields,"!");
        logger.debug("Sending header: " + header);
        responder.sendDataPacket( new DataPacket( header, data ) );
    }

    public String sendFile( String rId, String name, String filePath ) {
        logger.debug( String.format("Portal: Sending file data to client for %s, filePath=%s", name, filePath ));
        File file = new File(filePath);
        String[] file_header_fields = { "array", rId, name, file.getName() };
        String file_header = StringUtils.join( file_header_fields, "|" );
        List<String> header_fields = Arrays.asList( rId,"file", file_header );
        String header = StringUtils.join(header_fields,"!");
        try {
            byte[] data = Files.toByteArray(file);
            responder.sendDataPacket( new DataPacket( header, data ) );
            logger.debug("Done sending file data packet: " + header);
        } catch ( IOException ex ) {
            logger.info( "Error sending file : " + filePath + ": " + ex.toString() );
        }
        return file_header_fields[3];
    }

    public abstract void postArray( String header, byte[] data );
    public abstract void execUtility( String[] utilSpec );

    public abstract void execute( String[] taskSpec );
    public abstract void shutdown();
    public abstract void getCapabilities( String[] utilSpec );
    public abstract void describeProcess( String[] utilSpec );

    public String getHostInfo() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return String.format( "%s (%s)", ip.getHostName(), ip.getHostAddress() );
        } catch (UnknownHostException e) { return "UNKNOWN"; }
    }

    public void run() {
        String parts[] = {"","",""};
        while( active ) try {
            logger.info( String.format( "Listening for requests on port: %d, host: %s",  request_port, getHostInfo() ) );
            String request_header = new String(request_socket.recv(0)).trim();
            parts = request_header.split("[!]");
            logger.info( String.format( "  ###  Processing %s request: %s",  parts[1], request_header ) );
            if( parts[1].equals("array") ) {
                logger.info("Waiting for result data ");
                byte[] data = request_socket.recv(0);
                postArray(request_header, data);
            } else if( parts[1].equals("execute") ) {
                execute( parts );
            } else if( parts[1].equals("util") ) {
                execUtility( parts );
            } else if( parts[1].equals("quit") || parts[1].equals("shutdown") ) {
                term();
            } else if( parts[1].equals("getCapabilities") ) {
                getCapabilities( parts );
            } else if( parts[1].equals("describeProcess") ) {
                describeProcess( parts );
            } else {
                logger.info( "Unknown request header type: " + parts[0] );
            }
        } catch ( java.nio.channels.ClosedSelectorException ex ) {
            logger.info( "Request Socket closed." );
            active = false;
        } catch ( Exception ex ) {
            logger.error( "Error in Request: " + ex.toString() );
            ex.printStackTrace();
            sendErrorReport( parts, ex );
        }
    }

    public void term() {
        logger.info( "EDAS Shutdown");
        active = false;
        PythonWorkerPortal.getInstance().quit();
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        shutdown();
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

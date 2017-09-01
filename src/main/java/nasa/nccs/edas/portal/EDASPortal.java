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

class Response {
    String id = null;
    String rtype = null;

    public Response( String _rtype, String _id )  {
        rtype = _rtype;
        id = _id;
    }
}

class Message extends Response {
    String message = null;
    public Message( String _id, String _message ) {
        super( "message", _id );
        message = _message;
    }
    public String toString() { return "Message[" + id + "]: " + message; }
}

class ErrorReport extends Response {
    String message = null;
    public ErrorReport( String _id, String _message ) {
        super( "error", _id );
        message = _message;
    }
    public String toString() { return "ErrorReport[" + id + "]: " + message; }
}

class DataPacket extends Response {
    String header = null;
    byte[] data = null;
    public DataPacket( String _id, String _header, byte[] _data ) {
        super( "data", _id );
        header = _header;
        data = _data;
    }
    public String toString() { return "DataPacket[" + header + "]"; }
}


class Responder extends Thread {
    ZMQ.Context context = null;
    Boolean active = true;
    int response_port = -1;
    Logger logger = EDASLogManager.getCurrentLogger();
    String client_address = null;
    ConcurrentLinkedQueue<Response> response_queue = null;
    HashMap<String,String> status_reports = null;

    public Responder(ZMQ.Context _context, String _client_address, int _response_port ) {
        context =  _context;
        response_port = _response_port;
        response_queue = new ConcurrentLinkedQueue<Response>();
        status_reports = new HashMap<String,String>();
        client_address = _client_address;
    }

    public void sendResponse( Response msg ) {
        logger.info( "Post Message to response queue: " + msg.id );
        response_queue.add( msg );
    }

    public void sendDataPacket( DataPacket data ) {
        logger.info( "Post DataPacket to response queue: " + data.toString() );
        response_queue.add( data );
    }

    void doSendResponse( ZMQ.Socket socket, Response r ) {
        if( r.rtype == "message" ) { doSendMessage( socket, (Message)r ); }
        else if( r.rtype == "data" ) { doSendDataPacket( socket, (DataPacket)r ); }
        else if( r.rtype == "error" ) { doSendErrorReport( socket, (ErrorReport)r ); }
        else {
            logger.error( "Error, unrecognized response type: " + r.rtype );
            doSendErrorReport( socket, new ErrorReport( r.id, "Error, unrecognized response type: " + r.rtype ) );
        }
    }

    void doSendMessage( ZMQ.Socket socket, Message msg  ) {
        List<String> request_args = Arrays.asList( msg.id, "response", msg.message );
        String packaged_msg = StringUtils.join( request_args,  "!" );
        socket.send( packaged_msg.getBytes() );
        logger.info( " Sent response: " + msg.id + ", content sample: " + packaged_msg.substring( 0, Math.min( 300, packaged_msg.length() ) ) );
    }

    void doSendErrorReport( ZMQ.Socket socket, ErrorReport msg  ) {
        List<String> request_args = Arrays.asList( msg.id, "error", msg.message );
        socket.send( StringUtils.join( request_args,  "!" ).getBytes() );
        logger.info( " Sent error report: " + msg.id + ", content: " + msg.message );
    }

    void doSendDataPacket( ZMQ.Socket socket, DataPacket dataPacket ) {
        socket.send( dataPacket.header.getBytes() );
        socket.send( dataPacket.data );
        logger.info( " Sent data packet " + dataPacket.id + ", header: " + dataPacket.header );
    }

    public void setExeStatus( String rid, String status ) {
        status_reports.put(rid,status);
    }

    void heartbeat(ZMQ.Socket socket) {
        Message hb_msg = new Message( "status", status_reports.toString() );
        doSendMessage( socket, hb_msg );
    }

    public void run()  {
        int pause_time = 100;
        int accum_sleep_time = 0;
        int heartbeat_interval = 2000;
        ZMQ.Socket socket  = context.socket(ZMQ.PUSH);
        try{
            socket.bind(String.format("tcp://%s:%d", client_address, response_port));
            logger.info( String.format("Bound response socket to client at %s on port: %d", client_address, response_port) );
        } catch (Exception err ) { logger.error( String.format("Error initializing response socket on port %d: %s", response_port, err ) ); }
        try {
            while (active) {
                Response response = response_queue.poll();
                if (response == null) {
                    Thread.sleep(pause_time);
                    accum_sleep_time += pause_time;
                    if( accum_sleep_time >= heartbeat_interval ) {
                        heartbeat( socket );
                        accum_sleep_time = 0;
                    }
                } else {
                    doSendResponse(socket,response);
                    accum_sleep_time = 0;
                }
            }
        } catch ( InterruptedException ex ) {;}

        try { socket.close(); }
        catch( Exception err ) { ; }
    }

    public void term() { active = false; }

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
            zmqContext = ZMQ.context(2);
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
        responder.sendResponse( new Message(id,msg) );
    }

    public void sendErrorReport(  String id, String msg ) {
        logger.info("-----> SendErrorReport[" + id + "]" );
        responder.sendResponse( new ErrorReport(id,msg) );
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
        responder.sendDataPacket( new DataPacket( rid, header, data ) );
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
            responder.sendDataPacket( new DataPacket( rId, header, data ) );
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
        logger.info( "EXIT EDASPortal");
    }

    public void term() {
        logger.info( "EDAS Shutdown");
        active = false;
        PythonWorkerPortal.getInstance().quit();
        logger.info( "QUIT PythonWorkerPortal");
        try { request_socket.close(); }  catch ( Exception ex ) { ; }
        logger.info( "CLOSE request_socket");
        responder.term();
        logger.info( "TERM responder");
        shutdown();
        logger.info( "shutdown complete");
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

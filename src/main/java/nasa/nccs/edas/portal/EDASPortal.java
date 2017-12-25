package nasa.nccs.edas.portal;
import com.google.common.io.Files;
import nasa.nccs.edas.loaders.CollectionLoadServices;
import nasa.nccs.edas.workers.python.PythonWorkerPortal;
import nasa.nccs.utilities.Logger;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.zeromq.ZMQ;
import nasa.nccs.utilities.EDASLogManager;
import ucar.nc2.time.CalendarDate;
import nasa.nccs.edas.loaders.Collections;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.net.InetAddress;
import java.net.UnknownHostException;

class Response {
    String clientId = null;
    String responseId = null;
    String rtype = null;
    String _body = null;
    String id() { return clientId + ":" + responseId; }
    String message() { return _body; }

    public Response( String _rtype, String _clientId, String _responseId )  {
        rtype = _rtype;
        clientId = _clientId;
        responseId = _responseId;
    }
}

class Message extends Response {
    public Message( String _clientId, String _responseId, String _message ) {
        super( "message", _clientId, _responseId );
        _body = _message;
    }
    public String toString() { return "Message[" + id() + "]: " + _body; }
}

class ErrorReport extends Response {
    public ErrorReport( String _clientId, String _responseId, String _message ) {
        super( "error", _clientId, _responseId );
        _body = _message;
    }
    public String toString() { return "ErrorReport[" + id() + "]: " + _body; }
}

class DataPacket extends Response {
    private byte[] _data = null;
    public DataPacket( String client_id, String response_id, String header, byte[] data  ) {
        super( "data",  client_id, response_id );
        _body =  header;
        _data = data;
    }
    public DataPacket( String client_id, String response_id, String header  ) {
        super( "data",  client_id, response_id );
        _body =  header;
    }
    boolean hasData() { return (_data != null); }
    byte[] getTransferHeader() { return (clientId + ":" + _body).getBytes(); }
    String getHeaderString() { return _body; }

    byte[] getTransferData() { return concat(clientId.getBytes(), _data ); }
    byte[] getRawData() { return _data; }
    public String toString() { return "DataPacket[" + _body + "]"; }

    static byte[] concat( byte[] a, byte[] b ) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}


class Responder extends Thread {
    ZMQ.Context context = null;
    Boolean active = true;
    int response_port = -1;
    Logger logger = EDASLogManager.getCurrentLogger();
    String client_address = null;
    String clientId = null;
    ConcurrentLinkedQueue<Response> response_queue = null;
    HashMap<String,String> status_reports = null;

    public Responder(ZMQ.Context _context, String _client_address, int _response_port ) {
        context =  _context;
        response_port = _response_port;
        response_queue = new ConcurrentLinkedQueue<Response>();
        status_reports = new HashMap<String,String>();
        client_address = _client_address;
    }

    public void setClientId( String id ) { clientId = id; }
    public void clearClientId() { clientId = null; }

    public void sendResponse( Response msg ) {
        logger.info( "Post Message to response queue: " + msg.toString() );
        response_queue.add( msg );
    }

    public void sendDataPacket( DataPacket data ) {
        logger.info( "Post DataPacket to response queue: " + data.toString() );
        response_queue.add( data );
    }

    void doSendResponse(ZMQ.Socket socket, Response r ) {
        if( r.rtype == "message" ) {
            String packaged_msg = doSendMessage( socket, (Message)r );
            DateTime dateTime = new DateTime( CalendarDate.present().toDate() );
            logger.info( " Sent response: " + r.id() + " (" + dateTime.toString("MM/dd HH:mm:ss") + "), content sample: " + packaged_msg.substring( 0, Math.min( 300, packaged_msg.length() ) ) );
        }
        else if( r.rtype == "data" ) { doSendDataPacket( socket, (DataPacket)r ); }
        else if( r.rtype == "error" ) { doSendErrorReport( socket, (ErrorReport)r ); }
        else {
            logger.error( "Error, unrecognized response type: " + r.rtype );
            doSendErrorReport( socket, new ErrorReport( r.clientId, r.responseId, "Error, unrecognized response type: " + r.rtype ) );
        }
    }

    String doSendMessage( ZMQ.Socket socket, Message msg  ) {
        List<String> request_args = Arrays.asList( msg.id(), "response", msg.message() );
        String packaged_msg = StringUtils.join( request_args,  "!" );
//        logger.info( " Sending message to client, msgId=" + msg.id() + ", content: " + packaged_msg );
        socket.send( packaged_msg.getBytes() );
        return packaged_msg;
    }

    void doSendErrorReport( ZMQ.Socket socket, ErrorReport msg  ) {
        List<String> request_args = Arrays.asList( msg.id(), "error", msg.message() );
        socket.send( StringUtils.join( request_args,  "!" ).getBytes() );
        logger.info( " Sent error report: " + msg.id() + ", content: " + msg.message() );
    }

    void doSendDataPacket( ZMQ.Socket socket, DataPacket dataPacket ) {
        socket.send( dataPacket.getTransferHeader() );
        if( dataPacket.hasData() ) { socket.send( dataPacket.getTransferData() ); }
        logger.info( " Sent data packet " + dataPacket.id() + ", header: " + dataPacket.getHeaderString() );
    }

    public void setExeStatus( String rid, String status ) {
        status_reports.put(rid,status);
    }

    void heartbeat(ZMQ.Socket socket) {
        if( clientId != null ) {
            Message hb_msg = new Message( clientId, "status", status_reports.toString() );
            doSendMessage(socket, hb_msg);
        }
    }

    public void run()  {
        int pause_time = 100;
        int accum_sleep_time = 0;
        int heartbeat_interval = 2000;
        ZMQ.Socket socket  = context.socket(ZMQ.PUB);
        try{
            socket.bind(String.format("tcp://%s:%d", client_address, response_port));
            logger.info( String.format(" --> Bound response socket to client at %s on port: %d", client_address, response_port) );
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

    public void term() {
        logger.info("Terminating responder thread");
        active = false;
    }

}

public abstract class EDASPortal {
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected int request_port = -1;
    protected Responder responder = null;
    protected SimpleDateFormat timeFormatter = new SimpleDateFormat("MM/dd HH:mm:ss");
    protected Logger logger = EDASLogManager.getCurrentLogger();
    private boolean active = true;

    public abstract void sendErrorReport( String[] taskSpec,  Exception err  );
    protected EDASPortal( String client_address, int _request_port, int _response_port ) {
        try {
            request_port = _request_port;
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.REP);
            responder = new Responder( zmqContext, client_address, _response_port);
            responder.setDaemon(true);
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
                logger.info(String.format(" --> Bound request socket to client at %s on port: %d", client_address, request_port));
            } catch (Exception err ) { logger.error( String.format("Error initializing request socket on port %d: %s", request_port, err ) ); }

        } catch (Exception err ) {
            logger.error( String.format("\n-------------------------------\nEDAS Init error: %s -------------------------------\n", err ) );
        }
    }

//    public void sendResponse(  String clientId, String responseId, String msg ) {
//        responder.sendResponse( new Message(clientId,responseId,msg) );
//    }

    public void sendErrorReport(  String clientId, String responseId, String msg ) {
        logger.info("-----> SendErrorReport[" + clientId +":" + responseId + "]" );
        responder.sendResponse( new ErrorReport(clientId,responseId,msg) );
    }

    public void setExeStatus( String rid, String status ) {
        responder.setExeStatus(rid,status);
    }

    public void sendArrayData( String clientId, String rid, int[] origin, int[] shape, byte[] data, Map<String, String> metadata ) {
        logger.debug( String.format("@@ Portal: Sending response data to client for rid %s, nbytes=%d", rid, data.length ));
        List<String> array_header_fields = Arrays.asList( "array", rid, ia2s(origin), ia2s(shape), m2s(metadata), "1" );
        String array_header = StringUtils.join(array_header_fields,"|");
        List<String> header_fields = Arrays.asList( rid, "array", array_header );
        String header = StringUtils.join(header_fields,"!");
        logger.debug("Sending header: " + header);
        responder.sendDataPacket( new DataPacket( clientId, rid, header, data ) );
    }

    public String sendFile( String clientId, String jobId, String name, String filePath, Boolean sendData  ) {
        logger.debug( String.format("Portal: Sending file data to client for %s, filePath=%s", name, filePath ));
        File file = new File(filePath);
        ArrayList<String> file_header_fields = new ArrayList<>( Arrays.asList( "array", jobId, name, file.getName() ) );
        if( !sendData ) { file_header_fields.add(filePath); }
        String file_header = StringUtils.join( file_header_fields, "|" );
        List<String> header_fields = Arrays.asList( jobId,"file", file_header );
        String header = StringUtils.join(header_fields,"!");
        try {
            byte[] data = sendData ? Files.toByteArray( file ) : null;
            logger.debug(" ##sendDataPacket: clientId=" + clientId + " jobId=" + jobId + " name=" + name + " path=" + filePath );
            responder.sendDataPacket( new DataPacket( clientId, jobId, header, data ) );
            logger.debug("Done sending file data packet: " + header);
        } catch ( IOException ex ) {
            logger.info( "Error sending file : " + filePath + ": " + ex.toString() );
        }
        return file.getName();
    }

    public abstract Message execUtility( String[] utilSpec );
    public abstract Response execute( String[] taskSpec );
    public abstract void shutdown();
    public abstract Message getCapabilities( String[] utilSpec );
    public abstract Message describeProcess( String[] utilSpec );

    public String sendResponseMessage( Response msg ) {
        List<String> request_args = Arrays.asList( msg.id(), msg.message() );
        String packaged_msg = StringUtils.join( request_args,  "!" );
        String timeStamp = timeFormatter.format( Calendar.getInstance().getTime() );
        logger.info( String.format( "@@ Sending response %s on request_socket @(%s): %s", msg.responseId, timeStamp, msg.toString() ) );
        request_socket.send( packaged_msg.getBytes(),0 );
        return packaged_msg;
    }

    public static String getCurrentStackTrace() {
        try{ throw new Exception("Current"); } catch(Exception ex)  {
            Writer result = new StringWriter();
            PrintWriter printWriter = new PrintWriter(result);
            ex.printStackTrace(printWriter);
            return result.toString();
        }
    }

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
            try {
                String timeStamp = timeFormatter.format(Calendar.getInstance().getTime());
                logger.info(String.format("  ###  Processing %s request: %s @(%s)", parts[1], request_header, timeStamp));
                if (parts[1].equals("execute")) {
                    sendResponseMessage(execute(parts));
                } else if (parts[1].equals("util")) {
                    sendResponseMessage(execUtility(parts));
                } else if (parts[1].equals("quit") || parts[1].equals("shutdown")) {
                    sendResponseMessage(new Message(parts[0], "quit", "Terminating"));
                    logger.info("Received Shutdown Message");
                    System.exit(0);
                } else if (parts[1].toLowerCase().equals("getcapabilities")) {
                    sendResponseMessage(getCapabilities(parts));
                } else if (parts[1].toLowerCase().equals("describeprocess")) {
                    sendResponseMessage(describeProcess(parts));
                } else {
                    String msg = "Unknown request header type: " + parts[1];
                    logger.info(msg);
                    sendResponseMessage(new Message(parts[0], "error", msg));
                }
            } catch ( Exception ex ) {   // TODO: COnvert to Java
//                String clientId = elem(taskSpec,0)
//                val runargs = getRunArgs( taskSpec )
//                String jobId = runargs.getOrElse("jobId",randomIds.nextString)
//                sendResponseMessage( error_response );
            }
        } catch ( Exception ex ) {
            logger.info( "Request Communication error: Shutting down." );
            active = false;
        }
        logger.info( "EXIT EDASPortal");
    }


    public void term(String msg) {
        logger.info( "!!EDAS Shutdown: " + msg );
        active = false;
        try { CollectionLoadServices.term(); }  catch ( Exception ex ) { ; }
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

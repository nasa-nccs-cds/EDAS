package nasa.nccs.edas.portal;
import nasa.nccs.edas.workers.TransVar;
import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Logger;
import org.apache.commons.lang.StringUtils;
import org.zeromq.ZMQ;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

class RandomString {

    private static final char[] symbols;

    static {
        StringBuilder tmp = new StringBuilder();
        for (char ch = '0'; ch <= '9'; ++ch)
            tmp.append(ch);
        for (char ch = 'a'; ch <= 'z'; ++ch)
            tmp.append(ch);
        for (char ch = 'A'; ch <= 'Z'; ++ch)
            tmp.append(ch);
        symbols = tmp.toString().toCharArray();
    }

    private final Random random = new Random();

    private final char[] buf;

    public RandomString(int length) {
        if (length < 1)
            throw new IllegalArgumentException("length < 1: " + length);
        buf = new char[length];
    }

    public String nextString() {
        for (int idx = 0; idx < buf.length; ++idx)
            buf[idx] = symbols[random.nextInt(symbols.length)];
        return new String(buf);
    }
}


public class EDASPortalClient {
    protected int MB = 1024 * 1024;
    protected static int DefaultPort = 5670;
    protected Logger logger = EDASLogManager.getCurrentLogger();
    protected ZMQ.Context zmqContext = null;
    protected ZMQ.Socket request_socket = null;
    protected String app_host = "";
    protected String clientId = "";
    protected Map<String,String> configuration = null;
    protected ResponseManager response_manager = null;
    protected RandomString randomIds = new RandomString(8);
    protected int request_port = -1;
    protected int response_port = -1;
    protected SimpleDateFormat timeFormatter = new SimpleDateFormat("MM/dd HH:mm:ss");

    static String getOrDefault( Map<String,String> map, String key, String defvalue ) {
        String result = map.get(key);
        return (result == null) ? defvalue : result;
    }
    public String getConfiguration( String key, String default_value ) { return getOrDefault(configuration,key,default_value ); }
    public String getConfiguration( String key ) { return configuration.get(key); }
    public void del() { shutdown(); }

//    static int MAX_PORT = 65535;
//    public static int bindSocket(ZMQ.Socket socket, String server, int port) {
//        int test_port = (port > 0) ? port : DefaultPort;
//        while (true) {
//            try {
//                socket.bind(String.format("tcp://%s:%d", server, port));
//                return test_port;
//            } catch (Exception err) {
//                if( test_port >= MAX_PORT ) { return -1; }
//                test_port = test_port + 1;
//            }
//        }
//    }

    public static int connectSocket(ZMQ.Socket socket, String host, int port) {
        socket.connect(String.format("tcp://%s:%d", host, port));
        return port;
    }

    public EDASPortalClient( Map<String,String> portal_config ) {
        try {
            configuration = portal_config;
            clientId = randomIds.nextString();
            request_port = Integer.parseInt( getOrDefault(configuration,"edas.server.port.request","5670" ) );
            response_port = Integer.parseInt( getOrDefault(configuration,"edas.server.port.response","5671" ) );
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.REQ);
            app_host = getOrDefault(configuration,"edas.server.address","localhost" );
            connectSocket(request_socket, app_host, request_port );
            logger.info( String.format("[2]Connected request socket to server %s on port: %d",app_host, request_port) );
            logger.info( String.format("Connected response socket on port: %d", response_port ) );
            logger.info( String.format("Starting EDASPortalClient with server = %s", app_host ) );


//            if (connectionMode == ConnectionMode.BIND) {
//                request_port = bindSocket(request_socket, app_host, request_port);
//                response_port = bindSocket(response_socket, app_host,  response_port);
//                logger.info(String.format("Binding request socket to port: %d",request_port));
//                logger.info(String.format("Binding response socket to port: %d",response_port));
//            } else {
//                request_port = connectSocket(request_socket, app_host, request_port);
//                response_port = connectSocket(response_socket, app_host, response_port);
//                logger.info(String.format("Connected request socket to server %s on port: %d",app_host, request_port));
//                logger.info(String.format("Connected response socket on port: %d",response_port));
//            }


        } catch(Exception err) {
            String err_msg = String.format("\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n", err.getMessage(), err.getStackTrace().toString() );
            logger.error(err_msg);
            shutdown();
        }
    }


    public ResponseManager createResponseManager() {
        String socket_address = String.format("tcp://%s:%d", app_host, response_port );
        logger.info("Creating ResponseManager, socket_address = " + socket_address );
        response_manager = new ResponseManager( zmqContext, clientId, socket_address, configuration );
        response_manager.setDaemon(true);
        response_manager.start();
        return response_manager;
    }

    public Path getFileCacheDir(String role) {
        return response_manager.getFileCacheDir(role);
    }

    public void shutdown() {
        logger.info(" ############################## SHUT DOWN EDAS PORTAL ##############################");
        try { request_socket.close(); }
        catch ( Exception err ) {;}
        if( response_manager != null ) {
            response_manager.term();
        }
    }

    public String timestamp() { return timeFormatter.format( Calendar.getInstance().getTime() ); }

    public String sendMessage( String type, String[] mDataList ) {
        String[] msgElems = new String[ mDataList.length + 2 ];
        String response = "";
        msgElems[0] = clientId;
        msgElems[1] = type;
        String message = null;
        for (int i = 0; i < mDataList.length; i++) { msgElems[i+2] = mDataList[i].replace("'", "\"" ); }
        try {
            message = StringUtils.join( msgElems, "!");
            logger.info( String.format( "Sending %s request '%s' on port %d @(%s)", type, message, request_port, timestamp() ) );
            request_socket.send(message.getBytes(),0);
            response = new String( request_socket.recv(0) );
            logger.info( String.format( "Received request response, sample: { %s } @(%s)", response.substring(0,Math.min(100,response.length())), timestamp() ) );
        } catch ( Exception err ) { logger.error( String.format( "Error sending message %s on request socket: %s", message, err.getMessage() )); }
        return response;
    }
}







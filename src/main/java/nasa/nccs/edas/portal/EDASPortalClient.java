package nasa.nccs.edas.portal;
import nasa.nccs.edas.workers.TransVar;
import nasa.nccs.utilities.EDASLogManager;
import nasa.nccs.utilities.Logger;
import org.apache.commons.lang.StringUtils;
import org.zeromq.ZMQ;
import nasa.nccs.edas.portal.EDASPortal.ConnectionMode;
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
    protected ZMQ.Socket response_socket = null;
    protected String app_host = null;
    protected ResponseManager response_manager = null;
    protected RandomString randomIds = new RandomString(8);
    protected int _request_port = -1;
    protected int _response_port = -1;


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

    public EDASPortalClient( String server, int request_port, int response_port ) {
        try {
            _request_port = request_port;
            _response_port = response_port;
            zmqContext = ZMQ.context(1);
            request_socket = zmqContext.socket(ZMQ.PUSH);
            response_socket = zmqContext.socket(ZMQ.PULL);
            app_host = server;
            request_port = connectSocket(request_socket, app_host, request_port);
            response_port = connectSocket(response_socket, app_host, response_port);
            logger.info(String.format("[2]Connected request socket to server %s on port: %d",app_host, request_port));
            logger.info(String.format("Connected response socket on port: %d",response_port));


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

    public void del() {
        shutdown();
    }

    public ResponseManager createResponseManager() {
        response_manager = new ResponseManager(this);
        response_manager.start();
        return response_manager;
    }

    public void shutdown() {
        logger.info(" ############################## SHUT DOWN EDAS PORTAL ##############################");
        try { request_socket.close(); }
        catch ( Exception err ) {;}
        if( response_manager != null ) {
            response_manager.term();
        }
    }

    public String sendMessage( String type, String[] mDataList ) {
        String msgId = randomIds.nextString();
        String[] msgElems = new String[ mDataList.length + 2 ];
        msgElems[0] = msgId;
        msgElems[1] = type;
        String message = null;
        for (int i = 0; i < mDataList.length; i++) { msgElems[i+2] = mDataList[i].replace("'", "\"" ); }
        try {
            message = StringUtils.join( msgElems, "!");
            logger.info( String.format( "Sending %s request '%s' on port %d.", type, message, _request_port ) );
            request_socket.send(message.getBytes(),0);
        } catch ( Exception err ) { logger.error( String.format( "Error sending message %s on request socket: %s", message, err.getMessage() )); }
        return msgId;
    }
}







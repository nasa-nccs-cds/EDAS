import zmq, traceback, time, logging, xml, cdms2
from threading import Thread
from pyedas.edasArray import npArray
from cdms2.variable import DatasetVariable
import random, string, os
MB = 1024 * 1024

class ConnectionMode():
    BIND = 1
    CONNECT = 2
    DefaultPort = 4336

    @classmethod
    def bindSocket( cls, socket, server_address, port ):
        test_port = port if( port > 0 ) else cls.DefaultPort
        while( True ):
            try:
                socket.bind( "tcp://{0}:{1}".format(server_address,test_port) )
                return test_port
            except Exception as err:
                test_port = test_port + 1

    @classmethod
    def connectSocket( cls, socket, host, port ):
        socket.connect("tcp://{0}:{1}".format( host, port ) )
        return port

class ResponseManager(Thread):

    def __init__(self, edasPortal ):
        Thread.__init__(self)
        self.socket = edasPortal.response_socket
        self.logger = edasPortal.logger
        self.active = True
        self.setName('EDAS Response Thread')
        self.cached_results = {}
        self.cached_arrays = {}
        self.filePaths = {}
        self.setDaemon(True)
        self.cacheDir = os.environ.get( 'EDAS_CACHE_DIR','/tmp/' )
        self.log("Created RM")

    def cacheResult(self, id, result ):
        self.logger.info( "Caching result array: " + id )
        self.getResults(id).append(result)

    def getResults(self, id ):
        return self.cached_results.setdefault(id,[])

    def cacheArray(self, id, array ):
        print( "Caching array: " + id )
        self.getArrays(id).append(array)

    def getArrays(self, id ):
        return self.cached_arrays.setdefault(id,[])

    def run(self):
        self.log("Run RM thread")
        while( self.active ):
            print "."
            self.processNextResponse()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def term(self):
        if self.active:
            self.active = False;

    def popResponse(self):
        if( len( self.cached_results ) == 0 ):
            return None
        else:
            return self.cached_results.pop()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def log(self, msg, maxPrintLen = 300 ):
        self.logger.info( "[RM] " + msg )
        print "[RM] " + msg[0:maxPrintLen]


    #    def getResponse(self, key, default = None ):
    #       return self.cached_results.get( key, default )

    def getItem(self, str_array, itemIndex, default_val="NULL" ):
        try: return str_array[itemIndex]
        except Exception as err: return default_val

    def processNextResponse(self):
        try:
            response = self.socket.recv()
            toks = response.split('!')
            rId = self.getItem( toks, 0 )
            type = self.getItem( toks, 1 )
            msg = self.getItem(toks, 2)
            self.log(" #### Received response, rid: " + rId + ", type: " + type )
            if type == "array":
                self.log( "\n\n #### Received array " + rId + ": " + msg )
                data = self.socket.recv()
                array = npArray.createInput(msg,data)
                self.logger.info("Received array: {0}".format(rId))
                self.cacheArray( rId, array )
            elif type == "file":
                data = self.socket.recv()
                filePath = self.saveFile( msg, data )
                self.filePaths[rId] = filePath
                self.log("Received file '{0}' for rid {1}".format(msg,rId))
            elif type == "error":
                self.log(  "\n\n #### ERROR REPORT " + rId + ": " + msg )
                print " *** Execution Error Report: " + msg
                self.cacheResult( rId, msg )
            elif type == "response":
                if rId == "status":
                    print " *** Execution Status Report: " + msg
                else:
                    self.log(  " Caching response message " + rId  + ", sample: " + msg[0:300] )
                    self.cacheResult( rId, msg )
            else:
                self.log(" #### EDASPortal.ResponseThread-> Received unrecognized message type: {0}".format(type))

        except Exception as err:
            self.log( "EDAS error: {0}\n{1}\n".format(err, traceback.format_exc() ), 1000 )

    def getFileCacheDir(self,role):
        filePath = os.path.join( self.cacheDir, "transfer", role )
        if not os.path.exists(filePath): os.makedirs(filePath)
        return filePath

    def saveFile(self, header, data):
        header_toks = header.split('|')
        id = header_toks[1]
        role = header_toks[2]
        fileName = header_toks[3]
        filePath = os.path.join( self.getFileCacheDir(role), fileName )
        with open( filePath, mode='wb') as file:
            file.write( bytearray(data) )
            self.log(" ***->> Saving File, path = {0}".format(filePath) )
        return filePath


    def getResponses( self, rId, wait=True ):
        import subprocess, sys
        self.log(  "Waiting for a response from the server... " )
        count = 0
        while( True ):
            results = self.getResults(rId)
            if( (len(results) > 0) or not wait): return results
            else:
                print ".",
                time.sleep(0.25)
                if( count % 4 == 0 ):
                    self.logger.info( " ** Waiting, secs: " + str(count) )
#                    subprocess.call( [ 'free', '-h' ], stdout=sys.stdout )
                count = count + 1

    def getResponseVariables(self, rId, wait=True):
        """  :rtype: list[DatasetVariable] """
        responses = self.getResponses( rId, wait )
        gridFileDir = self.getFileCacheDir("gridfile")
        vars = []
        for response in responses:
            self.log( "Processing response node: " + response )
            e = xml.etree.ElementTree.fromstring( response )
            for data_node in e.iter('data'):
                resultUri = data_node.get("href","")
                if resultUri:
                    self.log( "Processing response: " + resultUri )
                    resultId = resultUri.split("/")[-1]
                    result_arrays = self.cached_arrays.get( resultId, [] )
                    if result_arrays:
                        for result_array in result_arrays:
                            gridFilePath = os.path.join(gridFileDir, result_array.gridFile )
                            vars.append( result_array.getVariable( gridFilePath ) )
                    else:
                        from cdms2.dataset import Dataset
                        resultFilePath = self.filePaths.get( rId, data_node.get("file", "") )
                        self.log( "Processing file: " + resultFilePath )
                        if resultFilePath:
                            dset = cdms2.open( resultFilePath ); """:type : Dataset """
                            for fvar in dset.variables:
                                vars.append( dset(fvar) )
                        else:
                            self.logger.error( "Empty response node: " + str(data_node) )
                else :
                    self.logger.error( "Empty response node: " + str(data_node) )

            return vars

class EDASPortal:

    def __init__( self, host="127.0.0.1", request_port=0, response_port=0, **kwargs ):
        try:
            self.active = True
            self.logger =  logging.getLogger("portal")
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PUSH)
            self.response_socket = self.context.socket(zmq.PULL)
            self.app_host = host

            # if( connectionMode == ConnectionMode.BIND ):
            #     self.request_port = ConnectionMode.bindSocket( self.request_socket, self.app_host, request_port )
            #     self.response_port = ConnectionMode.bindSocket( self.response_socket, self.app_host, response_port )
            #     self.logger.info( "Binding request socket to port: {0} (Requested {1})".format( self.request_port, request_port ) )
            #     self.logger.info( "Binding response socket to port: {0} (Requested {1}".format( self.response_port, response_port ) )
            # else:

            self.request_port = ConnectionMode.connectSocket(self.request_socket, self.app_host, request_port)
            self.response_port = ConnectionMode.connectSocket(self.response_socket, self.app_host, response_port)
            self.log("[3]Connected request socket to server {0} on port: {1}".format( self.app_host, self.request_port ) )
            self.log( "Connected response socket on port: {0}".format( self.response_port ) )

            self.response_manager = None
            self.application_thread = None

        except Exception as err:
            err_msg =  "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() )
            self.logger.error(err_msg)
            print err_msg
            self.shutdown()

    def log(self, msg ):
        self.logger.info( "[P] " + msg )
        print msg

    def __del__(self): self.shutdown()

    def start_EDAS(self):  # Stage the EDAS app using the "{EDAS_HOME}>> sbt stage" command.
        self.application_thread = AppThread( self.app_host, self.request_port, self.response_port )
        self.application_thread.start()

    def createResponseManager(self):
        self.response_manager = ResponseManager(self)
        self.log( "Starting ResponseManager thread" )
        self.response_manager.start()
        return self.response_manager

    def shutdown(self):
        if self.active:
            self.active = False
            self.log(  " ############################## Disconnect Portal Client from Server & shutdown Client ##############################"  )
            try: self.request_socket.close()
            except Exception: pass
            if( self.application_thread ):
                self.sendMessage("shutdown")
                self.application_thread.term()
            if self.response_manager != None:
                self.log(  " Terminate Response Manager " )
                self.response_manager.term()
                self.log(  " Completed shutdown " )

    def randomId(self, length):
        sample = string.lowercase+string.digits+string.uppercase
        return ''.join(random.choice(sample) for i in range(length))

    def sendMessage( self, type, mDataList = [""] ):
        msgId = self.randomId(8)
        msgStrs = [ str(mData).replace("'",'"') for mData in mDataList ]
        self.logger.info( "Sending {0} request {1} on port {2}.".format( type, msgStrs, self.request_port )  )
        try:
            message = "!".join( [msgId,type] + msgStrs )
            self.request_socket.send( message )
        except zmq.error.ZMQError as err:
            self.logger.error( "Error sending message {0} on request socket: {1}".format( message, str(err) ) )
        return msgId

class AppThread(Thread):
    def __init__(self, host, request_port, response_port):
        Thread.__init__(self)
        self.logger = logging.getLogger("portal")
        self._response_port = response_port
        self._request_port = request_port
        self._host = host
        self.process = None
        self.setDaemon(True)

    def run(self):
        import subprocess, shlex, os
        from psutil import virtual_memory
        try:
            mem = virtual_memory()
            total_ram = mem.total / MB
            EDAS_DRIVER_MEM = os.environ.get( 'EDAS_DRIVER_MEM', str( total_ram - 1000 ) + 'M' )
            edas_startup = "edas connect {0} {1} -J-Xmx{2} -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC".format( self._request_port, self._response_port, EDAS_DRIVER_MEM )
            self.process = subprocess.Popen(shlex.split(edas_startup))
            print "Staring EDAS with command: {0}, total RAM: {1}\n".format( edas_startup, mem.total )
            self.process.wait()
        except KeyboardInterrupt as ex:
            print "  ----------------- EDAS TERM ----------------- "
            self.process.kill()

    def term(self):
        if(self.process != None):
            self.process.terminate()

    def join(self):
        self.process.wait()

#if __name__ == "__main__":
#    env_test = "echo $CLASSPATH"
#    process = Popen( env_test, shell=True, executable="/bin/bash" )

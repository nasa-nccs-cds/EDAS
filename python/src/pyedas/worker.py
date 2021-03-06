import os, traceback
import zmq, cdms2, logging
import platform, socket
import numpy as np
from messageParser import mParse
from edasArray import npArray, IO_DType
from kernels.OperationsManager import edasOpManager
from task import Task

class Worker(object):

    def __init__( self, request_port, result_port ):
        self.cached_results = {};                   """ :type : dict[str,npArray] """
        self.cached_inputs = {};                    """ :type : dict[str,npArray] """
        self.logger = logging.getLogger()
        try:
            self.context = zmq.Context()
            self.request_socket = self.context.socket(zmq.PULL)
            self.request_socket.connect("tcp://localhost:" + str(request_port) )
            self.logger.info( "[4]Connected request socket on port: {0}".format( request_port ) )
            self.result_socket = self.context.socket(zmq.PUSH)
            self.result_socket.connect("tcp://localhost:" + str(result_port) )
            self.logger.info( "Connected result socket on port: {0}".format( result_port ) )
        except Exception as err:
            self.logger.error( "\n-------------------------------\nWorker Init error: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )


    def __del__(self): self.shutdown()

    def shutdown(self):
        self.request_socket.close()
        self.result_socket.close()

    def getMessageField(self, header, index ):
        toks = header.split('|')
        return toks[index]

    def run(self):
        active = True
        while active:
            header = self.request_socket.recv()
            type = self.getMessageField(header,0)
            self.logger.info( " Received '{0}' message: {1}".format( type, header ) )
            if type == "array":
                try:
                    withData = int(self.getMessageField(header,5))
                    data = self.request_socket.recv() if withData else None
                    self.logger.info(" @@@ Creating data input '{0}', withData: {1}".format( header, withData ) )
                    array = npArray.createInput(header,data)
                    if( withData ):
                        self.logger.info(" @@@ Array shape: '{0}', data is None: {1}".format( mParse.ia2s(array.shape), str(data is None) ) )
                    self.cached_inputs[array.uid()] = array
                except Exception as err:
                    self.sendError( err )

            elif type == "task":
                try:
                    resultVars = self.processTask( Task(header) )
                    for resultVar in resultVars:
                        self.sendVariableData( resultVar )
                except Exception as err:
                    self.sendError( err )

            elif type == "util":
                try:
                    utype = self.getMessageField(header,1)
                    if utype == "capabilities":
                        capabilities = edasOpManager.getCapabilitiesStr()
                        self.sendInfoMessage( capabilities )
                    elif utype == "quit":
                        self.logger.info(  " Quitting main thread. " )
                        active = False
                    elif utype == "exception":
                        raise Exception("Test Exception")
                except Exception as err:
                    self.sendError( err )


    def sendVariableData( self, resultVar ):
        header = "|".join( [ "array-"+str(os.getpid()), resultVar.id,  mParse.ia2s(resultVar.origin), mParse.ia2s(resultVar.shape), mParse.m2s(resultVar.metadata) ] )
        self.logger.info( "Sending Result, header: {0}".format( header ) )
        self.result_socket.send( header )
        result_data = resultVar.toBytes( IO_DType )
        self.result_socket.send(result_data)

    def sendError( self, err ):
        msg = "Worker [{0}:{1}] Error {2}: {3}".format( platform.node(), socket.gethostname(), err, traceback.format_exc() )
        self.logger.error( msg  )
        header = "|".join( [ "error-"+str(os.getpid()), msg ] )
        self.result_socket.send( header )

    def sendInfoMessage( self, info ):
        header = "|".join( [ "info-"+platform.node()+"-"+str(os.getpid()), info ] )
        self.result_socket.send( header )

    def processTask(self, task ):
        opModule = edasOpManager.getModule( task )
        self.logger.info( " Processing task: {0}, op module: {1}".format( task, opModule.getName() ) )
        return opModule.executeTask( task, self.cached_inputs )

if __name__ == "__main__":
    request_port = mParse.getIntArg( 1, 8200 )
    result_port = mParse.getIntArg( 2, 8201 )
    worker = Worker( request_port, result_port )
    worker.run()
    worker.logger.info(  " ############################## EXITING WORKER ##############################"  )
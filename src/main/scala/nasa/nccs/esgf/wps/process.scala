package nasa.nccs.esgf.wps
import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.edas.loaders.EDAS_XML
import nasa.nccs.edas.portal.EDASPortal.ConnectionMode
import nasa.nccs.edas.portal.EDASPortalClient
import nasa.nccs.utilities.Loggable
import nasa.nccs.wps.ResponseSyntax

import scala.collection.JavaConversions._
import scala.collection.JavaConversions._

class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

trait GenericProcessManager {
  def describeProcess(service: String, name: String, runArgs: Map[String,String]): xml.Node;
  def getCapabilities(service: String, identifier: String, runArgs: Map[String,String]): xml.Node;
  def executeProcess(service: String, process_name: String, dataInputsSpec: String, parsedDataInputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Node
  def getResultFilePath( service: String, resultId: String ): Option[String]
  def getResult( service: String, resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node
  def getResultStatus( service: String, resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node
}



class ProcessManager( serverConfiguration: Map[String,String] ) extends GenericProcessManager with Loggable {
  def apiManager = new APIManager( serverConfiguration )

  def unacceptable(msg: String): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def shutdown(service: String) = {
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.shutdown()
  }

  def describeProcess(service: String, name: String, runArgs: Map[String,String]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
    //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.describeWPSProcess( name, runArgs )
  }

  def getCapabilities(service: String, identifier: String, runArgs: Map[String,String]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.getWPSCapabilities( identifier, runArgs )
  }

  def executeProcess(service: String, process_name: String, dataInputsSpec: String, dataInputsObj: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
    logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.executeProcess(process_name, dataInputsSpec, dataInputsObj, runargs)
  }

  def getResultFilePath( service: String, resultId: String ): Option[String] = {
    logger.info( "EDAS ProcessManager-> getResultFile: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultFilePath(resultId)
  }

  def getResult( service: String, resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResult( resultId, response_syntax )
  }

  def getResultVariable( service: String, resultId: String ): Option[RDDTransientVariable] = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariable(resultId)
  }

  def getResultStatus( service: String, resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultStatus(resultId,response_syntax)
  }
}

class zmqProcessManager( serverConfiguration: Map[String,String] )  extends GenericProcessManager with Loggable {
  val server = serverConfiguration.getOrElse("edas.server.address","localhost")
  val request_port = serverConfiguration.getOrElse("edas.server.port.request","5670").toInt
  val response_port = serverConfiguration.getOrElse("edas.server.port.response","5671").toInt
  logger.info( s"Starting EDASPortalClient with server='${server}', request_port=${request_port}, response_port=${response_port}" )
  val portal = new EDASPortalClient( "localhost", request_port, response_port )
  val response_manager = portal.createResponseManager()
  def quote( input: String ): String = "\"" + input + "\""
  def map2Str( inputs: Map[String, String] ): String = inputs.map { case ( key, value ) => quote(key) + ": " + quote(value) }.mkString("{ ",", "," }")

  def unacceptable(msg: String) = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def describeProcess(service: String, name: String, runArgs: Map[String,String]): xml.Node  =  {
    val rId = portal.sendMessage( "describeProcess", List( name ).toArray )
    val responses = response_manager.getResponses(rId,true).toList
    EDAS_XML.loadString( responses(0) )
  }

  def getCapabilities(service: String, identifier: String, runArgs: Map[String,String]): xml.Node = {
    val rId = portal.sendMessage( "getCapabilities", List( "" ).toArray )
    val responses = response_manager.getResponses(rId,true).toList
    EDAS_XML.loadString( responses(0) )
  }

  def executeProcess(service: String, process_name: String, dataInputsSpec: String, parsedDataInputs: Map[String, Seq[Map[String, Any]]], runargs: Map[String, String]): xml.Node = {
    val rId = portal.sendMessage( "execute", List( process_name, dataInputsSpec, map2Str(runargs) ).toArray )
    val responses: List[String] = response_manager.getResponses(rId,true).toList
    EDAS_XML.loadString( responses(0) )
  }

  def getResultFilePath( service: String, resultId: String ): Option[String] = {
    throw new Exception("getResultFilePath: Not yet supported!")
  }

  def getResult( service: String, resultId: String, responseSyntax: ResponseSyntax.Value ): xml.Node = {
    val responses = response_manager.getResponses(resultId,true).toList
    EDAS_XML.loadString( responses(0) )
  }

  def getResultStatus( service: String, resultId: String, responseSyntax: ResponseSyntax.Value ): xml.Node = {
    throw new Exception("getResultStatus: Not yet supported!")
  }
}



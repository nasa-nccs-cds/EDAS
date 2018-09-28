package nasa.nccs.esgf.wps
import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.edas.engine.ExecutionCallback
import nasa.nccs.edas.portal.EDASPortalClient
import nasa.nccs.edas.sources.{CollectionLoadServices, EDAS_XML}
import nasa.nccs.esgf.process.{TaskRequest, WorkflowExecutor}
import nasa.nccs.esgf.wps.edasServiceProvider.getResponseSyntax
import nasa.nccs.utilities.Loggable
import scala.xml
import scala.collection.JavaConversions._
import nasa.nccs.wps
import nasa.nccs.wps.{ResponseSyntax, WPSExceptionReport, WPSExecuteStatusError, WPSExecuteStatusStarted }

import scala.xml.XML

class NotAcceptableException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

case class Job( requestId: String, identifier: String, datainputs: String, private val _runargs: Map[String,String], _priority: Float ) extends Comparable[Job] {
  def this( requestId: String, identifier: String, __priority: Float ) { this(requestId, identifier, "", Map( "jobId"->requestId ), __priority ); }
  def sign(f: Float): Int = if( f > 0f ) { 1 } else if( f < 0f ) { -1 } else { 0 }
  def priority: Float = { _priority }
  def compareTo( job: Job ): Int = sign( priority - job.priority )
  def runargs: Map[String,String] = _runargs + ( "jobId"->requestId  )
  private val _startNTime = System.nanoTime()
  def elapsed = ((System.nanoTime()-_startNTime)/1.0e9).toInt
}

trait GenericProcessManager {
  def describeProcess(service: String, name: String, runArgs: Map[String,String]): xml.Node;
  def getCapabilities(service: String, identifier: String, runArgs: Map[String,String]): xml.Node;
  def executeProcess( service: String, job: Job, executionCallback: Option[ExecutionCallback] = None): ( String, xml.Elem )
//  def getResultFilePath( service: String, resultId: String, executor: WorkflowExecutor ): Option[String]
  def getResult( service: String, resultId: String, response_syntax: wps.ResponseSyntax.Value ): xml.Node
  def getResultStatus( service: String, resultId: String, response_syntax: wps.ResponseSyntax.Value ): xml.Node
  def hasResult( service: String, resultId: String ): Boolean
  def serverIsDown: Boolean
  def term();
  def waitUntilJobCompletes( service: String, resultId: String  ) = {
    while( !hasResult(service,resultId) ) { Thread.sleep(500); }
  }
}

class ProcessManager( serverConfiguration: Map[String,String] ) extends GenericProcessManager with Loggable {
  private var _apiManagerOpt: Option[APIManager] = None
  CollectionLoadServices.startService()

  def alloc = if( _apiManagerOpt.isEmpty ) { _apiManagerOpt = Some( new APIManager( serverConfiguration ) ) }
  def apiManager: APIManager = { alloc; _apiManagerOpt.get }
  def serverIsDown: Boolean = { false; }

  def unacceptable(msg: String): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def term = _apiManagerOpt.foreach( _.shutdown )

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

  def executeProcess( service: String, job: Job, executionCallback: Option[ExecutionCallback] = None ): ( String, xml.Elem ) = {
    val dataInputsObj = if( !job.datainputs.isEmpty ) wpsObjectParser.parseDataInputs( job.datainputs ) else Map.empty[String, Seq[Map[String, Any]]]
    val request: TaskRequest = TaskRequest( job.requestId, job.identifier, dataInputsObj )
    val serviceProvider = apiManager.getServiceProvider("edas")
    ( job.requestId, serviceProvider.executeProcess( request, job.datainputs, job.runargs, executionCallback ) )
  }

//  def getResultFilePath( service: String, resultId: String, executor: WorkflowExecutor ): Option[String] = {
//    val serviceProvider = apiManager.getServiceProvider(service)
//    val path = serviceProvider.getResultFilePath( resultId, executor )
//    logger.info( "EDAS ProcessManager-> getResultFile: " + resultId + ", path = " + path.getOrElse("NULL") )
//    path
//  }

  def hasResult( service: String, resultId: String ): Boolean = { false }

  def getResult( service: String, resultId: String, response_syntax: wps.ResponseSyntax.Value ): xml.Node = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResult( resultId, response_syntax )
  }

  def getResultVariable( service: String, resultId: String ): Option[RDDTransientVariable] = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariable(resultId)
  }

  def getResultVariables( service: String ): Iterable[String] = {
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariables
  }

  def getResultStatus( service: String, resultId: String, response_syntax: wps.ResponseSyntax.Value ): xml.Node = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultStatus(resultId,response_syntax)
  }
}

class zmqProcessManager( serverConfiguration: Map[String,String] )  extends GenericProcessManager with Loggable {
  logger.info( "Starting zmqProcessManager with serverConfiguration:\n\t ** " + serverConfiguration.mkString("\n\t ** "))
  val portal = new EDASPortalClient( serverConfiguration )
  val response_manager = portal.createResponseManager()
  def quote( input: String ): String = "\"" + input + "\""
  def map2Str( inputs: Map[String, String] ): String = inputs.map { case ( key, value ) => quote(key) + ": " + quote(value) }.mkString("{ ",", "," }")

  def term() {
    response_manager.term()
    portal.shutdown()
  }
  def serverIsDown: Boolean = { response_manager.serverIsDown }

  def unacceptable(msg: String) = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def hasResult( service: String, resultId: String ): Boolean = { response_manager.hasResult( resultId ); }

  def describeProcess(service: String, name: String, runArgs: Map[String,String]): xml.Node  =  {
    val response = portal.sendMessage( "describeProcess", List( name ).toArray )
    val message = response.split('!').last
    logger.info( "Received 'describeProcess' response, Sample:: " + message.substring(0,Math.min(500,message.length)) )
    EDAS_XML.loadString( message )
  }

  def getCapabilities(service: String, identifier: String, runArgs: Map[String,String]): xml.Node = {
    val response = portal.sendMessage( "getCapabilities", List( identifier ).toArray )
    val message = response.split('!').last
    logger.info( s" EDASW: Received getCapabilities(${identifier}) response: \n" + response.substring( 0, Math.min(500,response.length) ) )
    EDAS_XML.loadString( message )
  }


//  def executeProcess(request: TaskRequest, process_name: String, dataInputsSpec: String, runargs: Map[String, String], executionCallback: Option[ExecutionCallback] = None): xml.Node = {
//    logger.info( "zmqProcessManager executeProcess: " + request.id.toString )
//    val rId = portal.sendMessage( "execute", List( process_name, dataInputsSpec, map2Str(runargs) ).toArray )
//    val responses: List[String] = response_manager.getResponses(rId,true).toList
//    logger.info( "Received responses:\n\t--> " + responses.mkString("\n\t--> "))
//    EDAS_XML.loadString( responses(0) )

  def executeProcess( service: String, job: Job, executionCallback: Option[ExecutionCallback] = None): ( String, xml.Elem ) = {
    var message = ""
    var resultId = ""
    try {
      logger.info( "zmqProcessManager executeProcess: " + job.requestId.toString )
      val response = portal.sendMessage( "execute", List( job.requestId, job.datainputs, map2Str(job.runargs) ).toArray )
      resultId = response.split(':').head
      message = response.substring( response.indexOf('!') + 1 )
      logger.info( "Received 'execute' response, resultId: " + resultId + ", message (xml): " + message )
//      val resultNode = EDAS_XML.loadString(message)
      val resultNode = new WPSExecuteStatusStarted( "EDAS",  message, resultId, 0  ).toXml()
      executionCallback.foreach( _.success(resultNode) )
      ( resultId, resultNode )
    } catch {
      case ex: Exception =>
        val err_msg = message + " :" + ex.toString
        executionCallback.foreach( _.failure(err_msg) )
        logger.error( "zmqProcessManager executeProcess error: " + ex.toString )
        logger.error( "\n\t" + ex.getStackTrace.map(_.toString).mkString("\n\t") )
        val response = new WPSExecuteStatusError( "EDAS", err_msg, job.requestId )
        ( resultId, response.toXml( ResponseSyntax.WPS ) )
    }
  }

  def getResultFilePath( service: String, resultId: String, executor: WorkflowExecutor ): Option[String] = {
    Some( response_manager.getPublishFile( "publish", resultId + ".nc" ).toString )
  }

  def getResult( service: String, resultId: String, responseSyntax: wps.ResponseSyntax.Value ): xml.Node = {
    val responses = response_manager.getResponses(resultId,true).toList
    EDAS_XML.loadString( responses(0) )
  }

  def getResultStatus( service: String, resultId: String, responseSyntax: wps.ResponseSyntax.Value ): xml.Node = {
    throw new Exception("getResultStatus: Not yet supported!")
  }
}



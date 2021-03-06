package nasa.nccs.esgf.wps

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.ExecutionException
import scala.xml
import nasa.nccs.caching.{JobRecord, RDDTransientVariable, collectionDataCache}
import nasa.nccs.edas.engine.ExecutionCallback
import nasa.nccs.esgf.process.{TaskRequest, WorkflowExecutor}
import nasa.nccs.wps.{BlockingExecutionResult, ResponseSyntax, WPSExceptionReport, WPSResponse}
import nasa.nccs.utilities.{Loggable, cdsutils}

trait ServiceProvider extends Loggable {

  def executeProcess(request: TaskRequest, dataInputsSpec: String, runargs: Map[String, String], executionCallback: Option[ExecutionCallback] = None): xml.Elem

  //  def listProcesses(): xml.Elem

  def describeWPSProcess( identifier: String, runArgs: Map[String,String] ): xml.Elem

  def getWPSCapabilities( identifier: String, runArgs: Map[String,String] ): xml.Elem

  def getCause( e: Throwable ): Throwable = e match {
    case err: ExecutionException => err.getCause; case x => e
  }

//  def getResultFilePath( resultId: String, executor: WorkflowExecutor ): Option[String]
  def getResultVariable( resultId: String ): Option[RDDTransientVariable]
  def getResultVariables: Iterable[String]
  def getResult( resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node
  def getResultStatus( resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node
  def executeUtility(service: String, identifiers: Array[String], runArgs: Map[String,String]): xml.Node

  def fatal( e: Throwable ): WPSExceptionReport = {
    val err = getCause( e )
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    new WPSExceptionReport(err)
  }

  def shutdown()

}

object edasServiceProvider extends ServiceProvider {
  import nasa.nccs.edas.engine.EDASExecutionManager
  import nasa.nccs.esgf.process.TaskRequest

  val cds2ExecutionManager = try { new EDASExecutionManager() } catch {
    case err: Throwable =>
      logger.error( "  *** ERROR initializing CDS2ExecutionManager: " + err.toString );
      err.printStackTrace();
      shutdown()
      throw err
    }

  def shutdown() = { EDASExecutionManager.shutdown(); }

  def datainputs2Str( datainputs: Map[String, Seq[Map[String, Any]]] ): String = {
    datainputs.map { case ( key:String, value:Seq[Map[String, Any]] ) =>
      key  + ": " + value.map( _.map { case (k1:String, v1:Any) => k1 + "=" + v1.toString  }.mkString(", ") ).mkString("{ ",", "," }")  }.mkString("{ ",", "," }")
  }
  def getResponseSyntax( runargs: Map[String, String] ): ResponseSyntax.Value = runargs.getOrElse("responseform", "generic") match {
    case x: String if x.toLowerCase ==  "generic" => ResponseSyntax.Generic
    case z => ResponseSyntax.WPS
  }

  override def executeProcess(request: TaskRequest, dataInputsSpec: String, _run_args: Map[String, String], executionCallback: Option[ExecutionCallback] = None): xml.Elem = {
    val jobRec = request.getJobRec(_run_args)
    val jobId = collectionDataCache.addJob( jobRec )
    val runargs = Map("jobId" ->  jobId ) ++ _run_args
    val syntax = getResponseSyntax(runargs)
    try {
      logger.info( " @@edasServiceProvider: exec process: " + request.name + ", runArgs = " + runargs.mkString("; ") )

      cdsutils.time(logger, "\n\n-->> Process %s, datainputs: %s \n\n".format(request.name, dataInputsSpec ) ) {
        if (runargs.getOrElse("status", "true").toBoolean) {
          val result = cds2ExecutionManager.asyncExecute( jobId, request, runargs, executionCallback )
          result.toXml(syntax)
        } else {
          val result = cds2ExecutionManager.blockingExecute( jobId, request, runargs, executionCallback )
          result.toXml(syntax)
        }
      }
    } catch {
      case e: Exception =>
        collectionDataCache.removeJob( jobId )
        throw e
    }
  }
  def describeWPSProcess(process_name: String, runArgs: Map[String,String]): xml.Elem = {
    val syntax = getResponseSyntax(runArgs)
    try {
      cds2ExecutionManager.describeWPSProcess( process_name, syntax )

    } catch { case e: Exception => fatal(e).toXml(syntax) }
  }
  def getWPSCapabilities(identifier: String, runArgs: Map[String,String]): xml.Elem = {
    val syntax = getResponseSyntax(runArgs)
    try {
      cds2ExecutionManager.getWPSCapabilities( if(identifier == null) "" else identifier, syntax )

    } catch { case e: Exception => fatal(e).toXml(syntax) }
  }

  def executeUtility(service: String, identifiers: Array[String], runArgs: Map[String,String]): xml.Node= {
    val syntax = getResponseSyntax(runArgs)
    try {
      cds2ExecutionManager.getWPSCapabilities( identifiers.mkString("|"), syntax )

    } catch { case e: Exception => fatal(e).toXml(syntax) }
  }

  def executeUtilityRequest(request: TaskRequest, identifiers: Array[String], _run_args: Map[String,String]): xml.Elem = {
    val syntax = getResponseSyntax( _run_args )
    val jobRec = request.getJobRec(_run_args)
    val jobId = collectionDataCache.addJob( jobRec )
    val runargs = Map("jobId" ->  jobId ) ++ _run_args
    try {
      cds2ExecutionManager.executeUtilityRequest( jobId, identifiers.mkString("!"), request, _run_args  ).toXml( syntax )

    } catch { case e: Exception => fatal(e).toXml(syntax) }
  }

//  override def getResultFilePath( resultId: String, executor: WorkflowExecutor ): Option[String] = cds2ExecutionManager.getResultFilePath( resultId, executor )
  override def getResult( resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node = cds2ExecutionManager.getResult( resultId, response_syntax )
  override def getResultVariable( resultId: String ): Option[RDDTransientVariable] = cds2ExecutionManager.getResultVariable( resultId )
  override def getResultVariables: Iterable[String] = cds2ExecutionManager.getResultVariables
  override def getResultStatus( resultId: String, response_syntax: ResponseSyntax.Value ): xml.Node = cds2ExecutionManager.getResultStatus( resultId, response_syntax )

}
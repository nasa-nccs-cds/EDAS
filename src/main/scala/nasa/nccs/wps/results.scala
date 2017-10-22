package nasa.nccs.wps

import java.text.SimpleDateFormat
import java.util.Calendar
import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process.UID.ndigits
import nasa.nccs.esgf.process.{DataFragmentSpec, TargetGrid}
import nasa.nccs.esgf.wps.CDSecurity
import nasa.nccs.utilities.Loggable
import nasa.nccs.wps.ResponseSyntax.Value
import org.apache.commons.lang.RandomStringUtils

import scala.collection.mutable.ListBuffer
import scala.xml.Node

object WPSProcessExecuteResponse {
  def merge(  serviceInstance: String, responses: List[WPSProcessExecuteResponse] ): WPSProcessExecuteResponse = new MergedWPSExecuteResponse( serviceInstance, responses )
}

object ResponseSyntax extends Enumeration {
  val WPS, Generic, Default = Value
  def fromString( syntax: String ): Value = {
    syntax.toLowerCase match {
      case "wps" => WPS
      case "generic" => Generic
      case "default" => Default
      case x => throw new Exception( "Error, unrecognizable response syntax: " + syntax )
    }
  }
}

trait WPSResponseElement {
  val timeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
  val default_syntax: ResponseSyntax.Value =  appParameters("wps.response.syntax","generic") match {
    case "generic" => ResponseSyntax.Generic
    case _ => ResponseSyntax.WPS
  }
  def currentTime = timeFormat.format( Calendar.getInstance().getTime() )
  def getSyntax(response_syntax: ResponseSyntax.Value) = if (response_syntax == ResponseSyntax.Default) default_syntax else response_syntax

  def toXml( syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem
}

trait WPSResponse extends WPSResponseElement {
  val wpsProxyAddress = appParameters("wps.server.proxy.href", "${wps.server.proxy.href}")
  val dapProxyAddress = appParameters("wps.dap.proxy.href", "${wps.dap.proxy.href}")
  val fileProxyAddress = appParameters("wps.file.proxy.href", "${wps.file.proxy.href}")
}

object WPS_XML {
  def extractErrorMessage(ex: Exception): Exception = try {
    val error_node = scala.xml.XML.loadString(ex.getMessage)
    val exception_text_nodes: Seq[Node] = (error_node \\ "ExceptionText").theSeq
    val error_text = if (exception_text_nodes.isEmpty) { error_node.toString } else { exception_text_nodes.head.text.trim }
    new Exception( error_text.stripPrefix("<![CDATA[").stripSuffix("]]>").trim, ex)
  } catch { case ex1: Exception => ex }

  def extractErrorMessage(msg: String): String = try {
    val error_node = scala.xml.XML.loadString(msg)
    val exception_text_nodes: Seq[Node] = (error_node \\ "ExceptionText").theSeq
    if (exception_text_nodes.isEmpty) { error_node.toString } else { exception_text_nodes.head.text }
  } catch { case ex1: Exception => msg.stripPrefix("<![CDATA[").stripSuffix("]]>").trim }
}

class WPSExecuteStatusStarted( val serviceInstance: String,  val statusMessage: String, val resId: String  ) extends WPSResponse {
  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem =  getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status>
          <wps:ProcessStarted>{statusMessage}</wps:ProcessStarted>
        </wps:Status>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response serviceInstance={serviceInstance} status="ProcessStarted" message={statusMessage}  creation_time={currentTime}/>
  }
}

class WPSExecuteStatusCompleted( val serviceInstance: String,  val statusMessage: String, val resId: String  ) extends WPSResponse {
  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem =  getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status>
          <wps:ProcessFinished>{statusMessage}</wps:ProcessFinished>
        </wps:Status>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
        <response serviceInstance={serviceInstance} status="ProcessFinished" message={statusMessage}  creation_time={currentTime}/>
  }
}

class WPSExecuteStatusQueued( val serviceInstance: String,  val statusMessage: String, val resId: String  ) extends WPSResponse {
  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem =  getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status>
          <wps:ProcessAccepted>{statusMessage}</wps:ProcessAccepted>
        </wps:Status>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
        <response serviceInstance={serviceInstance} status="ProcessAccepted" message={statusMessage}  creation_time={currentTime}/>
  }
}




class WPSExecuteStatusError( val serviceInstance: String,  val errorMessage: String, val resId: String  ) extends WPSResponse with Loggable {
  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem =  getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status> <wps:ProcessFailed> { getExceptionReport(errorMessage) } </wps:ProcessFailed> </wps:Status>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
        <response serviceInstance={serviceInstance} status="ERROR" creation_time={currentTime}> { "<![CDATA[\n " + CDSecurity.sanitize( errorMessage ) + "\n]]>" } </response>
  }

  def getExceptionReport( errorMessage: String ): xml.Node = {
      <ows:ExceptionReport dbgId="1" xmlns:ows="http://www.opengis.net/ows/1.1">
        <ows:Exception>
          <ows:ExceptionText> { "<![CDATA[\n " + CDSecurity.sanitize( WPS_XML.extractErrorMessage(errorMessage) ) + "\n]]>" } </ows:ExceptionText>
        </ows:Exception>
      </ows:ExceptionReport>
  }
}



class WPSExecuteResult( val serviceInstance: String, val tvar: RDDTransientVariable ) extends WPSResponse {

  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem = getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status>
          <wps:ProcessSucceeded>EDAS Process successfully calculated</wps:ProcessSucceeded>
        </wps:Status>
        <wps:ProcessOutputs>
          {tvar.result.elements.map { case (id, result) =>
          <wps:Output>
            <wps:Data id={id}>
              <wps:LiteralData uom={result.metadata.getOrElse("units", "")} shape={result.shape.mkString(",")}>
                {result.toCDFloatArray.mkDataString(" ", " ", " ")}
              </wps:LiteralData>
            </wps:Data>
          </wps:Output>
        }}
        </wps:ProcessOutputs>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response  serviceInstance={serviceInstance}  creation_time={currentTime} status="Success">
        <outputs>
          {tvar.result.elements.map { case (id, result) =>
          <output id={id} uom={result.metadata.getOrElse("units", "")} shape={result.shape.mkString(",")} >
                {result.toCDFloatArray.mkDataString(" ", " ", " ")}
          </output>
        }}
        </outputs>
      </response>
  }
}

abstract class WPSExecuteResponse( val serviceInstance: String ) extends WPSResponse  with Loggable {

  def getData( response_syntax: ResponseSyntax.Value, id: String, array: CDFloatArray, units: String, maxSize: Int = Int.MaxValue ): xml.Elem =  getSyntax(response_syntax) match {
    case ResponseSyntax.WPS =>
      <wps:Data id={id}>
        <wps:LiteralData uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </wps:LiteralData>
      </wps:Data>
    case ResponseSyntax.Generic =>
      <data id={id} uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </data>
  }

  def getDataRef( response_syntax: ResponseSyntax.Value, id: String, resultId: String, optFileRef: Option[String] ): xml.Elem = getSyntax(response_syntax) match {
    case ResponseSyntax.WPS => optFileRef match {
      case Some( fileRef ) => <wps:Data id={id} href={"result://"+resultId} file={fileRef}> </wps:Data>
      case None =>            <wps:Data id={id} href={"result://"+resultId}> </wps:Data>
    }
    case ResponseSyntax.Generic => optFileRef match {
      case Some( fileRef ) => <data id={id} href={"result://"+resultId} file={fileRef}> </data>
      case None =>            <data id={id} href={"result://"+resultId}> </data>
    }
  }
}

abstract class WPSProcessExecuteResponse( serviceInstance: String, val processes: List[WPSProcess] ) extends WPSExecuteResponse(serviceInstance) {
  def this( serviceInstance: String, process: WPSProcess ) = this( serviceInstance, List(process) )
  def getReference: Option[WPSReference]
  def getFileReference: Option[WPSReference]
  def getResultReference: Option[WPSReference]
  def getDapResultReference: Option[WPSReference]

  def toXml( response_syntax: ResponseSyntax.Value): xml.Elem = {
    val syntax = getSyntax(response_syntax)
    syntax match {
      case ResponseSyntax.WPS =>
        <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                             xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" serviceInstance={serviceInstance}  creation_time={currentTime}>
          {processes.map(_.ExecuteHeader(syntax))}<wps:Status>
          <wps:ProcessStarted>EDAS Process executing</wps:ProcessStarted>
        </wps:Status>
          <wps:ProcessOutputs>
            {getOutputs(syntax)}
          </wps:ProcessOutputs>
        </wps:ExecuteResponse>
      case ResponseSyntax.Generic =>
        <response creation_time={currentTime}>  {getOutputs(syntax)} </response>
    }
  }

  def getOutputs( response_syntax: ResponseSyntax.Value ): List[xml.Elem] = {
    val syntax = getSyntax(response_syntax)
    syntax match {
      case ResponseSyntax.Generic =>
        val outlist: List[xml.Elem]  = processes.flatMap(p => p.outputs.map(output => <outputs> {getProcessOutputs(syntax, p.identifier, output.identifier)} </outputs>))
        outlist  ++ getReferenceOutputs(syntax)
      case ResponseSyntax.WPS =>
        val outlist = processes.flatMap(p => p.outputs.map(output => <wps:Output> {output.getHeader(syntax)}{getProcessOutputs(syntax,p.identifier, output.identifier)} </wps:Output>))
        outlist ++ getReferenceOutputs(syntax)
    }
  }

  def getProcessOutputs(response_syntax: ResponseSyntax.Value, process_id: String, output_id: String ): Iterable[xml.Elem] = Iterable.empty[xml.Elem]
  def getReferenceOutputs(response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = Iterable.empty[xml.Elem]
}

case class WPSReference( id: String, href: String ) extends WPSResponseElement {
  def toXml(response_syntax: ResponseSyntax.Value): xml.Elem = getSyntax(response_syntax) match {
    case ResponseSyntax.WPS => <wps:Reference id={id} encoding="UTF-8" mimeType="text/xml" href={href}/>
    case ResponseSyntax.Generic => <reference id={id} href={href}/>
  }
}

abstract class WPSReferenceExecuteResponse( serviceInstance: String, processes: List[WPSProcess], val resultId: String, resultFileOpt: Option[String] = None )  extends WPSProcessExecuteResponse( serviceInstance, processes ) {
  val statusHref: String = wpsProxyAddress + s"/cwt/status?id=$resultId"
  val resultHref: String = wpsProxyAddress + s"/cwt/result?id=$resultId"
  val dapHrefOpt: Option[String] = if (dapProxyAddress.isEmpty) None else Some(dapProxyAddress + s"/publish/$resultId.nc")
  val fileHrefOpt: Option[String] = if (fileProxyAddress.isEmpty) None else Some(fileProxyAddress + s"/publish/$resultId.nc")

  def getOutputTag(response_syntax: ResponseSyntax.Value): String = getSyntax(response_syntax)  match {
    case ResponseSyntax.WPS => "Output"
    case ResponseSyntax.Generic => "output"
  }
  def getReference: Option[WPSReference] = Some( WPSReference("status",statusHref) )
  def getResultReference: Option[WPSReference] =  Some( WPSReference("result",resultHref) )
  def getDapResultReference: Option[WPSReference] = dapHrefOpt.map ( WPSReference("dap",_) )
  def getFileReference: Option[WPSReference] = fileHrefOpt.map ( WPSReference("file",_ ) )
  def getResultId: String = resultId
}

class MergedWPSExecuteResponse( serviceInstance: String, responses: List[WPSProcessExecuteResponse] ) extends WPSProcessExecuteResponse( serviceInstance, responses.flatMap(_.processes) ) with Loggable {
  val process_ids: List[String] = responses.flatMap( response => response.processes.map( process => process.identifier ) )
  def getReference: Option[WPSReference] = responses.head.getReference
  def getFileReference: Option[WPSReference] = responses.head.getFileReference
  def getResultReference: Option[WPSReference] = responses.head.getResultReference
  def getDapResultReference: Option[WPSReference] = responses.head.getDapResultReference
  if( process_ids.distinct.size != process_ids.size ) { logger.warn( "Error, non unique process IDs in process list: " + processes.mkString(", ") ) }
  val responseMap: Map[String,WPSProcessExecuteResponse] = Map( responses.flatMap( response => response.processes.map( process => ( process.identifier -> response ) ) ): _* )
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, response_id: String ): Iterable[xml.Elem] = responseMap.get( process_id ) match {
    case Some( response ) =>
      val syntax = getSyntax(response_syntax)
      response.getProcessOutputs( syntax, process_id, response_id );
    case None => throw new Exception( "Unrecognized process id: " + process_id )
  }
}

class RDDExecutionResult(serviceInstance: String, processes: List[WPSProcess], id: String, val result: RDDRecord, resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, processes, resultId )  with Loggable {
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    val syntax = getSyntax(response_syntax)
    result.elements map { case (id, array) => getData( syntax, id, array.toCDFloatArray, array.metadata.getOrElse("units","") ) }
  }
}

class RefExecutionResult(serviceInstance: String, process: WPSProcess, id: String, resultId: String, resultFileOpt: Option[String] ) extends WPSReferenceExecuteResponse( serviceInstance, List(process), resultId, resultFileOpt )  with Loggable {
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    val syntax = getSyntax(response_syntax)
    Seq( getDataRef( syntax, id, resultId, resultFileOpt ) )
  }
}


//class ExecutionErrorReport( serviceInstance: String, processes: List[WPSProcess], id: String, val err: Throwable ) extends WPSReferenceExecuteResponse( serviceInstance, processes, "" )  with Loggable {
//  print_error
//  override def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem = {
//    val syntax = getSyntax(response_syntax)
//    syntax match {
//      case ResponseSyntax.WPS =>
//        <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
//                             xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
//          {getReport(syntax)} </ows:ExceptionReport>
//      case ResponseSyntax.Generic => <response> <exceptions> {getReport(syntax)} </exceptions> </response>
//    }
//  }
//  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  {
//    val syntax = getSyntax(response_syntax)
//    val error_mesage = CDSecurity.sanitize(err.getMessage + ":\n\t" + err.getStackTrace.map(_.toString).mkString("\n\t"))
//    syntax match {
//      case ResponseSyntax.WPS =>
//        List(<ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>  {"<![CDATA[\n " + error_mesage + "\n]]>"} </ows:ExceptionText> </ows:Exception>)
//      case ResponseSyntax.Generic =>
//        List(<exception name={err.getClass.getName}>  {"<![CDATA[\n " + error_mesage + "\n]]>"} </exception>)
//    }
//  }
//  def print_error = {
//    val err1 = if (err.getCause == null) err else err.getCause
//    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
//    logger.error(  err1.getStackTrace.mkString("\n")  )
//    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
//    logger.error( "\n-------------------------------------------\n\n")
//  }
//}


abstract class WPSEventReport extends WPSResponse {
  def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem =  <response> <EventReports>  { getReport(response_syntax) } </EventReports> </response>
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem]
}

class UtilityExecutionResult( id: String, val report: xml.Elem )  extends WPSEventReport with Loggable {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  List( <UtilityReport utilityId={id}>  { report } </UtilityReport> )
}

class WPSExceptionReport( val err: Throwable, serviceInstance: String = "WPS" ) extends WPSExecuteResponse(serviceInstance) {
  val eId = print_error
  val stack = Thread.currentThread().getStackTrace()
  def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem = {
    val syntax = getSyntax(response_syntax)
    syntax match {
      case ResponseSyntax.WPS =>
        <ows:ExceptionReport dbgId="2" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                             xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
          {getReport(eId,syntax)} </ows:ExceptionReport>
      case ResponseSyntax.Generic => <response> <exceptions> {getReport(eId,syntax)} </exceptions> </response>
    }
  }
  def getReport( eId: String, response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  {
    val syntax = getSyntax(response_syntax)
    val error_mesage = CDSecurity.sanitize( err.getClass.getName + ": " + err.getMessage )
    syntax match {
      case ResponseSyntax.WPS =>
        List(<ows:Exception exceptionCode={eId}> <ows:ExceptionText>  {"<![CDATA[\n " + error_mesage + ", Stack:\n\t" + stack.mkString("\n\t") + "\n]]>"} </ows:ExceptionText> </ows:Exception>)
      case ResponseSyntax.Generic =>
        List(<exception name={eId}> {"<![CDATA[\n " + error_mesage + ", Stack:\n\t" + stack.mkString("\n\t") + "\n]]>"} </exception>)
    }
  }
  def print_error: String = {
    val eId = RandomStringUtils.random( 6, true, true )
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\nERROR " + eId + ": " + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
    eId
  }
}


class AsyncExecutionResult( serviceInstance: String, processes: List[WPSProcess], resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, processes, resultId ) {
  override def getReferenceOutputs( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = {
    val refs = List( getReference, getFileReference, getDapResultReference ).flatten
    getSyntax(response_syntax) match {
      case ResponseSyntax.WPS =>     List( <wps:Output> { refs.map(_.toXml(ResponseSyntax.WPS)) } </wps:Output> )
      case ResponseSyntax.Generic => List( <output>  { refs.map(_.toXml(ResponseSyntax.Generic)) }  </output> )
    }
  }
}

class WPSMergedEventReport( val reports: List[WPSEventReport] ) extends WPSEventReport {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = reports.flatMap( _.getReport(response_syntax) )
}

class WPSMergedExceptionReport( val exceptions: List[WPSExceptionReport] ) extends WPSEventReport {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = exceptions.flatMap( ex => { val eid = ex.print_error; ex.getReport(eid,response_syntax) } )
}

class BlockingExecutionResult( serviceInstance: String, processes: List[WPSProcess], id: String, val intputSpecs: List[DataFragmentSpec], val gridSpec: TargetGrid, val result_tensor: CDFloatArray,
                               resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, processes, resultId )  with Loggable {
  //  def toXml_old = {
  //    val idToks = id.split('-')
  //    logger.info( "BlockingExecutionResult-> result_tensor(" + id + "): \n" + result_tensor.toString )
  //    val inputs = intputSpecs.map( _.toXml )
  //    val grid = gridSpec.toXml
  //    val results = result_tensor.mkDataString(",")
  //    <result id={id} op={idToks.head} rid={resultId.getOrElse("")}> { inputs } { grid } <data undefined={result_tensor.getInvalid.toString}> {results}  </data>  </result>
  //  }
  override def getProcessOutputs( syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = List( getData( syntax, output_id, result_tensor, intputSpecs.head.units, 250 ) )
}


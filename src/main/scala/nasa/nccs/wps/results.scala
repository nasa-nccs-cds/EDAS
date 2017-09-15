package nasa.nccs.wps

import java.text.SimpleDateFormat
import java.util.Calendar

import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process.{DataFragmentSpec, TargetGrid}
import nasa.nccs.esgf.wps.CDSecurity
import nasa.nccs.utilities.Loggable
import nasa.nccs.wps.ResponseSyntax.Value

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



trait WPSResponse extends {
  val proxyAddress =  appParameters("wps.server.proxy.href","")
  val timeFormat = new SimpleDateFormat("HH-mm-ss MM-dd-yyyy")
  val default_syntax: ResponseSyntax.Value =  appParameters("wps.response.syntax","generic") match {
    case "generic" => ResponseSyntax.Generic
    case _ => ResponseSyntax.WPS
  }
  def currentTime = timeFormat.format( Calendar.getInstance().getTime() )

  def toXml( syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem
}

class WPSExecuteStatus( val serviceInstance: String,  val statusMessage: String, val resId: String  ) extends WPSResponse {
  val resultHref: String = proxyAddress + s"/file?id=$resId"

  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem =  { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax } match {
    case ResponseSyntax.WPS =>
      <wps:ExecuteResponse xmlns:wps="http://www.opengis.net/wps/1.0.0" xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 ../wpsExecute_response.xsd" service="WPS" version="1.0.0" xml:lang="en-CA" creation_time={currentTime}>
        <wps:Status>
          <wps:ProcessStarted>{statusMessage}</wps:ProcessStarted>
        </wps:Status>
        <wps:Reference encoding="UTF-8" mimeType="application/x-netcdf" href={resultHref}/>
      </wps:ExecuteResponse>
    case ResponseSyntax.Generic =>
      <response serviceInstance={serviceInstance} status={statusMessage}  creation_time={currentTime} href={resultHref}/>
  }
}


class WPSExecuteResult( val serviceInstance: String, val tvar: RDDTransientVariable ) extends WPSResponse {

  def toXml( response_syntax: ResponseSyntax.Value = ResponseSyntax.Default ): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax } match {
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

abstract class WPSExecuteResponse( val serviceInstance: String ) extends WPSResponse {

  def getData( response_syntax: ResponseSyntax.Value, id: String, array: CDFloatArray, units: String, maxSize: Int = Int.MaxValue ): xml.Elem =  { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax } match {
    case ResponseSyntax.WPS =>
      <wps:Data id={id}>
        <wps:LiteralData uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </wps:LiteralData>
      </wps:Data>
    case ResponseSyntax.Generic =>
      <data id={id} uom={units} shape={array.getShape.mkString(",")}>  {array.mkBoundedDataString(",", maxSize)}  </data>
  }

  def getDataRef( response_syntax: ResponseSyntax.Value, id: String, resultId: String, optFileRef: Option[String] ): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax } match {
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
  def getReference(response_syntax: ResponseSyntax.Value): xml.Elem
  def getFileReference(response_syntax: ResponseSyntax.Value): xml.Elem
  def getResultReference(response_syntax: ResponseSyntax.Value): xml.Elem

  def toXml( response_syntax: ResponseSyntax.Value): xml.Elem = {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
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
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
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

abstract class WPSReferenceExecuteResponse( serviceInstance: String, processes: List[WPSProcess], val resultId: String )  extends WPSProcessExecuteResponse( serviceInstance, processes )  {
  val statusHref: String = proxyAddress + s"/status?id=$resultId"
  val fileHref: String = proxyAddress + s"/file?id=$resultId"
  val resultHref: String = proxyAddress + s"/result?id=$resultId"
  def getReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax } match {
    case ResponseSyntax.WPS => <wps:Reference id="status" encoding="UTF-8" mimeType="text/xml" href={statusHref}/>
    case ResponseSyntax.Generic => <reference id="status" href={statusHref}/>
  }
  def getFileReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS => <wps:Reference id="file" encoding="UTF-8" mimeType="text/xml" href={fileHref}/>
    case ResponseSyntax.Generic =>   <reference id="file" href={fileHref}/>
  }
  def getResultReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS =>  <wps:Reference id="result" encoding="UTF-8" mimeType="text/xml" href={resultHref}/>
    case ResponseSyntax.Generic => <reference id="result" href={resultHref}/>
  }
  def getResultId: String = resultId
}

abstract class WPSDirectExecuteResponse( serviceInstance: String, val process: WPSProcess, val resultId: String, resultFileOpt: Option[String] )  extends WPSProcessExecuteResponse( serviceInstance, process )  {
  val statusHref: String = ""
  val fileHref: String = "file://" + resultFileOpt.getOrElse("")
  val resultHref: String = s"result://$resultId"
  def getOutputTag(response_syntax: ResponseSyntax.Value): String = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS => "Output"
    case ResponseSyntax.Generic => "output"
  }
  def getReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS => <wps:Reference id="status" href={statusHref}/>
    case ResponseSyntax.Generic => <reference id="status" href={statusHref}/>
  }
  def getFileReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS => <wps:Reference id="file" href={fileHref}/>
    case ResponseSyntax.Generic =>   <reference id="file" href={fileHref}/>
  }
  def getResultReference(response_syntax: ResponseSyntax.Value): xml.Elem = { if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax }  match {
    case ResponseSyntax.WPS =>  <wps:Reference id="result" href={resultHref}/>
    case ResponseSyntax.Generic => <reference id="result" href={resultHref}/>
  }
  def getResultId: String = resultId
}

class MergedWPSExecuteResponse( serviceInstance: String, responses: List[WPSProcessExecuteResponse] ) extends WPSProcessExecuteResponse( serviceInstance, responses.flatMap(_.processes) ) with Loggable {
  val process_ids: List[String] = responses.flatMap( response => response.processes.map( process => process.identifier ) )
  def getReference(response_syntax: ResponseSyntax.Value): xml.Elem = responses.head.getReference(response_syntax)
  def getFileReference(response_syntax: ResponseSyntax.Value): xml.Elem = responses.head.getFileReference(response_syntax)
  def getResultReference(response_syntax: ResponseSyntax.Value): xml.Elem = responses.head.getResultReference(response_syntax)
  if( process_ids.distinct.size != process_ids.size ) { logger.warn( "Error, non unique process IDs in process list: " + processes.mkString(", ") ) }
  val responseMap: Map[String,WPSProcessExecuteResponse] = Map( responses.flatMap( response => response.processes.map( process => ( process.identifier -> response ) ) ): _* )
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, response_id: String ): Iterable[xml.Elem] = responseMap.get( process_id ) match {
    case Some( response ) =>
      val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
      response.getProcessOutputs( syntax, process_id, response_id );
    case None => throw new Exception( "Unrecognized process id: " + process_id )
  }
}

class RDDExecutionResult(serviceInstance: String, processes: List[WPSProcess], id: String, val result: RDDRecord, resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, processes, resultId )  with Loggable {
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    result.elements map { case (id, array) => getData( syntax, id, array.toCDFloatArray, array.metadata.getOrElse("units","") ) }
  }
}

class RefExecutionResult(serviceInstance: String, process: WPSProcess, id: String, resultId: String, resultFileOpt: Option[String] ) extends WPSDirectExecuteResponse( serviceInstance, process, resultId, resultFileOpt )  with Loggable {
  override def getProcessOutputs( response_syntax: ResponseSyntax.Value, process_id: String, output_id: String  ): Iterable[xml.Elem] = {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    Seq( getDataRef( syntax, id, resultId, resultFileOpt ) )
  }
}


class ExecutionErrorReport( serviceInstance: String, processes: List[WPSProcess], id: String, val err: Throwable ) extends WPSReferenceExecuteResponse( serviceInstance, processes, "" )  with Loggable {
  print_error
  override def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem = {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    syntax match {
      case ResponseSyntax.WPS =>
        <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                             xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
          {getReport(syntax)} </ows:ExceptionReport>
      case ResponseSyntax.Generic => <response> <exceptions> {getReport(syntax)} </exceptions> </response>
    }
  }
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    val error_mesage = CDSecurity.sanitize(err.getMessage + ":\n\t" + err.getStackTrace.map(_.toString).mkString("\n\t"))
    syntax match {
      case ResponseSyntax.WPS =>
        List(<ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>  {"<![CDATA[\n " + error_mesage + "\n]]>"} </ows:ExceptionText> </ows:Exception>)
      case ResponseSyntax.Generic =>
        List(<exception name={err.getClass.getName}>  {"<![CDATA[\n " + error_mesage + "\n]]>"} </exception>)
    }
  }
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
}


abstract class WPSEventReport extends WPSResponse {
  def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem =  <response> <EventReports>  { getReport(response_syntax) } </EventReports> </response>
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem]
}

class UtilityExecutionResult( id: String, val report: xml.Elem )  extends WPSEventReport with Loggable {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  List( <UtilityReport utilityId={id}>  { report } </UtilityReport> )
}

class WPSExceptionReport( val err: Throwable, serviceInstance: String = "WPS" ) extends WPSExecuteResponse(serviceInstance) with Loggable {
  print_error
  def toXml( response_syntax: ResponseSyntax.Value ): xml.Elem = {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    syntax match {
      case ResponseSyntax.WPS =>
        <ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows/1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                             xsi:schemaLocation="http://www.opengis.net/ows/1.1 ../../../ows/1.1.0/owsExceptionReport.xsd" version="1.0.0" xml:lang="en-CA">
          {getReport(syntax)} </ows:ExceptionReport>
      case ResponseSyntax.Generic => <response> <exceptions> {getReport(syntax)} </exceptions> </response>
    }
  }
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] =  {
    val syntax = if(response_syntax == ResponseSyntax.Default) default_syntax  else response_syntax
    val error_mesage = CDSecurity.sanitize(err.getMessage + ":\n\t" + err.getStackTrace.map(_.toString).mkString("\n\t"))
    syntax match {
      case ResponseSyntax.WPS =>
        List(<ows:Exception exceptionCode={err.getClass.getName}> <ows:ExceptionText>  {"<![CDATA[\n " + error_mesage + "\n]]>"} </ows:ExceptionText> </ows:Exception>)
      case ResponseSyntax.Generic =>
        List(<exception name={err.getClass.getName}> {"<![CDATA[\n " + error_mesage + "\n]]>"} </exception>)
    }
  }
  def print_error = {
    val err1 = if (err.getCause == null) err else err.getCause
    logger.error("\n\n-------------------------------------------\n" + err1.toString + "\n")
    logger.error(  err1.getStackTrace.mkString("\n")  )
    if (err.getCause != null) { logger.error( "\nTriggered at: \n" + err.getStackTrace.mkString("\n") ) }
    logger.error( "\n-------------------------------------------\n\n")
  }
}


class AsyncExecutionResult( serviceInstance: String, processes: List[WPSProcess], resultId: String ) extends WPSReferenceExecuteResponse( serviceInstance, processes, resultId ) {
  override def getReferenceOutputs( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = {
    val syntax = if (response_syntax == ResponseSyntax.Default) default_syntax else response_syntax
    syntax match {
      case ResponseSyntax.WPS =>     List( <wps:Output>  {getReference(syntax)}  {getFileReference(syntax)} </wps:Output> )
      case ResponseSyntax.Generic => List( <output> {getReference(syntax)}  {getFileReference(syntax)}  </output> )
    }
  }
}

class WPSMergedEventReport( val reports: List[WPSEventReport] ) extends WPSEventReport {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = reports.flatMap( _.getReport(response_syntax) )
}

class WPSMergedExceptionReport( val exceptions: List[WPSExceptionReport] ) extends WPSEventReport {
  def getReport( response_syntax: ResponseSyntax.Value ): Iterable[xml.Elem] = exceptions.flatMap( _.getReport(response_syntax) )
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


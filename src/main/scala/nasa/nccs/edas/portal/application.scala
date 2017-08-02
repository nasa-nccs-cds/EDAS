package nasa.nccs.edas.portal
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.cdapi.data.{HeapFltArray, RDDRecord}
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.portal.EDASApplication.logger
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import nasa.nccs.edas.portal.EDASPortal.ConnectionMode._
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.utilities.{EDASLogManager, Loggable}
import nasa.nccs.wps.WPSExceptionReport
import org.apache.spark.SparkEnv

import scala.xml
import scala.io.Source

object EDASapp {
  def elem( array: Array[String], index: Int, default: String = "" ): String = if( array.length > index ) array(index) else default

  def getConfiguration( parameter_file_path: String ): Map[String, String] = {
    if( parameter_file_path.isEmpty ) { Map.empty[String, String]  }
    else if( Files.exists( Paths.get(parameter_file_path) ) ) {
      val params: Iterator[Array[String]] = for ( line <- Source.fromFile(parameter_file_path).getLines() ) yield { line.split('=') }
      Map( params.filter( _.length > 1 ).map( a => ( a.head.trim, a.last.trim ) ).toSeq: _* )
    }
    else { throw new Exception( "Can't find parameter file: " + parameter_file_path) }
  }
}

class EDASapp( mode: EDASPortal.ConnectionMode, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends EDASPortal( mode, request_port, response_port ) {
  import EDASapp._
  val processManager = new ProcessManager( appConfiguration )
  val process = "edas"
  val printer = new scala.xml.PrettyPrinter(200, 3)
  Runtime.getRuntime().addShutdownHook( new Thread() { override def run() { term() } } )


  override def postArray(header: String, data: Array[Byte]) = {

  }

  override def execUtility(utilSpec: Array[String]) = {

  }

  def getResult( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResult( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  def getResultStatus( resultSpec: Array[String] ) = {
    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0) )
    sendResponse( resultSpec(0), printer.format( result )  )
  }

  def getRunArgs( taskSpec: Array[String] ): Map[String,String] = {
    val runargs = if( taskSpec.length > 4 ) wpsObjectParser.parseMap( taskSpec(4) ) else Map.empty[String, Any]
    if( !runargs.keys.contains("response") ) {
      val async = runargs.getOrElse("async","false").toString.toBoolean
      val defaultResponseType = if( async ) { "file" } else { "xml" }
      runargs.mapValues(_.toString) + ("response" -> defaultResponseType )
    } else runargs.mapValues(_.toString)
  }

  override def execute( taskSpec: Array[String] ) = {
    val process_name = elem(taskSpec,2)
    val datainputs = if( taskSpec.length > 3 ) wpsObjectParser.parseDataInputs( taskSpec(3) ) else Map.empty[String, Seq[Map[String, Any]]]
    val runargs = getRunArgs( taskSpec )
    val responseType = runargs.getOrElse("response","xml")
    val response = processManager.executeProcess( process, process_name, datainputs, runargs )
    if( responseType == "object" ) { sendDirectResponse( taskSpec(0), response ) }
    else if( responseType == "file" ) { sendFileResponse( taskSpec(0), response ) }
    sendResponse( taskSpec(0), printer.format( response ) )
  }

  def sendErrorReport( id: String, exc: Exception ) = {
    val err = new WPSExceptionReport(exc)
    sendResponse( id, printer.format( err.toXml ) )
  }

  def shutdown = { processManager.shutdown( process ) }

  def sendDirectResponse( responseId: String, response: xml.Elem ): Unit =  {
    val refs: xml.NodeSeq = response \\ "data"
    val resultHref = refs.flatMap( _.attribute("href") ).find( _.nonEmpty ).map( _.text ) match {
      case Some( href ) =>
        val rid = href.split("[/]").last
        logger.info( "\n\n     **** Found result Id: " + rid + " ****** \n\n")
        processManager.getResultVariable("edas",rid) match {
          case Some( resultVar ) =>
            var gridfilename = ""
            resultVar.result.elements.foreach { case (key, data) =>
              if( gridfilename.isEmpty ) {
                val gridfilepath = data.metadata("gridfile")
                gridfilename = sendFile( rid, "gridfile", gridfilepath )
              }
              sendArrayData(rid, data.origin, data.shape, data.toByteArray, data.metadata + ("gridfile" -> gridfilename) + ( "elem" -> key.split('.').last ) )
            }
          case None => logger.error( "Can't find result variable " + rid)
        }

      case None =>
        logger.error( "Can't find result Id in direct response: " + response.toString() )
    }
  }

  def getNodeAttribute( node: xml.Node, attrId: String ): Option[String] = {
    node.attribute( attrId ).flatMap( _.find( _.nonEmpty ).map( _.text ) )
  }

  def sendFileResponse( responseId: String, response: xml.Elem ): Unit =  {
    val refs: xml.NodeSeq = response \\ "data"
    for( node: xml.Node <- refs; hrefOpt = getNodeAttribute( node,"href"); fileOpt = getNodeAttribute( node,"file");
         if hrefOpt.isDefined && fileOpt.isDefined; href = hrefOpt.get; filepath=fileOpt.get ) {
      print( "Processing result node: " + node.toString() )
      val rid = href.split("[/]").last
      logger.info( "\n\n     **** Found result Id: " + rid + ": sending File: " + filepath + " ****** \n\n" )
      sendFile( rid, "variable", filepath )
    }
  }

  override def getCapabilities(utilSpec: Array[String]) = {
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,2) )
    sendResponse( utilSpec(0), printer.format( result ) )
  }

  override def describeProcess(procSpec: Array[String]) = {
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,2) )
    sendResponse( procSpec(0), printer.format( result )  )
  }
}

object EDASApplication extends Loggable {
  def main(args: Array[String]) {
    import EDASapp._
    EDASLogManager.isMaster
    logger.info(s"Executing EDAS with args: ${args.mkString(",")}, nprocs: ${Runtime.getRuntime.availableProcessors()}")
    val connect_mode = elem(args, 0, "bind")
    val request_port = elem(args, 1, "0").toInt
    val response_port = elem(args, 2, "0").toInt
    val parameter_file = elem(args, 3, "")
    val appConfiguration = getConfiguration( parameter_file )
    val cmode = if (connect_mode.toLowerCase.startsWith("c")) CONNECT else BIND
    val app = new EDASapp(cmode, request_port, response_port, appConfiguration)
    sys.addShutdownHook( { app.term() } )
    app.run()
  }

}

object TestApplication extends Loggable {
  def main(args: Array[String]) {
    EDASLogManager.isMaster
    val sc = CDSparkContext()
    val indices = sc.sparkContext.parallelize( Array.range(0,500), 100 )
    val base_time = System.currentTimeMillis()
    val timings = indices.map( getProfileDiagnostic(base_time) )
    val time_list = timings.collect() mkString ("\n")
    println( " @@@ NPart = " + indices.getNumPartitions.toString )
    println( time_list )
  }
  def getProfileDiagnostic( base_time: Float )( index: Int ): String = {
    val result = s"  T{$index} => E${SparkEnv.get.executorId}:${ManagementFactory.getRuntimeMXBean.getName} -> %.4f".format( (System.currentTimeMillis() - base_time)/1.0E3 )
    logger.info( result )
    result
  }
}

// nasa.nccs.edas.portal.TestApplication

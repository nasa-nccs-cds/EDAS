package nasa.nccs.edas.portal
import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, Paths}
import java.sql.{Date, Timestamp}
import scala.xml
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.cdapi.data.{FastMaskedArray, HeapFltArray}
import nasa.nccs.edas.engine.{EDASExecutionManager, ExecutionCallback, TestProcess}
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.rdd.{CDRecord, TestClockProcess, TestDatasetProcess}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.esgf.wps.{Job, ProcessManager, wpsObjectParser}
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.esgf.wps.edasServiceProvider.getResponseSyntax
import nasa.nccs.utilities.{EDASLogManager, Loggable}
import nasa.nccs.wps.{WPSMergedEventReport, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SQLImplicits}
import ucar.ma2.{ArrayFloat, IndexIterator}
import ucar.nc2.Variable

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
// import org.apache.spark.implicits._
import org.apache.spark.SparkEnv
import org.apache.spark.api.java.function.MapPartitionsFunction
import org.apache.spark.sql.Row
import ucar.ma2
import ucar.nc2.dataset.{CoordinateAxis1DTime, NetcdfDataset}
import ucar.nc2.time.CalendarDate

import scala.collection.mutable
import scala.io.Source

//import gov.nasa.gsfc.cisto.cds.sia.scala.climatespark.core.EDASDriver
//
//object TestImportApp extends EDASDriver {
//
//}

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

class EDASapp( client_address: String, request_port: Int, response_port: Int, appConfiguration: Map[String,String] ) extends EDASPortal( client_address, request_port, response_port ) {
  import EDASapp._
  val processManager = new ProcessManager( appConfiguration )
  val process = "edas"
  val randomIds = new RandomString(16)
  val printer = new scala.xml.PrettyPrinter(200, 3)
  Runtime.getRuntime.addShutdownHook( new Thread() { override def run() { term("ShutdownHook Called") } } )

  def start( run_program: String = "" ): Unit = {
    if( run_program.isEmpty ) { run() }
    else { runAltProgram(run_program) }
  }

  def runAltProgram( run_program: String ): Unit = {

  }

  override def execUtility(utilSpec: Array[String]): Message = {
    new Message("","","")
  }

//  def getResult( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
//    val result: xml.Node = processManager.getResult( process, resultSpec(0),response_syntax )
//    sendResponse( resultSpec(0), printer.format( result )  )
//  }
//
//  def getResultStatus( resultSpec: Array[String], response_syntax: ResponseSyntax.Value ) = {
//    val result: xml.Node = processManager.getResultStatus( process, resultSpec(0), response_syntax )
//    sendResponse( resultSpec(0), printer.format( result )  )
//  }

  def getRunArgs( taskSpec: Array[String] ): Map[String,String] = {
    val runargs = if( taskSpec.length > 4 ) wpsObjectParser.parseMap( taskSpec(4) ) else Map.empty[String, Any]
    val responseForm = runargs.getOrElse("responseform","wps").toString
    if( responseForm == "wps" ) {
      val responseType = runargs.getOrElse( "response", defaultResponseType(runargs) ).toString
      runargs.mapValues(_.toString) + ("response" -> responseType )
    } else {
      val responseToks = responseForm.split(':')
      val new_runargs = runargs.mapValues(_.toString) + ("response" -> responseToks.head )
      if( responseToks.head.equalsIgnoreCase("collection") && responseToks.length > 1  )  { new_runargs + ("cid" -> responseToks.last ) } else { new_runargs }
    }
  }

  def defaultResponseType( runargs:  Map[String, Any] ): String = {
    val status = runargs.getOrElse("status","false").toString.toBoolean
    if( status ) { "file" } else { "xml" }
  }

  override def execute( taskSpec: Array[String] ): Response = {
    val clientId = elem(taskSpec,0)
    val runargs = getRunArgs( taskSpec )
    val jobId = runargs.getOrElse( "jobId", randomIds.nextString )
    logger.info( " @@E TaskSpec: " + taskSpec.mkString(", ") )
    try {
      val process_name = elem(taskSpec,2)
      val dataInputsSpec = elem(taskSpec,3)
      setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
      logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + runargs.mkString("; "))
      val response_syntax = getResponseSyntax(runargs)
      val responseType = runargs.getOrElse("response","file")
      val executionCallback: ExecutionCallback = new ExecutionCallback {
        override def success( results: xml.Node ): Unit = {
          logger.info(s" *** ExecutionCallback: jobId = $jobId, responseType = $responseType *** ")
          val metadata  =  if (responseType == "object")    { sendDirectResponse(response_syntax, clientId, jobId, results) }
                           else if (responseType == "file") { sendFileResponse(response_syntax, clientId, jobId, results) }
                           else { Map.empty }
          setExeStatus(clientId, jobId, "completed|" + ( metadata.map { case (key,value) => key + ":" + value } mkString "," ) )
        }
        override def failure( msg: String ): Unit = {
          logger.error( s"ERROR CALLBACK ($jobId:$clientId): " + msg )
          setExeStatus( clientId, jobId, "error" )
        }
      }

      val (rid, responseElem) = processManager.executeProcess( process, Job(jobId, process_name, dataInputsSpec, runargs, 1f ), Some(executionCallback) )
      new Message(clientId, jobId, printer.format(responseElem))
    } catch  {
      case e: Throwable =>
        logger.error( "Caught execution error: " + e.getMessage )
        logger.error( "\n" + e.getStackTrace.mkString("\n") )
        setExeStatus( clientId, jobId, "error" )
        new ErrorReport( clientId, jobId, e.getClass.getSimpleName + ": " + e.getMessage )
    }
  }

  def sendErrorReport( response_format: ResponseSyntax.Value, clientId: String, responseId: String, exc: Exception ): Unit = {
    val err = new WPSExceptionReport(exc)
    sendErrorReport( clientId, responseId, printer.format( err.toXml(response_format) ) )
  }

  def sendErrorReport( taskSpec: Array[String], exc: Exception ): Unit = {
    val clientId = taskSpec(0)
    val runargs = getRunArgs( taskSpec )
    val syntax = getResponseSyntax(runargs)
    val err = new WPSExceptionReport(exc)
    sendErrorReport( clientId, "requestError", printer.format( err.toXml(syntax) ) )
  }

  override def shutdown(): Unit = processManager.term

  def sendDirectResponse( response_format: ResponseSyntax.Value, clientId: String, responseId: String, response: xml.Node ): Map[String,String] =  {
    val refs: xml.NodeSeq = response \\ "data"
    val resultHref = refs.flatMap( _.attribute("href") ).find( _.nonEmpty ).map( _.text ) match {
      case Some( href ) =>
        val rid = href.split("[/]").last
        logger.info( "\n\n     **** Found result Id: " + rid + " ****** \n\n")
        processManager.getResultVariable("edas",rid) match {
          case Some( resultVar ) =>
            val slice: CDRecord = resultVar.result.concatSlices.records.head
            slice.elements.foreach { case ( id, array ) =>
              sendArrayData( clientId, rid, array.origin, array.shape, array.toByteArray, resultVar.result.metadata  + ( "elem" -> id ) )  // + ("gridfile" -> gridfilename)
            }

//            var gridfilename = ""
//            resultVar.result.slices.foreach { case (key, data) =>
//              if( gridfilename.isEmpty ) {
//                val gridfilepath = data.metadata("gridfile")
//                gridfilename = sendFile( clientId, rid, "gridfile", gridfilepath, true )
//              }
          case None =>
            logger.error( "Can't find result variable " + rid)
            sendErrorReport( response_format, clientId, rid, new Exception( "Can't find result variable " + rid + " in [ " + processManager.getResultVariables("edas").mkString(", ") + " ]") )
        }
      case None =>
        logger.error( "Can't find result Id in direct response: " + response.toString() )
        sendErrorReport( response_format, clientId, responseId, new Exception( "Can't find result Id in direct response: " + response.toString()  ) )
    }
    Map.empty[String,String]
  }

  def getNodeAttribute( node: xml.Node, attrId: String ): Option[String] = {
    node.attribute( attrId ).flatMap( _.find( _.nonEmpty ).map( _.text ) )
  }

  def getNodeAttributes( node: xml.Node ): String = node.attributes.toString()

  def sendFileResponse( response_format: ResponseSyntax.Value, clientId: String, jobId: String, response: xml.Node  ): Map[String,String] =  {
    val refs: xml.NodeSeq = response \\ "data"
    var result_files = new ArrayBuffer[String]()
    for( node: xml.Node <- refs; hrefOpt = getNodeAttribute( node,"href"); fileOpt = getNodeAttribute( node,"files") ) {
      if (hrefOpt.isDefined && fileOpt.isDefined) {
        val sharedDataDir = appParameters( "wps.shared.data.dir" )
//        val href = hrefOpt.get
//        val rid = href.split("[/]").last
        fileOpt.get.split(",").foreach( filepath => {
          logger.info("     ****>> Found file node for jobId: " + jobId + ", clientId: " + clientId + ", sending File: " + filepath + " ****** ")
          result_files += sendFile( clientId, jobId, "publish", filepath, sharedDataDir.isEmpty )
        })
      } else if (hrefOpt.isDefined && hrefOpt.get.startsWith("collection")) {
        responder.sendResponse( new Message( clientId, jobId, "Created " + hrefOpt.get ) )
      } else {
        sendErrorReport( response_format, clientId, jobId, new Exception( "Can't find href or files in attributes: " + getNodeAttributes( node ) ) )
      }
    }
    Map( "files" -> result_files.mkString(",") )
  }

  override def getCapabilities(utilSpec: Array[String]): Message = {
    val runargs: Map[String,String] = getRunArgs( utilSpec )
    logger.info(s"Processing getCapabilities request with args ${runargs.toList.mkString(",")}" )
    val result: xml.Elem = processManager.getCapabilities( process, elem(utilSpec,2), runargs )
    new Message( utilSpec(0), "capabilities", printer.format( result ) )
  }

  override def describeProcess(procSpec: Array[String]): Message = {
    val runargs: Map[String,String] = getRunArgs( procSpec )
    logger.info(s"Processing describeProcess request with args ${runargs.toList.mkString(",")}" )
    val result: xml.Elem = processManager.describeProcess( process, elem(procSpec,2), runargs )
    new Message( procSpec(0), "preocesses", printer.format( result ) )
  }
}

object EDASApplication extends Loggable {
  def main(args: Array[String]) {
    import EDASapp._
    EDASLogManager.isMaster()
    logger.info(s"Executing EDAS with args: ${args.mkString(",")}, nprocs: ${Runtime.getRuntime.availableProcessors()}")
    val request_port = elem(args, 0, "0").toInt
    val response_port = elem(args, 1, "0").toInt
    val parameter_file = elem(args, 2 )
    val run_program = elem(args, 3 )
    val appConfiguration = getConfiguration( parameter_file )
    val client_address: String = appConfiguration.getOrElse("client","*")
//    EDASExecutionManager.addTestProcess( new TestDatasetProcess( "testDataset") )
//    EDASExecutionManager.addTestProcess( new TestClockProcess( "testClock") )
    val app = new EDASapp( client_address, request_port, response_port, appConfiguration )
    app.start( run_program )
    logger.info(s"EXIT EDASApplication")
    sys.exit()
  }

}

object SparkCleanup extends Loggable {
  def main(args: Array[String]): Unit = {
    EDASExecutionManager.shutdown_python_workers()
    EDASExecutionManager.cleanup_spark_workers()
  }
}

object TestApplication extends Loggable {
  def main(args: Array[String]) {
    EDASLogManager.isMaster()
    val sc = CDSparkContext()
    val indices = sc.sparkContext.parallelize( Array.range(0,500), 100 )
    val base_time = System.currentTimeMillis()
    val timings = indices.map( getProfileDiagnostic(base_time) )
    val time_list = timings.collect() mkString "\n"
    println( " @@@ NPart = " + indices.getNumPartitions.toString )
    println( time_list )
  }
  def getProfileDiagnostic( base_time: Float )( index: Int ): String = {
    val result = s"  T{$index} => E${SparkEnv.get.executorId}:${ManagementFactory.getRuntimeMXBean.getName} -> %.4f".format( (System.currentTimeMillis() - base_time)/1.0E3 )
    logger.info( result )
    result
  }
}


object TestReadApplication extends Loggable {
  def main(args: Array[String]): Unit = {
    import ucar.ma2.ArrayFloat
    val data_path= "/dass/pubrepo/CREATE-IP/data/reanalysis"
    val dset_address = data_path + "/NASA-GMAO/GEOS-5/MERRA2/6hr/atmos/ta/ta_6hr_reanalysis_MERRA2_1980010100-1980013118.nc"
    val indices = List( 1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41 )
    val vname = "ta"
    val test = 3
    val t0 = System.nanoTime()
    val input_dataset = NetcdfDataset.openDataset(dset_address)
    val input_variable = input_dataset.findVariable(vname)
    val raw_data = input_variable.read()
    val slices = indices.map( raw_data.slice(1,_) )
    val attrs = input_variable.getAttributes.map( _.getStringValue ).mkString(", ")
    val in_shape = input_variable.getShape
    var out_shape = input_variable.getShape
    out_shape(1) = indices.length
    val out_array: ArrayFloat = ucar.ma2.Array.factory( ucar.ma2.DataType.FLOAT, out_shape ).asInstanceOf[ArrayFloat]
    val out_buffer: Any = out_array.getStorage
    val out_index = out_array.getIndex
    val out_size = out_array.getSize.toInt
    val nTS = out_shape(0)
    val nZ = out_shape(1)
    val out_tstride = out_size / nTS
    val out_zstride = out_tstride / nZ
    val slice_shape = slices.head.getShape
    val slice_size = slices.head.getSize.toInt
    val copy_size =  slice_size / nTS
    val t1 = System.nanoTime()
    logger.info( s"Running test, in_shape : [ ${in_shape.mkString(",")} ], out_shape : [ ${out_shape.mkString(",")} ], slice_shape : [ ${slice_shape.mkString(",")} ], slice_size : [ $slice_size ] , nTS : [ $nTS ]  " )
    if( test == 1 ) {
      for (si <- slices.indices; slice = slices(si); slice_index = slice.getIndex ) {
        for( iTS <- 0 until nTS ) {
          val slice_element = copy_size*iTS
          val out_element = si*slice_size + slice_element
          val slice_buffer: Any = slice.get1DJavaArray( slice.getElementType )
          System.arraycopy( slice_buffer, slice_element, out_buffer, out_element,  copy_size.toInt )
        }
      }
    } else if( test == 2 )  {
      for (slice_index <- slices.indices; slice = slices(slice_index); slice_iter = slice.getIndexIterator) {
        logger.info(s"Merging slice $slice_index, shape = [ ${slice.getShape.mkString(", ")} ]")
        while (slice_iter.hasNext) {
          val f0 = slice_iter.getFloatNext
          val counter = slice_iter.getCurrentCounter
          out_index.set(counter(0), slice_index, counter(1), counter(2))
          out_array.setFloat(out_index.currentElement, f0)
        }
      }
    } else if( test == 3 )  {
      for ( iZ <- slices.indices; slice = slices(iZ) ) {
        for( iT <- 0 until nTS ) {
          for( iXY <- 0 until copy_size ) {
            val i0 = iT * copy_size + iXY
            val f0 = slice.getFloat(i0)
            val i1 = iT * out_tstride + iZ * out_zstride + iXY
            out_array.setFloat( i1, f0 )
          }
        }
      }
    }
    val t2 = System.nanoTime()
    logger.info( s"Completed test, time = %.4f sec, array join time = %.4f sec".format( (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9 ) )
  }
}

//    val levs = List(100000, 97500, 95000, 92500, 90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000, 55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)


// nasa.nccs.edas.portal.TestApplication
// nasa.nccs.edas.portal.TestReadApplication

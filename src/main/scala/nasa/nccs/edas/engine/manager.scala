package nasa.nccs.edas.engine
import java.io.{IOException, PrintWriter, StringWriter}
import java.nio.file.{Files, Paths}
import java.io.File

import scala.xml
import scala.collection.concurrent.TrieMap
import nasa.nccs.cdapi.cdm._
import nasa.nccs.esgf.process._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import nasa.nccs.utilities.{EDTime, EventAccumulator, Loggable, cdsutils}
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelMgr, KernelModule}
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.{Await, Future, Promise}
import nasa.nccs.cdapi.tensors.{CDArray, CDByteArray, CDFloatArray}
import nasa.nccs.caching._
import nasa.nccs.cdapi.data.FastMaskedArray
import nasa.nccs.edas.engine.EDASExecutionManager.logger
import ucar.{ma2, nc2}
import nasa.nccs.edas.utilities.{GeoTools, appParameters, runtime}

import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.portal.CleanupManager
import nasa.nccs.edas.rdd.{ArraySpec, CDRecord, QueryResultCollection}
import nasa.nccs.edas.sources.{Collection, Collections}
import nasa.nccs.wps.{WPSExecuteStatusStarted, WPSResponse, _}
import org.joda.time.DateTime
import ucar.ma2.DataType
import ucar.nc2.constants.AxisType
import ucar.nc2.{Attribute, Dimension}
import ucar.nc2.dataset.{CoordinateAxis, NetcdfDataset}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingDefault, Nc4ChunkingStrategyNone}

import scala.io.Source


class Counter(start: Int = 0) {
  private val index = new AtomicReference(start)
  def get: Int = {
    val i0 = index.get
    if(index.compareAndSet( i0, i0 + 1 )) i0 else get
  }
}

trait ExecutionCallback extends Loggable {
  def success( results: xml.Node  )
  def failure( msg: String )
}

object EDASExecutionManager extends Loggable {
  val handler_type_key = "execution.handler.type"
  private var _killed = false
  private var _testProcesses = TrieMap.empty[ String, TestProcess ]
  def addTestProcess( test_process: TestProcess ) = { _testProcesses +=  (test_process.id -> test_process) }
  def getTestProcess( id: String ): Option[TestProcess] = _testProcesses.get(id)

  def checkIfAlive: Unit = { if(_killed) {  _killed = false; throw new Exception("Job Killed") }; }
  def killJob = _killed = true;

  def shutdown() = {
    print( "Shutting down CDS2ExecutionManager")
    shutdown_python_workers()
  }

//  def shutdown_python_workers() = {
//    import sys.process._
//    print( "Shutting down python workers.")
//    val slaves_file = Paths.get( sys.env("SPARK_HOME"), "conf", "slaves" ).toFile
//    val shutdown_script = Paths.get( sys.env("HOME"), ".edas", "sbin", "shutdown_python_worker.sh" ).toFile
  //        val remote_shutdown_script = "ssh %s bash -c \"'pkill -u $USER java; pkill -u $USER java'\"".format(slave_node)
  //        print( s"\nCleaning up spark worker ${slave_node} with shutdown script: '${remote_shutdown_script}'"  )
//    if( slaves_file.exists && slaves_file.canRead ) {
//      val shutdown_futures = for (slave <- Source.fromFile(slaves_file).getLines(); slave_node = slave.trim; if !slave_node.isEmpty && !slave_node.startsWith("#") ) yield  {
//          Future {  try { "ssh %s %s".format(slave_node,shutdown_script.toString) ! }
//                    catch { case err: Exception => println( "Error shutting down python workers on slave_node '" + slave_node + "' using shutdown script '" + shutdown_script.toString + "': " + err.toString ); }
//          }
//      }
//      Future.sequence( shutdown_futures )
//    } else try {
//      logger.info( "No slaves file found, shutting down python workers locally:")
//      try { shutdown_script.toString ! }
//      catch { case err: Exception => println( "Error shutting down local python workers using shutdown script '" + shutdown_script.toString + "': " + err.toString ); }
//    } catch {
//      case err: Exception => logger.error( "Error shutting down python workers: " + err.toString )
//    }
//  }

  def shutdown_python_workers() = try {
    import sys.process._
    val slaves_file = Paths.get( sys.env("SPARK_HOME"), "conf", "slaves" ).toFile
    println( s"Cleaning up python workers using slaves file ${slaves_file}")
    if( slaves_file.exists && slaves_file.canRead ) {
      for (slave <- Source.fromFile(slaves_file).getLines(); slave_node = slave.trim; if !slave_node.isEmpty && !slave_node.startsWith("#") )  {
        println( s"\nCleaning up python worker ${slave_node} " )
        try { Seq("ssh", slave_node, "bash", "-c", "'pkill -u $USER python'" ).! }
        catch { case err: Exception => println( "Error shutting down spark workers on slave_node '" + slave_node + ": " + err.toString ); }
      }
    } else try {
      println( "No slaves file found, shutting down python workers locally:")
      try { "bash -c \"'pkill -u $USER python'\"" ! }
      catch { case err: Exception => println( "Error cleaning up local python workers: " + err.toString ); }
    } catch {
      case err: Exception => logger.error( "Error cleaning up python workers: " + err.toString )
    }
  } catch {
    case err: Exception => logger.error( "Error in shutdown_python_workers: " + err.toString )
  }

  def cleanup_spark_workers() = try {
    import sys.process._
    val slaves_file = Paths.get( sys.env("SPARK_HOME"), "conf", "slaves" ).toFile
    println( s"Cleaning up spark workers using slaves file ${slaves_file}")
    if( slaves_file.exists && slaves_file.canRead ) {
      for (slave <- Source.fromFile(slaves_file).getLines(); slave_node = slave.trim; if !slave_node.isEmpty && !slave_node.startsWith("#") ) {
        println( s"\nCleaning up spark worker ${slave_node} " )
        try { Seq("ssh", slave_node, "bash", "-c", "'pkill -u $USER java; pkill -u $USER python; rm -rf /tmp/$USER/logs/*'" ).! }
        catch { case err: Exception => println( "Error shutting down spark workers on slave_node '" + slave_node + ": " + err.toString ); }
//        println( s"\nCleaning up tmp files on ${slave_node} " )
//        try { Seq("ssh", slave_node, "bash", "-c", "'rm -rf /tmp/$USER/logs/*'" ).! }
//        catch { case err: Exception => println( "Error cleaning up tmp files on slave_node '" + slave_node + ": " + err.toString ); }
      }
    } else try {
      println( "No slaves file found, shutting down spark workers locally:")
      try { "bash -c \"'pkill -u $USER java; pkill -u $USER java'\"" ! }
      catch { case err: Exception => logger.error( "Error cleaning up local spark workers: " + err.toString ); }
    } catch {
      case err: Exception => logger.error( "Error cleaning up spark workers: " + err.toString )
    }
  } catch {
    case err: Exception => logger.error( "Error in cleanup_spark_workers: " + err.toString )
  }

  //    appParameters( handler_type_key, "spark" ) match {
//      case exeMgr if exeMgr.toLowerCase.startsWith("future") =>
//        throw new Exception( "CDFuturesExecutionManager no currently supported.")
////        import nasa.nccs.cds2.engine.futures.CDFuturesExecutionManager
////        logger.info("\nExecuting Futures manager: serverConfig = " + exeMgr)
////        new CDFuturesExecutionManager()
//      case exeMgr if exeMgr.toLowerCase.startsWith("spark") =>
//        logger.info("\nExecuting Spark manager: serverConfig = " + exeMgr)
//        new CDSparkExecutionManager()
//      case x => throw new Exception("Unrecognized execution.manager.type: " + x)
//    }


  def getConfigParamValue( key: String, serverConfiguration: Map[String,String], default_val: String  ): String =
    serverConfiguration.get( key ) match {
      case Some( htype ) => htype
      case None => appParameters( key, default_val )
    }
  def generateRange( start: Long, end: Long, nTimeSteps: Int ): IndexedSeq[Double] = {
    val dT: Double =  ( end - start ) / ( nTimeSteps * 60000.0 )
    ( 0 until nTimeSteps ).map ( iT => start/60000.0 + ( iT + 0.5 )*dT )
  }

  def saveResultToFile(executor: WorkflowExecutor, result: QueryResultCollection, dsetMetadata: List[nc2.Attribute] = List.empty[nc2.Attribute] ): Iterable[String] = {
    if( result.records.isEmpty ) { return Iterable.empty }
    val multiFiles: Boolean = executor.requestCx.getConf("response", "xml") == "collection"
    val records_list: Array[(CDRecord,Int)] = if(multiFiles) { result.records.zipWithIndex } else { result.concatSlices.records.zipWithIndex }
    val file_lists = for( ( record, timeIndex ) <- records_list ) yield {
      val partitioned_records = record.partitionByShape.zipWithIndex
      for( ( (shape, record), fileIndex ) <-  partitioned_records ) yield {
        saveResultToFile(executor, record, result.getMetadata, dsetMetadata, fileIndex, timeIndex, partitioned_records.size )
      }
    }
    file_lists.toIterable.flatten
  }
  def ex( msg: String ) = throw new Exception( msg )

  def getAxisSize( shape: Array[Int], axis: String ): Int = {
    val axisIndex = axis match {
      case "X" => if( shape.length == 3 ) { 2 } else { 3 }
      case "T" => 0
      case "Z" => if( shape.length == 3 ) { ex("Illegal Z axis" ) } else { 1 }
      case "Y" => if( shape.length == 3 ) { 1 } else { 2 }
      case x =>  ex("Unrecognized axis: " + x )
    }
    shape(axisIndex)
  }

//  def generateCoordValues( newRange: ma2.Range, size: Int  ): ma2.Array = {
//    newRange.
//  }

  def saveResultToFile(executor: WorkflowExecutor, slice: CDRecord, varMetadata: Map[String,String], dsetMetadata: List[nc2.Attribute], fileIndex: Int, timeIndex: Int, nFiles: Int  ): String = {
    val t0 = System.nanoTime()
    val multiFiles = false
    val head_elem: ArraySpec = slice.elements.values.head
    val resultId: String = if(multiFiles) { executor.requestCx.jobId + "-" + slice.elements.keys.head } else { executor.requestCx.jobId }
    val chunker: Nc4Chunking = new Nc4ChunkingStrategyNone()
    val baseFileName = if( fileIndex == 0 ) { resultId } else { resultId + "-" + fileIndex }
    val fileName = if( timeIndex == 0 ) { baseFileName } else { baseFileName + "-" + timeIndex }
    val resultFile = Kernel.getResultFile( fileName, true )
    val path = resultFile.getAbsolutePath
    val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, path, chunker)
    var optGridDset: Option[NetcdfDataset] = None
    var originalDataset: Option[NetcdfDataset] = None
    try {
      logger.info(s" #CV#  Saving slice, elems: [ ${slice.elements.keys.mkString(", ")} ], shape: [${slice.elements.head._2.shape.mkString(",")}], sliceMetadata: ${slice.metadata.toString()}, varMetadata: ${varMetadata.toString()}, dsetMetadata: [ ${dsetMetadata.map( attr => attr.getShortName + ": " +  attr.getStringValue ).mkString(",")} ] "  )
      val inputSpec: DataFragmentSpec = executor.requestCx.getInputSpec().getOrElse( throw new Exception( s"Missing InputSpec in saveResultToFile for result $resultId"))
      val shape: Array[Int] = head_elem.shape
      val timeValues: IndexedSeq[Double] = generateRange( slice.startTime , slice.endTime, shape(0) )
      val dateRange = List( new DateTime(slice.startTime).toString, new DateTime(slice.endTime).toString ).mkString(", ")
      val timeUnits: String = "minutes since 1970-01-01T00:00:00Z" // EDTime.units
      val gridFileOpt: Option[String] = varMetadata.get( "gridspec" )
      val targetGrid: TargetGrid = executor.getTargetGrid.getOrElse( throw new Exception( s"Missing Target Grid in saveResultToFile for result $resultId"))
      originalDataset = Some( targetGrid.grid.grid.getGridDataset(false) )
      val t1 = System.nanoTime()
      val dimsMap: Map[String,nc2.Dimension] = targetGrid.grid.axes.indices.map(idim => {
        val axisSpec = targetGrid.grid.getAxisSpec(idim)
        axisSpec.coordAxis.getAxisType.getCFAxisName -> writer.addDimension(null, axisSpec.getAxisName, shape(idim))
      }).toMap
      val t2 = System.nanoTime()
      val gblTimeCoordAxis = targetGrid.getTimeCoordAxis.getOrElse( throw new Exception( s"Missing Time Axis in Target Grid in saveResultToFile for result $resultId"))
//      val gblTimeCoordAxis = originalDataset.flatMap( dset => CDGrid.getTimeAxis(dset)).getOrElse( throw new Exception( s"Missing Time Axis in Target Grid in saveResultToFile for result $resultId"))
      val t3 = System.nanoTime()
      val timeCoordAxis = gblTimeCoordAxis.section( inputSpec.roi.getRange(0) )
      val t4 = System.nanoTime()

      val coordAxes: List[CoordinateAxis] = gridFileOpt match {
        case Some( gridFilePath ) =>
          val gridDSet = NetcdfDataset.openDataset(gridFilePath)
          gridDSet.getCoordinateAxes.toList :+ timeCoordAxis
        case None =>
          targetGrid.grid.grid.getCoordinateAxes :+ timeCoordAxis
      }

      logger.info(" WWW Writing result %s to file '%s', vars=[%s], dims=(%s), shape=[%s], coords = [%s], roi=[%s], dateRange=[%s]".format(
        resultId, path, slice.elements.keys.mkString(","), dimsMap.mapValues( dim => s"${dim.getShortName}:${dim.getLength}" ).mkString(","), shape.mkString(","),
        coordAxes.map { caxis => "%s: (%s)".format(caxis.getShortName, caxis.getShape.mkString(",")) }.mkString(","), inputSpec.roi.toString, dateRange ) )


//      val newCoordVars: List[(nc2.Variable, ma2.Array)] = coordAxes.map( coordAxis => {
//        val coordVar: nc2.Variable = writer.addVariable(null, coordAxis.getShortName, coordAxis.getDataType, coordAxis.getShortName)
//        for (attr <- coordAxis.getAttributes) writer.addVariableAttribute(coordVar, attr)
//        val data = coordAxis.read()
//        (coordVar, data)
//      })

      val optInputSpec: Option[DataFragmentSpec] = executor.requestCx.getInputSpec()
      val axisTypes = if( shape.length == 4 ) Array("T", "Z", "Y", "X" )  else Array("T", "Y", "X" )
      logger.info( s" InputSpec: {${optInputSpec.fold("")(_.getMetadata().mkString(", "))}} ")
      val newCoordVars: List[(nc2.Variable, ma2.Array)] = (for (coordAxis <- coordAxes) yield optInputSpec flatMap { inputSpec =>
        inputSpec.getRangeCF( coordAxis.getAxisType.getCFAxisName ) match {
          case Some(range) =>
            val coordDataType = if( coordAxis.getAxisType == AxisType.Time ) { DataType.DOUBLE } else { coordAxis.getDataType }
            val dims = dimsMap.get( coordAxis.getAxisType.getCFAxisName ).toList
            val coordAxisName = dims.head.getShortName
            val newVar = writer.addVariable( null, coordAxisName, coordAxis.getDataType, dims )
            val coordVar: nc2.Variable = if( newVar == null ) { writer.findVariable(coordAxisName) } else { newVar }
            if( coordVar == null ) { logger.info("#CV# X2: " + coordAxisName ); None }
            else {
              writer.addVariableAttribute( coordVar, new Attribute( "axis", coordAxis.getAxisType.getCFAxisName.toUpperCase ) )
              writer.addVariableAttribute( coordVar, new Attribute( "standard_name", coordAxis.getDODSName ) )
              writer.addVariableAttribute( coordVar, new Attribute( "long_name", coordAxis.getFullName ) )
              for (attr <- coordAxis.getAttributes; if attr != null) { writer.addVariableAttribute(coordVar, attr) }
              if (coordAxis.getAxisType == AxisType.Time) {
                coordVar.addAttribute(new Attribute("units", timeUnits))
              }
              val newRange = dimsMap.get(coordAxis.getAxisType.getCFAxisName).fold(range)(dim =>
                if (dim.getLength == 1) {
                  val center = (range.first + range.last) / 2; new ma2.Range(range.getName, center, center)
                } else {
                  range
                }
              )
              val data = if (coordAxis.getAxisType == AxisType.Time) {
                ma2.Array.factory(timeValues.toArray)
              } else {
                if (dims.head.getLength == 1) {
                  ma2.Array.makeArray(coordDataType, Array("0"))
                } else if (coordAxis.getSize > newRange.length) {
                  coordAxis.read(List(newRange))
                } else {
                  coordAxis.read()
                }

              }
              Some(coordVar, data)
            }
          case None => logger.info("#CV# X1: " + coordAxis.getShortName ); None
        }
      }).flatten

      val varDims: Array[Dimension] = axisTypes.map( aType => dimsMap.getOrElse(aType, throw new Exception( s"Missing coordinate type ${aType} in saveResultToFile") ) )
      val dataMap: Map[String,CDFloatArray] = slice.elements.mapValues( _.toCDFloatArray )
      val variables = dataMap.map { case ( tname, maskedTensor ) =>
        val optVarName = executor.getRelatedInputSpec( tname.split('-').head ).map( _.varname )
        val baseNameOpt  = varMetadata.get("name" ).map( _.replace(' ','_') )
        val varname: String = List( optVarName, baseNameOpt, Some(tname) ).flatten.mkString("-")
        logger.info("Creating var %s: dims = [%s], data sample = [ %s ]".format(varname, varDims.map( _.getShortName).mkString(", "), maskedTensor.getSectionArray( Math.min(10,maskedTensor.getSize.toInt) ).mkString(", ") ) )
        var variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, varDims.toList )
        if( variable == null ) { variable = writer.findVariable(varname) }
        assert( variable != null, s"Error creating variable $varname with dims [${varDims.toList.mkString(",")}] in output file $baseFileName" )
        varMetadata map { case (key, value) => if( (key != null) && (value != null) ) { variable.addAttribute(new Attribute(key, value)) } }
        variable.addAttribute(new nc2.Attribute("missing_value", maskedTensor.getInvalid))
        dsetMetadata.foreach(attr => writer.addGroupAttribute(null, attr))
        ( variable, maskedTensor )
      }
      writer.addGroupAttribute( null, new Attribute("nFiles", nFiles) )
      writer.create()

      for ((coordVar, coordData) <- newCoordVars) {
        logger.info("#CV# Writing cvar %s: var shape = [%s], data shape = [%s], dataType = %s".format(coordVar.getShortName, coordVar.getShape.mkString(","), coordData.getShape.mkString(","), coordVar.getDataType.toString ))
        try {
          logger.info("#CV# Coord data sample = [%f,%f,...], size = %d".format( coordData.getFloat(0), coordData.getFloat(1), coordData.getSize ) )
          writer.write(coordVar, coordData)
        } catch { case ex: Exception => logger.info("#CV# MISSING Coord data: " + ex.getMessage + " -> " + ex.getStackTrace.head.toString ) }
      }
      variables.foreach { case (variable, maskedTensor) => {
        logger.info(" #V# Writing var %s: var shape = [%s], data Shape = %s".format(variable.getShortName, variable.getShape.mkString(","), maskedTensor.getShape.mkString(",") ))
        writer.write(variable, maskedTensor)
      } }

      writer.close()
      originalDataset.foreach( _.close )
      optGridDset.foreach( _.close )
      logger.info("Done writing output to file %s, time = %.3f ( %.3f, %.3f, %.3f, %.3f )".format(path,(System.nanoTime() - t0) / 1.0E9, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t3 - t2) / 1.0E9, (t4 - t3) / 1.0E9 ) )
//      println( "\n ------ ---> Saving output to:" + path + "\n")

    } catch {
      case ex: IOException =>
        logger.error("*** ERROR creating file %s%n%s".format(resultFile.getAbsolutePath, ex.getMessage()));
        ex.printStackTrace(logger.writer)
        ex.printStackTrace()
        throw ex
    }
    path
  }

//  def saveRecordToFile( resultId: String, grid: Grid, slice: CDRecord, varMetadata: Map[String,String], dsetMetadata: List[nc2.Attribute], fileIndex: Int, timeIndex: Int, nFiles: Int  ): String = {
//    val t0 = System.nanoTime()
//    val multiFiles = false
//    val head_elem: ArraySpec = slice.elements.values.head
//    val chunker: Nc4Chunking = new Nc4ChunkingStrategyNone()
//    val baseFileName = if( fileIndex == 0 ) { resultId } else { resultId + "-" + fileIndex }
//    val fileName = if( timeIndex == 0 ) { baseFileName } else { baseFileName + "-" + timeIndex }
//    val resultFile = Kernel.getResultFile( fileName, true )
//    val path = resultFile.getAbsolutePath
//    val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, path, chunker)
//    var optGridDset: Option[NetcdfDataset] = None
//    var originalDataset: Option[NetcdfDataset] = None
//    try {
//      logger.info(s" #CV#  Saving slice, elems: [ ${slice.elements.keys.mkString(", ")} ], shape: [${slice.elements.head._2.shape.mkString(",")}], sliceMetadata: ${slice.metadata.toString()}, varMetadata: ${varMetadata.toString()}, dsetMetadata: [ ${dsetMetadata.map( attr => attr.getShortName + ": " +  attr.getStringValue ).mkString(",")} ] "  )
//      val shape: Array[Int] = head_elem.shape
//      val timeValues: IndexedSeq[Double] = generateRange( slice.startTime , slice.endTime, shape(0) )
//      val dateRange = List( new DateTime(slice.startTime).toString, new DateTime(slice.endTime).toString ).mkString(", ")
//      val timeUnits: String = "minutes since 1970-01-01T00:00:00Z" // EDTime.units
//      val gridFileOpt: Option[String] = varMetadata.get( "gridspec" )
//      val t1 = System.nanoTime()
//      val coordsList: List[(CoordAxis,Int)] = grid.getAxes
//      for( (coord,idim) <- coordsList ) { writer.addDimension(null, coord.dims.head.name, shape(idim)) }
//      val t2 = System.nanoTime()
//      val gblTimeCoordAxis = grid.getAxis("T")
//      val t3 = System.nanoTime()
//      val timeCoordAxis = gblTimeCoordAxis.section( inputSpec.roi.getRange(0) )
//      val t4 = System.nanoTime()
//
//      val coordAxes: List[CoordinateAxis] = gridFileOpt match {
//        case Some( gridFilePath ) =>
//          val gridDSet = NetcdfDataset.openDataset(gridFilePath)
//          gridDSet.getCoordinateAxes.toList :+ timeCoordAxis
//        case None =>
//          targetGrid.grid.grid.getCoordinateAxes :+ timeCoordAxis
//      }
//
//      logger.info(" WWW Writing result %s to file '%s', vars=[%s], dims=(%s), shape=[%s], coords = [%s], roi=[%s], dateRange=[%s]".format(
//        resultId, path, slice.elements.keys.mkString(","), dimsMap.mapValues( dim => s"${dim.getShortName}:${dim.getLength}" ).mkString(","), shape.mkString(","),
//        coordAxes.map { caxis => "%s: (%s)".format(caxis.getShortName, caxis.getShape.mkString(",")) }.mkString(","), inputSpec.roi.toString, dateRange ) )
//
//
//      //      val newCoordVars: List[(nc2.Variable, ma2.Array)] = coordAxes.map( coordAxis => {
//      //        val coordVar: nc2.Variable = writer.addVariable(null, coordAxis.getShortName, coordAxis.getDataType, coordAxis.getShortName)
//      //        for (attr <- coordAxis.getAttributes) writer.addVariableAttribute(coordVar, attr)
//      //        val data = coordAxis.read()
//      //        (coordVar, data)
//      //      })
//
//      val axisTypes = if( shape.length == 4 ) Array("T", "Z", "Y", "X" )  else Array("T", "Y", "X" )
//      val newCoordVars: List[(nc2.Variable, ma2.Array)] = (for (coordAxis <- coordAxes) yield optInputSpec flatMap { inputSpec =>
//        inputSpec.getRangeCF( coordAxis.getAxisType.getCFAxisName ) match {
//          case Some(range) =>
//            val coordDataType = if( coordAxis.getAxisType == AxisType.Time ) { DataType.DOUBLE } else { coordAxis.getDataType }
//            val dims = dimsMap.get( coordAxis.getAxisType.getCFAxisName ).toList
//            val coordAxisName = dims.head.getShortName
//            val newVar = writer.addVariable( null, coordAxisName, coordAxis.getDataType, dims )
//            val coordVar: nc2.Variable = if( newVar == null ) { writer.findVariable(coordAxisName) } else { newVar }
//            if( coordVar == null ) { logger.info("#CV# X2: " + coordAxisName ); None }
//            else {
//              writer.addVariableAttribute( coordVar, new Attribute( "axis", coordAxis.getAxisType.getCFAxisName.toUpperCase ) )
//              writer.addVariableAttribute( coordVar, new Attribute( "standard_name", coordAxis.getDODSName ) )
//              writer.addVariableAttribute( coordVar, new Attribute( "long_name", coordAxis.getFullName ) )
//              for (attr <- coordAxis.getAttributes; if attr != null) { writer.addVariableAttribute(coordVar, attr) }
//              if (coordAxis.getAxisType == AxisType.Time) {
//                coordVar.addAttribute(new Attribute("units", timeUnits))
//              }
//              val newRange = dimsMap.get(coordAxis.getAxisType.getCFAxisName).fold(range)(dim =>
//                if (dim.getLength == 1) {
//                  val center = (range.first + range.last) / 2; new ma2.Range(range.getName, center, center)
//                } else {
//                  range
//                }
//              )
//              val data = if (coordAxis.getAxisType == AxisType.Time) {
//                ma2.Array.factory(timeValues.toArray)
//              } else {
//                if (dims.head.getLength == 1) {
//                  ma2.Array.makeArray(coordDataType, Array("0"))
//                } else if (coordAxis.getSize > newRange.length) {
//                  coordAxis.read(List(newRange))
//                } else {
//                  coordAxis.read()
//                }
//
//              }
//              Some(coordVar, data)
//            }
//          case None => logger.info("#CV# X1: " + coordAxis.getShortName ); None
//        }
//      }).flatten
//
//      val varDims: Array[Dimension] = axisTypes.map( aType => dimsMap.getOrElse(aType, throw new Exception( s"Missing coordinate type ${aType} in saveResultToFile") ) )
//      val dataMap: Map[String,CDFloatArray] = slice.elements.mapValues( _.toCDFloatArray )
//      val variables = dataMap.map { case ( tname, maskedTensor ) =>
//        val optVarName = executor.getRelatedInputSpec( tname.split('-').head ).map( _.varname )
//        val baseName  = varMetadata.getOrElse("name", varMetadata.getOrElse("longname", "result") ).replace(' ','_')
//        val varname: String = optVarName.getOrElse( baseName + "-" + tname )
//        logger.info("Creating var %s: dims = [%s], data sample = [ %s ]".format(varname, varDims.map( _.getShortName).mkString(", "), maskedTensor.getSectionArray( Math.min(10,maskedTensor.getSize.toInt) ).mkString(", ") ) )
//        val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, varDims.toList )
//        varMetadata map { case (key, value) => variable.addAttribute(new Attribute(key, value)) }
//        variable.addAttribute(new nc2.Attribute("missing_value", maskedTensor.getInvalid))
//        dsetMetadata.foreach(attr => writer.addGroupAttribute(null, attr))
//        ( variable, maskedTensor )
//      }
//      writer.addGroupAttribute( null, new Attribute("nFiles", nFiles) )
//      writer.create()
//
//      for ((coordVar, coordData) <- newCoordVars) {
//        logger.info("#CV# Writing cvar %s: var shape = [%s], data shape = [%s], dataType = %s".format(coordVar.getShortName, coordVar.getShape.mkString(","), coordData.getShape.mkString(","), coordVar.getDataType.toString ))
//        try {
//          logger.info("#CV# Coord data sample = [%f,%f,...], size = %d".format( coordData.getFloat(0), coordData.getFloat(1), coordData.getSize ) )
//          writer.write(coordVar, coordData)
//        } catch { case ex: Exception => logger.info("#CV# MISSING Coord data: " + ex.getMessage + " -> " + ex.getStackTrace.head.toString ) }
//      }
//      variables.foreach { case (variable, maskedTensor) => {
//        logger.info(" #V# Writing var %s: var shape = [%s], data Shape = %s".format(variable.getShortName, variable.getShape.mkString(","), maskedTensor.getShape.mkString(",") ))
//        writer.write(variable, maskedTensor)
//      } }
//
//      writer.close()
//      originalDataset.foreach( _.close )
//      optGridDset.foreach( _.close )
//      logger.info("Done writing output to file %s, time = %.3f ( %.3f, %.3f, %.3f, %.3f )".format(path,(System.nanoTime() - t0) / 1.0E9, (t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9, (t3 - t2) / 1.0E9, (t4 - t3) / 1.0E9 ) )
//      //      println( "\n ------ ---> Saving output to:" + path + "\n")
//
//    } catch {
//      case ex: IOException =>
//        logger.error("*** ERROR creating file %s%n%s".format(resultFile.getAbsolutePath, ex.getMessage()));
//        ex.printStackTrace(logger.writer)
//        ex.printStackTrace()
//        throw ex
//    }
//    path
//  }

  def searchForAttrValue(metadata: Map[String, nc2.Attribute], keys: List[String], default_val: String): String = {
    keys.length match {
      case 0 => default_val
      case x => metadata.get(keys.head) match {
        case Some(valueAttr) => valueAttr.getStringValue()
        case None => searchForAttrValue(metadata, keys.tail, default_val)
      }
    }
  }

  def searchForValue(metadata: Map[String,String], keys: List[String], default_val: String): String = {
    keys.length match {
      case 0 => default_val
      case x => metadata.get(keys.head) match {
        case Some(valueAttr) => valueAttr
        case None => searchForValue(metadata, keys.tail, default_val)
      }
    }
  }

}

abstract class TestProcess( val id: String ) extends Loggable {
  def execute( spark: CDSparkContext, jobId: String, optRequest: Option[TaskRequest]=None, run_args: Map[String, String]=Map.empty ): WPSMergedEventReport;
}

class EDASExecutionManager extends WPSServer with Loggable {
  import EDASExecutionManager._
  shutdown_python_workers()

  val serverContext = new ServerContext( collectionDataCache, CDSparkContext() )
  val kernelManager = new KernelMgr()
  val EDAS_CACHE_DIR = sys.env("EDAS_CACHE_DIR")
  val USER = sys.env("USER")

  val cleanupManager = new CleanupManager()
                          .addFileCleanupTask( Kernel.getResultDir.getPath, 2.0f, false, ".*" )
                          .addFileCleanupTask( EDAS_CACHE_DIR, 1.0f, true, "blockmgr-.*" )
                          .addFileCleanupTask( EDAS_CACHE_DIR, 1.0f, true, "spark-.*" )
                          .addFileCleanupTask( EDAS_CACHE_DIR + "/collections/agg", 0.1f, true, ".*CdmsRegrid.*" )
                          .addFileCleanupTask( s"/tmp/$USER/logs", 4.0f, true, ".*" )


//  def getOperationInputs( context: EDASExecutionContext ): Map[String,OperationInput] = {
//    val items = for (uid <- context.operation.inputs) yield {
//      context.request.getInputSpec(uid) match {
//        case Some(inputSpec) =>
//          logger.info("getInputSpec: %s -> %s ".format(uid, inputSpec.longname))
//          uid -> context.server.getOperationInput(inputSpec)
//        case None => collectionDataCache.getExistingResult(uid) match {
//          case Some(tVar: RDDTransientVariable) =>
//            logger.info("getExistingResult: %s -> %s ".format(uid, tVar.result.elements.values.head.metadata.mkString(",")))
//            uid -> new OperationTransientInput(tVar)
//          case None => throw new Exception("Unrecognized input id: " + uid)
//        }
//      }
//    }
//    Map(items:_*)
//  }

  def describeWPSProcess( process: String, response_syntax: ResponseSyntax.Value ): xml.Elem = DescribeProcess( process, response_syntax  )

  def getProcesses: Map[String,WPSProcess] = kernelManager.getKernelMap(visibility)

  def getKernelModule( moduleName: String  ): KernelModule = {
    kernelManager.getModule( moduleName.toLowerCase ) match {
      case Some(kmod) => kmod
      case None => throw new Exception("Unrecognized Kernel Module %s, modules = %s ".format( moduleName, kernelManager.getModuleNames.mkString("[ ",", "," ]") ) )
    }
  }
  def getResourcePath( resource: String ): Option[String] = Option(getClass.getResource(resource)).map( _.getPath )

  def getKernel( moduleName: String, operation: String  ): Kernel = {
    val kmod = getKernelModule( moduleName )
    kmod.getKernel( operation  ) match {
      case Some(kernel) => kernel
      case None =>
        throw new Exception( s"Unrecognized Kernel %s in Module %s, kernels = %s ".format( operation, moduleName, kmod.getKernelNames.mkString("[ ",", "," ]")) )
    }
  }
  def getKernel( kernelName: String  ): Kernel = {
    val toks = kernelName.split('.')
    getKernel( toks.dropRight(1).mkString("."), toks.last )
  }

  def fatal(err: Throwable): String = {
    logger.error( "\nError Executing Kernel: %s\n".format(err.getMessage) )
    val sw = new StringWriter
    err.printStackTrace(new PrintWriter(sw))
    logger.error( sw.toString )
    err.getMessage
  }

//  def createTargetGrid( request: TaskRequest ): TargetGrid = {
//    request.metadata.get("id") match {
//      case Some(varId) => request.variableMap.get(varId) match {
//        case Some(dataContainer: DataContainer) =>
//          serverContext.createTargetGrid( dataContainer, request.getDomain(dataContainer.getSource) )
//        case None => throw new Exception( "Unrecognized variable id in Grid spec: " + varId )
//      }
//      case None => throw new Exception("Target grid specification method has not yet been implemented: " + request.metadata.toString)
//    }
//  }

  def createRequestContext( jobId: String, request: TaskRequest, run_args: Map[String,String], executionCallback: Option[ExecutionCallback] = None ): RequestContext = {
    val sourceContainers = request.variableMap.values.filter(_.isSource)
    val profileParm: String = request.operations.flatMap( _.getConfParm("profile") ).headOption.getOrElse( run_args.getOrElse("profile","") )
    val sources = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource ) yield {
      val domainOpt: Option[DomainContainer] = data_container.getSource.getDomain.flatMap(request.getDomain)
      serverContext.createInputSpec( data_container, domainOpt, request )
    }
    val sourceMap: Map[String,Option[DataFragmentSpec]] = Map(sources.toSeq:_*)
    val rc = new RequestContext ( jobId, sourceMap, request, run_args, executionCallback )
    logger.info( s"#P# Initialize Profiler: '${profileParm}'" )
    rc.initializeProfiler( run_args.getOrElse( "profile", profileParm ), serverContext.spark.sparkContext )
    rc
  }

//  def cacheInputData(request: TaskRequest, run_args: Map[String, String] ): Iterable[Option[(DataFragmentKey, Future[PartitionedFragment])]] = {
//    val sourceContainers = request.variableMap.values.filter(_.isSource)
//    for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource; domain <- data_container.getSource.getDomain.map(request.getDomain) )
//      yield serverContext.cacheInputData( data_container, run_args, domain, request.getTargetGrid(data_container), None )
//  }

  def deleteFragments( fragIds: Iterable[String] ) = {
    logger.info("Deleting frags: " + fragIds.mkString(", ") + "; Current Frags = " + FragmentPersistence.getFragmentIdList.mkString(", "))
    serverContext.deleteFragments( fragIds )
  }

  def clearCache: Set[String] = serverContext.clearCache

  def isCollectionPath( path: File ): Boolean = { path.isDirectory || path.getName.endsWith(".csv") }

  def executeUtilityRequest(jobId: String, util_id: String, request: TaskRequest, run_args: Map[String, String]): WPSMergedEventReport = {
    logger.info("executeUtilityRequest, test processses: " + _testProcesses.keys.mkString(", ") )
    _testProcesses.get(util_id) match {
      case Some( testProcess ) => testProcess.execute( serverContext.spark, jobId, Some(request), run_args )
      case None => runUtilityRequest( jobId, util_id, request, run_args )
    }
  }

  def runUtilityRequest( jobId: String, util_id: String, request: TaskRequest, run_args: Map[String, String] ): WPSMergedEventReport = {
    val result = util_id match {
//      case "magg" =>
//        val collectionNodes =  request.variableMap.values.flatMap( ds => {
//          val pcol = ds.getSource.collection
//          val base_dir = new File(pcol.dataPath)
//          val base_id = pcol.id
//          val col_dirs: Array[File] = base_dir.listFiles
//          for( col_path <- col_dirs; if isCollectionPath(col_path); col_id = base_id + "/" + col_path.getName ) yield {
//            Collection.aggregate( col_id, col_path )
//          }
//        })
//        new WPSMergedEventReport( collectionNodes.map( cnode => new UtilityExecutionResult( "aggregate", cnode )).toList )
//      case "agg" =>
//        val collectionNodes =  request.variableMap.values.flatMap( ds => if(ds.isSource) { Some( Collection.aggregate( ds.getSource ) ) } else { None } )
//        new WPSMergedEventReport( collectionNodes.map( cnode => new UtilityExecutionResult( "aggregate", cnode )).toList )
      case "clearCache" =>
        val fragIds = clearCache
        new WPSMergedEventReport( List( new UtilityExecutionResult( "clearCache", <deleted fragments={fragIds.mkString(",")}/> ) ) )
//      case "cache" =>
//        val cached_data: Iterable[(DataFragmentKey,Future[PartitionedFragment])] = cacheInputData(request, run_args).flatten
//        FragmentPersistence.close()
//        new WPSMergedEventReport( cached_data.map( cache_result => new UtilityExecutionResult( cache_result._1.toStrRep, <cache/> ) ).toList )
      case "dcol" =>
        val colIds = request.variableMap.values.map( _.getSource.collection.id )
        val deletedCollections = Collections.removeCollections( colIds.toArray )
        new WPSMergedEventReport(List(new UtilityExecutionResult("dcol", <deleted collections={deletedCollections.mkString(",")}/> )))
      case "dfrag" =>
        val fragIds: Iterable[String] = request.variableMap.values.flatMap( ds => ds.getSource.getDomain.map( domId => Array( ds.getSource.name, ds.getSource.collection.id, domId ).mkString("|") ) )
        deleteFragments( fragIds )
        new WPSMergedEventReport(List(new UtilityExecutionResult("dfrag", <deleted fragments={fragIds.mkString(",")}/> )))
      case "dres" =>
        val resIds: Iterable[String] = request.variableMap.values.map( ds => ds.uid )
        logger.info( "Deleting results: " + resIds.mkString(", ") + "; Current Results = " + collectionDataCache.getResultIdList.mkString(", ") )
        resIds.foreach( resId => collectionDataCache.deleteResult( resId ) )
        new WPSMergedEventReport(List(new UtilityExecutionResult("dres", <deleted results={resIds.mkString(",")}/> )))
      case x => throw new Exception( "Unrecognized Utility:" + x )
    }
    collectionDataCache.removeJob( jobId )
    result
  }

  def futureExecute( jobId: String, request: TaskRequest, run_args: Map[String,String], executionCallback: Option[ExecutionCallback] = None ): Future[WPSResponse] = Future {
    logger.info("ASYNC Execute { runargs: " + run_args.toString + ",  request: " + request.toString+ ",  jobId: " + jobId + " }")
    try {
      val requestContext = createRequestContext( jobId, request, run_args, executionCallback )
      val results = executeWorkflows(requestContext)
      val response = results.toXml(ResponseSyntax.Generic)
      executionCallback.foreach(_.success(response))
      results
    } catch {
      case err: Exception =>
        executionCallback.foreach( _.failure(err.getMessage) )
        runtime.printMemoryUsage
        new WPSExceptionReport(err)
    } finally {
      collectionDataCache.removeJob(jobId)
    }
  }

  def blockingExecute( jobId: String, request: TaskRequest, run_args: Map[String,String], executionCallback: Option[ExecutionCallback] = None ): WPSResponse =  {
    //    runtime.printMemoryUsage(logger)
    val t0 = System.nanoTime
    val req_ids = request.name.split('.')
    val opNames = request.operations.headOption.fold( Array("") )( _.name.split('.') )
    logger.info("Blocking Execute { runargs: " + run_args.toString + ", request: " + request.toString + " }")
    if(  req_ids.head.equals("util") ) {
      logger.info( "Executing utility request " + req_ids(1) )
      executeUtilityRequest(jobId, req_ids(1), request, run_args)
    } else if( opNames.head.equals("util") ) {
      logger.info( "Executing utility request " + opNames(1) )
      executeUtilityRequest( jobId, opNames(1), request, run_args )
    } else {
      logger.info("Executing task request " + request.name )
      val requestContext = createRequestContext ( jobId, request, run_args )
      val results = executeWorkflows ( requestContext )
      val response = results.toXml( ResponseSyntax.Generic )
      executionCallback.foreach( _.success( response ) )
      collectionDataCache.removeJob( jobId )
      results
    }
  }

//  def futureExecute( request: TaskRequest, run_args: Map[String,String] ): Future[xml.Elem] = Future {
//    try {
//      val sourceContainers = request.variableMap.values.filter(_.isSource)
//      val inputFutures: Iterable[Future[DataFragmentSpec]] = for (data_container: DataContainer <- request.variableMap.values; if data_container.isSource) yield {
//        serverContext.dataLoader.loadVariableDataFuture(data_container, request.getDomain(data_container.getSource))
//      }
//      inputFutures.flatMap( inputFuture => for( input <- inputFuture ) yield executeWorkflows(request, run_args).toXml )
//    } catch {
//      case err: Exception => fatal(err)
//    }
//  }

  def getResultVariable( resId: String ): Option[RDDTransientVariable] = collectionDataCache.getExistingResult( resId )
  def getResultVariables: Iterable[String] = collectionDataCache.getResultIdList

  def getResultFilePath( executionResult: KernelExecutionResult, executor: WorkflowExecutor ): List[String] = {
    if( executionResult.holdsData ) {
      val resId = executor.requestCx.jobId
      getResultVariable(resId) match {
        case Some(tvar: RDDTransientVariable) =>
          val resultFile = Kernel.getResultFile(resId)
          if (resultFile.exists) List(resultFile.getAbsolutePath)
          else {
            logger.info( s" #RS# Result ${resId}, metadata: [${tvar.result.metadata.mkString(",")}] " )
            saveResultToFile(executor, tvar.result ).toList.filter( ! _.isEmpty )
          }
        case None => List.empty[String]
      }
    } else { executionResult.files }
  }

  def getResult( resId: String, response_syntax: ResponseSyntax.Value ): xml.Node = {
    logger.info( "Locating result: " + resId )
    collectionDataCache.getExistingResult( resId ) match {
      case None =>
        new WPSMergedExceptionReport( List( new WPSExceptionReport( new Exception("Unrecognized resId: " + resId + ", existing resIds: " + collectionDataCache.getResultIdList.mkString(", ") )) ) ).toXml(response_syntax)
      case Some( tvar: RDDTransientVariable ) =>
        new WPSExecuteResult( "WPS", tvar ).toXml(response_syntax)
    }
  }

  def getResultStatus( resId: String, response_syntax: ResponseSyntax.Value ): xml.Node = {
    val message = collectionDataCache.getExistingResult( resId ) match {
      case None =>
        collectionDataCache.findJob(resId) match {
          case Some( jobRec: JobRecord ) => new WPSExecuteStatusStarted("WPS", "", resId, jobRec.elapsed ).toXml(response_syntax)
          case None =>
            logger.warn( s" %J% >> Can't find job for resId: ${resId}" )
            new WPSExecuteStatusStarted("WPS", "", resId, 0 ).toXml(response_syntax)
        }
      case Some( tvar: RDDTransientVariable ) => new WPSExecuteStatusCompleted( "WPS", "", resId ).toXml( response_syntax )
    }
    message
  }

  def asyncExecute( jobId: String, request: TaskRequest, run_args: Map[String,String], executionCallback: Option[ExecutionCallback] = None ): WPSReferenceExecuteResponse = {
    logger.info("Execute { runargs: " + run_args.toString + ",  request: " + request.toString + ",  jobId: " + jobId + " }")
//    runtime.printMemoryUsage(logger)
    val req_ids = request.name.split('.')
    req_ids(0) match {
      case "util" =>
        val util_result = executeUtilityRequest( jobId, req_ids(1), request, run_args )
        Future(util_result)
      case _ =>
        val futureResult = this.futureExecute( jobId, request, run_args, executionCallback )
        futureResult onFailure { case e: Throwable =>
          fatal(e);
          collectionDataCache.removeJob(jobId);
          throw e
        }
    }
    new AsyncExecutionResult( request.id.toString, List(request.getProcess), jobId )
  }

//  def execute( request: TaskRequest, runargs: Map[String,String] ): xml.Elem = {
//    val async = runargs.getOrElse("async","false").toBoolean
//    if(async) executeAsync( request, runargs ) else  blockingExecute( request, runargs )
//  }

  def getWPSCapabilities( identifier: String, response_syntax: ResponseSyntax.Value ): xml.Elem =
    identifier match {
      case x if x.startsWith("ker") => kernelManager.toXml
      case x if x.startsWith("frag") => FragmentPersistence.getFragmentListXml
      case x if x.startsWith("res") => collectionDataCache.getResultListXml // collectionDataCache
      case x if x.startsWith("job") => collectionDataCache.getJobListXml
      case x if x.startsWith("col") => {
        val itToks = x.split(Array(':','|'))
        if( itToks.length < 2 ) Collections.toXml
        else <collection id={itToks(0)}> { Collections.getCollectionMetadata( itToks(1) ).map( attr => attrToXml( attr ) ) } </collection>
      }
      case x if x.startsWith("op") => kernelManager.getModulesXml
      case x if x.startsWith("var") => {
        println( "getCapabilities->identifier: " + identifier )
        val itToks = x.split(Array(':','|'))
        if( itToks.length < 2 )  <error message="Unspecified collection and variables" />
        else                     Collections.getVariableListXml( itToks(1).split(',') )
      }
      case _ => GetCapabilities(response_syntax)
    }

  def attrToXml( attr: nc2.Attribute ): xml.Elem = {
    val sb = new StringBuffer()
    val svals = for( index <- (0 until attr.getLength) )  {
      if( index > 0 ) sb.append(",")
      if (attr.isString) sb.append(attr.getStringValue(index)) else sb.append(attr.getNumericValue(index))
    }
    <attr id={attr.getShortName.split("--").last}> { sb.toString } </attr>
  }

  def executeWorkflows( requestCx: RequestContext ): WPSResponse = {
    val task = requestCx.task
    val results = task.operations.headOption match {
      case Some(opContext) => opContext.moduleName match {
        case "util" =>  new WPSMergedEventReport( task.operations.map( utilityExecution( _, requestCx )))
        case x =>
          logger.info( "Execute Workflow: " + task.operations.mkString(",") )
          val responses = task.workflow.executeRequest( requestCx )
          new MergedWPSExecuteResponse( requestCx.jobId, responses )
      }
      case None => throw new Exception( "Error, no operation specified, cannot define workflow")
    }
    FragmentPersistence.close()
//    requestCx.saveTimingReport( "/tmp/edas-profile.txt", s"Request: ${requestCx.jobId}" )
    results
  }

  def executeUtility( operationCx: OperationContext, requestCx: RequestContext ): UtilityExecutionResult = {
    val report: xml.Elem =  <ReportText> {"Completed executing utility " + operationCx.name.toLowerCase } </ReportText>
    new UtilityExecutionResult( operationCx.name.toLowerCase + "~u0", report )
  }

  def utilityExecution( operationCx: OperationContext, requestCx: RequestContext ): UtilityExecutionResult = {
    logger.info( " ***** Utility Execution: utilName=%s, >> Operation = %s ".format( operationCx.name, operationCx.toString ) )
    executeUtility( operationCx, requestCx )
  }
}

//object SampleTaskRequests {
//
//  def createTestData() = {
//    var axes = Array("time","lev","lat","lon")
//    var shape = Array(1,1,180,360)
//    val maskedTensor: CDFloatArray = CDFloatArray( shape, Array.fill[Float](180*360)(1f), Float.MaxValue)
//    val varname = "ta"
//    val resultFile = "/tmp/SyntheticTestData.nc"
//    val writer: nc2.NetcdfFileWriter = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, resultFile )
//    val dims: IndexedSeq[nc2.Dimension] = shape.indices.map( idim => writer.addDimension(null, axes(idim), maskedTensor.getShape(idim)))
//    val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, dims.toList)
//    variable.addAttribute( new nc2.Attribute( "missing_value", maskedTensor.getInvalid ) )
//    writer.create()
//    writer.write( variable, maskedTensor )
//    writer.close()
//    println( "Writing result to file '%s'".format(resultFile) )
//  }
//
//  def getSpatialAve(collection: String, varname: String, weighting: String, level_index: Int = 0, time_index: Int = 0): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
//      "operation" -> List( Map( "input"->"v0", "axes"->"xy", "weights"->weighting ) ))
//    TaskRequest( "CDSpark.average", dataInputs )
//  }
//
//  def getMaskedSpatialAve(collection: String, varname: String, weighting: String, level_index: Int = 0, time_index: Int = 0): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List( Map("name" -> "d0", "mask" -> "#ocean50m", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> time_index, "end" -> time_index, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
//      "operation" -> List( Map( "input"->"v0", "axes"->"xy", "weights"->weighting ) ))
//    TaskRequest( "CDSpark.average", dataInputs )
//  }
//
//  def getConstant(collection: String, varname: String, level_index: Int = 0 ): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List( Map("name" -> "d0", "lev" -> Map("start" -> level_index, "end" -> level_index, "system" -> "indices"), "time" -> Map("start" -> 10, "end" -> 10, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> s"collection:/$collection", "name" -> s"$varname:v0", "domain" -> "d0")),
//      "operation" -> List( Map( "input"->"v0") ))
//    TaskRequest( "CDSpark.const", dataInputs )
//  }
//
//  def getAnomalyTest: TaskRequest = {
//    val dataInputs = Map(
//      "domain" ->  List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> 12.075, "end" -> 12.075, "system" -> "values"), "lev" -> Map("start" -> 1000, "end" -> 1000, "system" -> "values"))),
//      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggtest", "name" -> "t:v0", "domain" -> "d0")),  // collection://merra300/hourly/asm_Cp
//      "operation" -> List( Map( "input"->"v0", "axes"->"t" ) ))
//    TaskRequest( "CDSpark.anomaly", dataInputs )
//  }
//}

//abstract class SyncExecutor {
//  val printer = new scala.xml.PrettyPrinter(200, 3)
//
//  def main(args: Array[String]) {
//    val executionManager = getExecutionManager
//    val final_result = getExecutionManager.blockingExecute( getTaskRequest(args), getRunArgs )
//    println(">>>> Final Result: " + printer.format(final_result.toXml))
//  }
//
//  def getTaskRequest(args: Array[String]): TaskRequest
//  def getRunArgs = Map("async" -> "false")
//  def getExecutionManager = CDS2ExecutionManager(Map.empty)
//  def getCollection( id: String ): Collection = Collections.findCollection(id) match { case Some(collection) => collection; case None=> throw new Exception(s"Unknown Collection: $id" ) }
//}
//
//object TimeAveSliceTask extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "hur:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
//    TaskRequest("CDSpark.average", dataInputs)
//  }
//}
//
//object YearlyCycleSliceTask extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12)))
//    TaskRequest("CDSpark.bin", dataInputs)
//  }
//}
//
////object AveTimeseries extends SyncExecutor {
////  def getTaskRequest(args: Array[String]): TaskRequest = {
////    import nasa.nccs.esgf.process.DomainAxis.Type._
////    val workflows = List[WorkflowContainer](new WorkflowContainer(operations = List( OperationContext("CDSpark.average", List("v0"), Map("axis" -> "t")))))
////    val variableMap = Map[String, DataContainer]("v0" -> new DataContainer(uid = "v0", source = Some(new DataSource(name = "hur", collection = getCollection("merra/mon/atmos"), domain = "d0"))))
////    val domainMap = Map[String, DomainContainer]("d0" -> new DomainContainer(name = "d0", axes = cdsutils.flatlist(DomainAxis(Z, 1, 1), DomainAxis(Y, 100, 100), DomainAxis(X, 100, 100)), None))
////    new TaskRequest("CDSpark.average", variableMap, domainMap, workflows, Map("id" -> "v0"))
////  }
////}
//
//object CreateVTask extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
//        Map("name" -> "d1", "time" -> Map("start" -> "2010-01-16T12:00:00", "end" -> "2010-01-16T12:00:00", "system" -> "values"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDSpark.anomaly"), Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12, "name" -> "CDSpark.timeBin"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDSpark.subset")))
//    TaskRequest("CDSpark.workflow", dataInputs)
//  }
//}
//
//object YearlyCycleTask extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "period" -> 1, "unit" -> "month", "mod" -> 12)))
//    TaskRequest("CDSpark.timeBin", dataInputs)
//  }
//}
//
//object SeasonalCycleRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "time" -> Map("start" -> 0, "end" -> 36, "system" -> "indices"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "period" -> 3, "unit" -> "month", "mod" -> 4, "offset" -> 2)))
//    TaskRequest("CDSpark.timeBin", dataInputs)
//  }
//}
//
//object YearlyMeansRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "period" -> 12, "unit" -> "month")))
//    TaskRequest("CDSpark.timeBin", dataInputs)
//  }
//}
//
//object SubsetRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 45, "end" -> 45, "system" -> "values"), "lon" -> Map("start" -> 30, "end" -> 30, "system" -> "values"), "lev" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")),
//        Map("name" -> "d1", "time" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "domain" -> "d1")))
//    TaskRequest("CDSpark.subset", dataInputs)
//  }
//}
//
//object TimeSliceAnomaly extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lon" -> Map("start" -> 10, "end" -> 10, "system" -> "values"), "lev" -> Map("start" -> 8, "end" -> 8, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
//    TaskRequest("CDSpark.anomaly", dataInputs)
//  }
//}
//
//object MetadataRequest extends SyncExecutor {
//  val level = 0
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs: Map[String, Seq[Map[String, Any]]] = level match {
//      case 0 => Map()
//      case 1 => Map("variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0")))
//    }
//    TaskRequest("CDSpark.metadata", dataInputs)
//  }
//}
//
//object CacheRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra300/hourly/asm_Cp", "name" -> "t:v0", "domain" -> "d0")))
//    TaskRequest("util.cache", dataInputs)
//  }
//}
//
//object AggregateAndCacheRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggTest3", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/", "name" -> "t", "domain" -> "d0")))
//    TaskRequest("util.cache", dataInputs)
//  }
//}
//
//object AggregateRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map( "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggTest37", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/" ) ) )
//    TaskRequest("util.agg", dataInputs)
//  }
//}
//
//
//object MultiAggregateRequest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val baseCollectionId = args(0)
//    val baseDirectory = new java.io.File(args(1))
//    assert( baseDirectory.isDirectory, "Base directory is not a directory: " + args(1) )
//    val dataInputs = Map( "variable" -> baseDirectory.listFiles.map( dir => Map("uri" -> Array("collection:",baseCollectionId,dir.getName).mkString("/"), "path" -> dir.toString ) ).toSeq )
//    TaskRequest("util.agg", dataInputs)
//  }
//}
//
//object AggregateAndCacheRequest2 extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "path" -> "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY", "name" -> "t", "domain" -> "d0")))
//    TaskRequest("util.cache", dataInputs)
//  }
//}
//
//object AggregateAndCacheRequest1 extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra2/hourly/M2T1NXLND-2004-04", "path" -> "/att/pubrepo/MERRA/remote/MERRA2/M2T1NXLND.5.12.4/2004/04", "name" -> "SFMC", "domain" -> "d0")))
//    TaskRequest("util.cache", dataInputs)
//  }
//}
//
//object Max extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "xy")))
//    TaskRequest("CDSpark.max", dataInputs)
//  }
//}
//
//object Min extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lev" -> Map("start" -> 20, "end" -> 20, "system" -> "indices"), "time" -> Map("start" -> 0, "end" -> 0, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "xy")))
//    TaskRequest("CDSpark.min", dataInputs)
//  }
//}
//
//object AnomalyTest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> -7.0854263, "end" -> -7.0854263, "system" -> "values"), "lon" -> Map("start" -> 12.075, "end" -> 12.075, "system" -> "values"), "lev" -> Map("start" -> 1000, "end" -> 1000, "system" -> "values"))),
//      "variable" -> List(Map("uri" -> "collection://merra_1/hourly/aggtest", "name" -> "t:v0", "domain" -> "d0")), // collection://merra300/hourly/asm_Cp
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
//    TaskRequest("CDSpark.anomaly", dataInputs)
//  }
//}
//
//object AnomalyTest1 extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 20.0, "end" -> 20.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"))),
//      "variable" -> List(Map("uri" -> "collection://merra2/hourly/m2t1nxlnd-2004-04", "name" -> "SFMC:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
//    TaskRequest("CDSpark.anomaly", dataInputs)
//  }
//}
//object AnomalyTest2 extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d0", "lat" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "lon" -> Map("start" -> 0.0, "end" -> 0.0, "system" -> "values"), "level" -> Map("start" -> 10, "end" -> 10, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://merra/daily/aggTest", "name" -> "t:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t")))
//    TaskRequest("CDSpark.anomaly", dataInputs)
//  }
//}
//
//object AnomalyArrayTest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "collection://MERRA/mon/atmos", "name" -> "ta:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDSpark.anomaly"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDSpark.subset")))
//    TaskRequest("CDSpark.workflow", dataInputs)
//  }
//}
//
//object AnomalyArrayNcMLTest extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val dataInputs = Map(
//      "domain" -> List(Map("name" -> "d1", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices")), Map("name" -> "d0", "lat" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lon" -> Map("start" -> 3, "end" -> 3, "system" -> "indices"), "lev" -> Map("start" -> 30, "end" -> 30, "system" -> "indices"))),
//      "variable" -> List(Map("uri" -> "file://Users/tpmaxwel/data/AConaty/comp-ECMWF/ecmwf.xml", "name" -> "Temperature:v0", "domain" -> "d0")),
//      "operation" -> List(Map("input" -> "v0", "axes" -> "t", "name" -> "CDSpark.anomaly"), Map("input" -> "v0", "domain" -> "d1", "name" -> "CDSpark.subset")))
//    TaskRequest("CDSpark.workflow", dataInputs)
//  }
//}
//
////object AveArray extends SyncExecutor {
////  def getTaskRequest(args: Array[String]): TaskRequest = {
////    import nasa.nccs.esgf.process.DomainAxis.Type._
////
////    val workflows = List[WorkflowContainer](new WorkflowContainer(operations = List( OperationContext("CDSpark.average", List("v0"), Map("axis" -> "xy")))))
////    val variableMap = Map[String, DataContainer]("v0" -> new DataContainer(uid = "v0", source = Some(new DataSource(name = "t", collection = getCollection("merra/daily"), domain = "d0"))))
////    val domainMap = Map[String, DomainContainer]("d0" -> new DomainContainer(name = "d0", axes = cdsutils.flatlist(DomainAxis(Z, 0, 0)), None))
////    new TaskRequest("CDSpark.average", variableMap, domainMap, workflows, Map("id" -> "v0"))
////  }
////}
//
//object SpatialAve1 extends SyncExecutor {
//  def getTaskRequest(args: Array[String]): TaskRequest = SampleTaskRequests.getSpatialAve("/MERRA/mon/atmos", "ta", "cosine")
//}
//
//object cdscan extends App with Loggable {
//  val printer = new scala.xml.PrettyPrinter(200, 3)
//  val executionManager = CDS2ExecutionManager(Map.empty)
//  val final_result = executionManager.blockingExecute( getTaskRequest(args), Map("async" -> "false") )
//  println(">>>> Final Result: " + printer.format(final_result.toXml))
//
//  def getTaskRequest(args: Array[String]): TaskRequest = {
//    val baseCollectionId = args(0)
//    val baseDirectory = new java.io.File(args(1))
//    logger.info( s"Running cdscan with baseCollectionId $baseCollectionId and baseDirectory $baseDirectory")
//    assert( baseDirectory.isDirectory, "Base directory is not a directory: " + args(1) )
//    val dataInputs = Map( "variable" -> baseDirectory.listFiles.filter( f => Collections.hasChildNcFile(f) ).map(
//      dir => Map("uri" -> Array("collection:",baseCollectionId,dir.getName).mkString("/"), "path" -> dir.toString ) ).toSeq )
//    TaskRequest("util.agg", dataInputs)
//  }
//}
//
//
//object IntMaxTest extends App {
//  printf( " MAXINT: %.2f G, MAXLONG: %.2f G".format( Int.MaxValue/1.0E9, Long.MaxValue/1.0E9 ) )
//}


//  TaskRequest: name= CWT.average, variableMap= Map(v0 -> DataContainer { id = hur:v0, dset = merra/mon/atmos, domain = d0 }, ivar#1 -> OperationContext { id = ~ivar#1,  name = , result = ivar#1, inputs = List(v0), optargs = Map(axis -> xy) }), domainMap= Map(d0 -> DomainContainer { id = d0, axes = List(DomainAxis { id = lev, start = 0, end = 1, system = "indices", bounds =  }) })


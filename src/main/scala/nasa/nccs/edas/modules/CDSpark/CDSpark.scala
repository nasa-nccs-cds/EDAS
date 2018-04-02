package nasa.nccs.edas.modules.CDSpark

import java.io.{File, IOException}
import java.nio.file.Paths

import nasa.nccs.cdapi.cdm.OperationInput

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.cdapi.data.{FastMaskedArray, HeapFltArray}
import nasa.nccs.cdapi.data.TimeCycleSorter._
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import ucar.{ma2, nc2}
import nasa.nccs.cdapi.tensors.{CDFloatArray, CDIndexMap}
import nasa.nccs.edas.engine.EDASExecutionManager.logger
import nasa.nccs.edas.engine.{EDASExecutionManager, Workflow, WorkflowNode}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.Kernel.getResultDir
import nasa.nccs.edas.kernels._
import nasa.nccs.edas.rdd._
import nasa.nccs.esgf.process._
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.joda.time.DateTime
import ucar.nc2.constants.AxisType
import ucar.nc2.{Attribute, Dimension}
import ucar.nc2.dataset.{CoordinateAxis, NetcdfDataset}
import ucar.nc2.write.{Nc4Chunking, Nc4ChunkingStrategyNone}
import org.apache.spark.mllib.rdd.RDDFunctions._
import ucar.nc2.time.CalendarDate

import scala.collection.immutable.{Map, TreeMap}
import scala.reflect.runtime.{universe => u}

//package object CDSpark {
//  val name = "CDSpark"
//  val version = "1.0-SNAPSHOT"
//  val organization = "nasa.nccs"
//  val author = "Thomas Maxwell"
//  val contact = "thomas.maxwell@nasa.gov"
//}

class max extends SingularRDDKernel(Map("mapreduceOp" -> "max")) {
  override val status = KernelStatus.public
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = false

//  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Maximum"
  val description = "Computes maximum element value from input variable data over specified axes and roi"
  override val initValue: Float = -Float.MaxValue
}

//  class const extends SingularRDDKernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Sets Input Fragment to constant value"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val axes: AxisIndices = context.getAxisIndices (context.config ("axes", "") )
//        val async = context.config ("async", "false").toBoolean
//        val resultFragSpec = dataFrag.getReducedSpec (axes)
//        val sval = context.config ("value", "1.0")
//        val t10 = System.nanoTime
//        val result_val_masked: CDFloatArray = (dataFrag.data := sval.toFloat)
//        val t11 = System.nanoTime
//        logger.info ("Constant op, time = %.4f s, result sample = %s".format ((t11 - t10) / 1.0E9, getDataSample(result_val_masked).mkString(",").toString) )
//        DataFragment (resultFragSpec, result_val_masked)
//      } )
//    }
//  }


//class filter extends KernelImpl() {
//  override val status = KernelStatus.restricted
//  val inputs = List(WPSDataInput("input variable", 1, 1))
//  val outputs = List(WPSProcessOutput("operation result"))
//  val title = "Filter"
//  val doesAxisElimination: Boolean = false
//  val description = "Filter data by cherry-picking slices, etc."
//
//  override def map ( context: KernelContext ) ( inputs: CDTimeSlice ): CDTimeSlice = {
//    val input_array_map: Map[String,HeapFltArray] = Map( context.operation.inputs.map( id => id -> inputs.findElements(id).head ):_*)
//    val input_fastArray_map:  Map[String,FastMaskedArray] = input_array_map.mapValues(_.toFastMaskedArray)
//    val levels: String = context.config("plev","")
//    val inputId = context.operation.inputs.head
//    val (ikey,input_data) = inputs.elements.find { case (key,value) => inputId.split(':').last.equals( key.split(':').last ) }.getOrElse( throw new Exception( s"Can't find input ${inputId} in 'compress' Kernel, inputs = ${inputs.elements.keys.mkString(",")}"))
//    val t0 = System.nanoTime
//    if( !levels.isEmpty ) {
//      val levelValues = levels.split(',').map( _.toFloat )
//      val ( axisIndex: Int, levelIndices: Array[Int] ) = context.grid.coordValuesToIndices( 'z', levelValues )
//      val compressed_array_map: Map[String,FastMaskedArray] = input_fastArray_map.mapValues( _.compress( levelIndices, axisIndex ) )
//      val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value")  ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements) )
//      val elems: List[(String,HeapFltArray)] = compressed_array_map.map { case (id, carray) =>
//        context.operation.rid -> HeapFltArray( carray.toCDFloatArray, input_data.origin, result_metadata, None ) }.toList
//      logger.info("&MAP: Finished Kernel %s, inputs = %s, output = %s, time = %.4f s".format(name, context.operation.inputs.mkString(","), context.operation.rid, (System.nanoTime - t0)/1.0E9) )
//      CDTimeSlice( TreeMap(elems:_*), inputs.metadata, inputs.partition )
//    } else {
//      logger.warn( "No operation performed in compress kernel")
//      inputs
//    }
//  }
//}


class eMin extends CombineRDDsKernel(Map("mapOp" -> "min")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Minimum"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise minimum values for input variables data over specified roi"
}

class eMax extends CombineRDDsKernel(Map("mapOp" -> "max")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise maximum values for input variables data over specified roi"
}

class eSum extends CombineRDDsKernel(Map("mapOp" -> "sum")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise sums for input variables data over specified roi"
}

class eDiff extends CombineRDDsKernel(Map("mapOp" -> "subt")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Difference"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise diffs for input variables over specified roi"
}

class eMult extends CombineRDDsKernel(Map("mapOp" -> "mult")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Product"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise products for input variables data over specified roi"
}

class eDiv extends CombineRDDsKernel(Map("mapOp" -> "divide")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Division"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise divisions for input variables data over specified roi"
}

class lowpass extends KernelImpl( Map( "reduceOp" -> "avew" ) ) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Computes sliding average of time series"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "Independently removes the high frequency dynamics from the time series at each spatial point, requires 'groupBy' parm to determine resolution"
  def map ( context: KernelContext ) ( inputs: CDRecord  ): CDRecord = { inputs; }

  override def mapRDD(input: CDRecordRDD, context: KernelContext ): CDRecordRDD = {
    EDASExecutionManager.checkIfAlive
    if( context.profiler.activated ) { input.exe }
    val ( times, regroupedRDD ) =  context.profiler.profile(s"lowpass.group(${KernelContext.getProcessAddress}):${inputs.toString}")(() => {
      val groupBy = context.getGroup.getOrElse(throw new Exception("lowpass operation requires a groupBy parameter"))
      val elemFilter = (elemId: String) => context.operation.inputs.contains(elemId)
      val filteredRdd: RDD[CDRecord] = input.rdd.map(_.selectElements(elemFilter))
      val times: Array[(Long, Long)] = filteredRdd.map(rec => (rec.startTime, rec.endTime)).collect().sortBy(_._1)
      val groupedRDD: RDD[CDRecord] = CDRecordRDD.reduceRddByGroup(filteredRdd, CDRecord.weightedSum(context), "normw", groupBy).map(_._2)
      val regrouped_rdd = input.newData(groupedRDD.sortBy(_.startTime)).sliding(3, 2)
      if( context.profiler.activated ) { regrouped_rdd.cache; regrouped_rdd.count }
      (times, regrouped_rdd )
    })

    val lowpassRdd:  RDD[CDRecord] = context.profiler.profile(s"lowpass.interpolate(${KernelContext.getProcessAddress}):${inputs.toString}")(() => {
      val rename = context.operation.getRenameOp
      val lowpass_rdd = regroupedRDD.mapPartitionsWithIndex(interpolate(regroupedRDD.getNumPartitions, times)).map(_.renameElements(rename))
      if( context.profiler.activated ) { lowpass_rdd.cache; lowpass_rdd.count }
      lowpass_rdd
    })

    context.profiler.profile(s"lowpass.join(${KernelContext.getProcessAddress}):${inputs.toString}")(() => {
      val rv = new CDRecordRDD(lowpassRdd, input.metadata, input.variableRecords) join input.rdd
      if( context.profiler.activated ) { rv.exe }
      rv
    })
  }

  override def mapReduce(input: CDRecordRDD, context: KernelContext, batchIndex: Int, merge: Boolean = false ): QueryResultCollection = {
    val t0 = System.nanoTime()
    val mapresult: CDRecordRDD = context.profiler.profile("mapReduce.mapRDD") (() => { mapRDD(input, context) } )
    if( KernelContext.workflowMode == WorkflowMode.profiling ) { mapresult.exe }
    val rv = context.profiler.profile("mapReduce.reduce") ( () => { mapresult.collect } )
    logger.info(" #M# Executed mapReduce, time: %.2f, metadata = { %s }".format( (System.nanoTime-t0)/1.0E9, rv.getMetadata.mkString("; ") ))
    rv
  }

  def date( time_index: Long ): String = CalendarDate.of(time_index).toString

  def interpolate(numPartitions: Int, times: Array[(Long,Long)] )(partitionIndex: Int, segments: Iterator[Array[CDRecord]] ) : Iterator[CDRecord] = {
    val segSeq  = segments.toSeq
    val nSegments = segSeq.length
    logger.info( s"   #I# --> Partition[$partitionIndex/$numPartitions], nSegments: ${nSegments}, Times: ${times.length} " )
    val interpRecs = segSeq.sortBy( _(0).startTime ).zipWithIndex.flatMap { case ( segment, segmentIndex ) => {
      logger.info( s"   #I# --> Segment[${segmentIndex}], num elements = ${segment.length}" )
      if( segment.length == 3 ) {
        val (startRec, midRec, endRec) = ( segment(0), segment(1), segment(2) )
        val (start_time, mid_time, end_time) =
          if( ( segmentIndex == 0 ) && ( partitionIndex == 0 )  )   { (startRec.startTime,  midRec.midpoint,  endRec.midpoint ) }
          else if( ( segmentIndex == nSegments-1 ) && ( partitionIndex == numPartitions-1 )   )  {  (startRec.midpoint,   midRec.midpoint,  endRec.endTime  ) }
          else { (startRec.midpoint,   midRec.midpoint,  endRec.midpoint ) }
        val segmentTimes = times.filter { case (sliceT0, sliceT1) => { val midT = (sliceT0+sliceT1)/2;  (midT < end_time) && (midT > start_time) } }
        logger.info( s"   #I# -->  Segment Times ${date(segmentTimes.head._1)} -> ${date(segmentTimes.last._2)}, NT: ${segmentTimes.length} ")
        segmentTimes.map { case (sliceT0, sliceT1) => {
          val sliceT = (sliceT0 + sliceT1) / 2
          if( sliceT < mid_time ) {
            CDRecord.interpolate( sliceT0, sliceT1, startRec, midRec )
          } else if( sliceT > mid_time ) {
            CDRecord.interpolate( sliceT0, sliceT1, midRec, endRec )
          } else {
            midRec
          }
        }}
      } else if( partitionIndex == numPartitions-1 ) {
        val (startRec, endRec) = ( segment(0), segment(1) )
        val (start_time, end_time) =
          if( segmentIndex == nSegments-1 ) { (startRec.midpoint,  endRec.endTime  ) }
          else { (startRec.midpoint,  endRec.midpoint ) }
        val segmentTimes = times.filter { case (sliceT0, sliceT1) => (sliceT0 < end_time) && (sliceT1 > start_time) }
        logger.info( s"   #I# -->  Segment Times ${date(segmentTimes.head._1)} -> ${date(segmentTimes.last._2)}, NT: ${segmentTimes.length} ")
        segmentTimes.map { case (sliceT0, sliceT1) => CDRecord.interpolate( sliceT0, sliceT1, startRec, endRec ) }
      } else { Iterator.empty }
    }}
    logger.info( s"   #I# --> Interp Times: ${interpRecs.length} " )
    interpRecs.sortBy(_.startTime).toIterator
  }

//  def interploate( numPartitions: Int, times: Array[(Long,Long)] )( partitionIndex: Int, segments: Iterator[Array[CDRecord]] ) : Iterator[CDRecord] = {
//    val segSeq  = segments.toSeq
//    if( partitionIndex == 1 ) {
//      val x = 1
//    }
//    val nRecords = segSeq.length
//    val interpRecs = segSeq.sortBy( _(0).startTime ).zipWithIndex.flatMap { case ( segment, recordIndex ) => {
//      if( segment.length == 3 ) {
//        val (startRec, midRec, endRec) = ( segment(0), segment(1), segment(2) )
//        val (start_time, mid_time, end_time) =
//          if( ( recordIndex == 0 ) && ( partitionIndex == 0 )  )   {
//            //            println( s"<< Segment[$partitionIndex:$recordIndex]  ${date(startRec.startTime)} -> ${date(midRec.midpoint)} -> ${date(endRec.midpoint)} ")
//            (startRec.startTime,  midRec.midpoint,  endRec.midpoint ) }
//          else if( ( recordIndex == nRecords-1 ) && ( partitionIndex == numPartitions-1 )   )  {
//            //            println( s">> Segment[$partitionIndex:$recordIndex]  ${date(startRec.midpoint)} -> ${date(midRec.midpoint)} -> ${date(endRec.endTime)} ")
//            (startRec.midpoint,   midRec.midpoint,  endRec.endTime  ) }
//          else {
//            //            println( s"-- Segment[$partitionIndex:$recordIndex]  ${date(startRec.midpoint)} -> ${date(midRec.midpoint)} -> ${date(endRec.midpoint)} ")
//            (startRec.midpoint,   midRec.midpoint,  endRec.midpoint ) }
//        val segmentTimes = times.filter { case (sliceT0, sliceT1) => { val midT = (sliceT0+sliceT1)/2;  (midT < end_time) && (midT > start_time) } }
//        //        println( s"   -->  Segment Times ${date(segmentTimes.head._1)} -> ${date(segmentTimes.last._2)}, NT: ${segmentTimes.length} ")
//        segmentTimes.map { case (sliceT0, sliceT1) => {
//          val sliceT = (sliceT0 + sliceT1) / 2
//          if( sliceT < mid_time ) {
//            CDRecord.interploate( sliceT0, sliceT1, startRec, midRec )
//          } else if( sliceT > mid_time ) {
//            CDRecord.interploate( sliceT0, sliceT1, midRec, endRec )
//          } else {
//            midRec
//          }
//        }}
//      } else if( partitionIndex == numPartitions-1 ) {
//        val (startRec, endRec) = ( segment(0), segment(1) )
//        val (start_time, end_time) =
//          if( recordIndex == nRecords-1 )  {
//            //            println( s">> Segment[$partitionIndex:$recordIndex]  ${date(startRec.midpoint)}  -> ${date(endRec.endTime)} ")
//            (startRec.midpoint,  endRec.endTime  ) }
//          else {
//            //            println( s"-- Segment[$partitionIndex:$recordIndex]  ${date(startRec.midpoint)}  -> ${date(endRec.midpoint)} ")
//            (startRec.midpoint,  endRec.midpoint ) }
//        val segmentTimes = times.filter { case (sliceT0, sliceT1) => (sliceT0 < end_time) && (sliceT1 > start_time) }
//        //        println( s"   xx-->  Segment Times ${date(segmentTimes.head._1)} -> ${date(segmentTimes.last._2)}, NT: ${segmentTimes.length} ")
//        segmentTimes.map { case (sliceT0, sliceT1) => CDRecord.interploate( sliceT0, sliceT1, startRec, endRec ) }
//      } else { Iterator.empty }
//    }}
//    interpRecs.sortBy(_.startTime).toIterator
//  }
}




class write extends KernelImpl( Map.empty ) {
  override val status = KernelStatus.developmental
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Write Result data"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "Writes the result data to disk on server as a data collection"
  def map ( context: KernelContext ) ( inputs: CDRecord  ): CDRecord = { inputs; }

  override def mapRDD(input: CDRecordRDD, context: KernelContext ): CDRecordRDD = {
    EDASExecutionManager.checkIfAlive
    val resultId = context.operation.rid
    val resultName = context.operation.config("name").getOrElse( resultId )
    val resultDir = new File( getResultDir.toString + s"/$resultName" )
    resultDir.mkdirs()
    context.operation.config("groupBy") map TSGroup.getGroup match {
      case Some( groupBy ) => input.rdd.groupBy( groupBy.group ).mapValues( slices=> writeSlicesToFile( context, resultName, resultDir )( slices.toIterator ) )
      case None => input.rdd.mapPartitions( writeSlicesToFile( context, resultName, resultDir ) )
    }
    input
  }

  def writeSlicesToFile(context: KernelContext, resultName: String, resultDir: File )( slices: Iterator[CDRecord]): Iterator[String] = {
    val gridFilePath = context.grid.gridFile
    val gridDSet = NetcdfDataset.openDataset(gridFilePath)
    val chunker: Nc4Chunking = new Nc4ChunkingStrategyNone()
    var writer: nc2.NetcdfFileWriter = null
    val resultFile = ""
    var varTups: Map[ArraySpec, nc2.Variable] = Map.empty
    for (slice <- slices) {
      if (writer == null) {
        val startDate = new DateTime(slice.startTime)
        val resultFile = Paths.get(resultDir.getCanonicalPath, resultName + startDate.toString("yyyy-MM-dd:HH") + ".nc").toString
        writer = nc2.NetcdfFileWriter.createNew(nc2.NetcdfFileWriter.Version.netcdf4, resultFile, chunker)
        val varDims: List[Dimension] = gridDSet.getDimensions.toList
        val newdims = varDims.map( dim => writer.addDimension(null, dim.getShortName, dim.getLength) )
        varTups = slice.elements.map(elem => (elem._2, writer.addVariable(null, elem._1, ma2.DataType.FLOAT, newdims)))
        writer.create()
      }
      varTups.foreach { case (arraySpec, variable) => writer.write(variable, arraySpec.toCDFloatArray) }
    }
    Iterator( resultFile )
  }

//        val ( coordAxes: List[CoordinateAxis], dims: IndexedSeq[nc2.Dimension]) = {
//            val coordAxes: List[CoordinateAxis] = gridDSet.getCoordinateAxes.toList
//            val space_dims: IndexedSeq[nc2.Dimension] = gridDSet.getDimensions.toIndexedSeq
//            val gblTimeCoordAxis = targetGrid.grid.getTimeCoordinateAxis.getOrElse( throw new Exception( s"Missing Time Axis in Target Grid in saveResultToFile for result $resultId"))
//            val timeCoordAxis = gblTimeCoordAxis.section( inputSpec.roi.getRange(0) )
//            val dims0 = space_dims :+ new Dimension(timeCoordAxis.getShortName, inputSpec.roi.getRange(0).length )
//            val newdims = dims0.map( dim => {
//              val newdim = writer.addDimension(null, dim.getShortName, dim.getLength)
//              logger.info(s"Writer addDimension ${dim.getShortName} ${dim.getLength.toString}")
//              newdim
//            })
//            optGridDest = Option(gridDSet)
//            ( coordAxes :+ timeCoordAxis, newdims )
//      }
//    }

    /*
    try {
      val inputSpec: DataFragmentSpec = context.operation..requestCx.getInputSpec().getOrElse( throw new Exception( s"Missing InputSpec in saveResultToFile for result $resultId"))
      val shape: Array[Int] = dataMap.values.head.getShape
      val gridFileOpt: Option[String] = varMetadata.get( "gridspec" )
      val targetGrid = executor.getTargetGrid.getOrElse( throw new Exception( s"Missing Target Grid in saveResultToFile for result $resultId"))
      val ( coordAxes: List[CoordinateAxis], dims: IndexedSeq[nc2.Dimension]) = gridFileOpt match {
        case Some( gridFilePath ) =>
          val gridDSet = NetcdfDataset.openDataset(gridFilePath)
          val coordAxes: List[CoordinateAxis] = gridDSet.getCoordinateAxes.toList
          val space_dims: IndexedSeq[nc2.Dimension] = gridDSet.getDimensions.toIndexedSeq
          val gblTimeCoordAxis = targetGrid.grid.getTimeCoordinateAxis.getOrElse( throw new Exception( s"Missing Time Axis in Target Grid in saveResultToFile for result $resultId"))
          val timeCoordAxis = gblTimeCoordAxis.section( inputSpec.roi.getRange(0) )
          val dims0 = space_dims :+ new Dimension(timeCoordAxis.getShortName, inputSpec.roi.getRange(0).length )
          val newdims = dims0.map( dim => {
            val newdim = writer.addDimension(null, dim.getShortName, dim.getLength)
            logger.info(s"Writer addDimension ${dim.getShortName} ${dim.getLength.toString}")
            newdim
          })
          optGridDest = Option(gridDSet)
          ( coordAxes :+ timeCoordAxis, newdims )
        case None =>
          val dims: IndexedSeq[nc2.Dimension] = targetGrid.grid.axes.indices.map(idim => {
            val aname = targetGrid.grid.getAxisSpec(idim).getAxisName
            val dim = writer.addDimension(null, aname, shape(idim))
            logger.info(s"Writer addDimension ${aname} ${shape(idim).toString}")
            dim
          })
          val coordAxes: List[CoordinateAxis] = targetGrid.grid.grid.getCoordinateAxes
          ( coordAxes, dims )
      }
      val dimsMap: Map[String, nc2.Dimension] = Map(dims.map(dim => (dim.getShortName -> dim)): _*)
      val coordsMap: Map[String, Dimension] = Map( coordAxes.map(axis =>
        axis.getAxisType.getCFAxisName -> axis.getDimensions.headOption.map(
          dim => dimsMap.getOrElse( dim.getShortName, throw new Exception(s"Missing dimension: ${axis.getDimension(0).getShortName}") ))): _* ).flatMap( item => item._2.map( dim => item._1 -> dim ) )

      logger.info(" WWW Writing result %s to file '%s', vars=[%s], dims=(%s), shape=[%s], coords = [%s], roi=[%s], varMetadata={ %s }".format(
        resultId, path, dataMap.keys.mkString(","), dims.map( dim => s"${dim.getShortName}:${dim.getLength}" ).mkString(","), shape.mkString(","),
        coordAxes.map { caxis => "%s: (%s)".format(caxis.getShortName, caxis.getShape.mkString(",")) }.mkString(","), inputSpec.roi.toString, varMetadata.mkString("; ") ) )


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
        inputSpec.getRange(coordAxis.getFullName) match {
          case Some(range) =>
            val coordVar: nc2.Variable = writer.addVariable(null, coordAxis.getFullName, coordAxis.getDataType, coordAxis.getFullName)
            for (attr <- coordAxis.getAttributes) writer.addVariableAttribute(coordVar, attr)
            val newRange = dimsMap.get(coordAxis.getFullName) match {
              case None =>
                logger.info( s" Reading coord var ${coordAxis.getFullName}, range = [${range.first}:${range.last}], axis shape = [${coordAxis.getShapeAll.mkString(",")}] " )
                range
              case Some(dim) =>
                logger.info( s" Reading coord var ${coordAxis.getFullName}, dim Length =${dim.getLength}, range = [${range.first}:${range.last}], axis shape = [${coordAxis.getShapeAll.mkString(",")}] " )
                if( coordAxis.getAxisType == AxisType.Time ) { new ma2.Range( range.getName, 0, dim.getLength-1 ) }
                else if( dim.getLength == 1 ) { val center = (range.first+range.last)/2; new ma2.Range( range.getName, center, center ) }
                else { range }
              //               if ( ( dim.getLength < range.length ) || ( coordAxis.getAxisType == AxisType.Time ) ) new ma2.Range(dim.getLength) else range
            }
            val data = coordAxis.read(List(newRange))
            Some(coordVar, data)
          case None => None
        }
      }).flatten

      val varDims: Array[Dimension] = axisTypes.map( aType => coordsMap.getOrElse(aType, throw new Exception( s"Missing coordinate type ${aType} in saveResultToFile") ) )
      val variables = dataMap.map { case ( tname, maskedTensor ) =>
        val baseName  = varMetadata.getOrElse("name", varMetadata.getOrElse("longname", "result") ).replace(' ','_')
        val varname = baseName + "-" + tname
        logger.info("Creating var %s: dims = [%s], data sample = [ %s ]".format(varname, varDims.map( _.getShortName).mkString(", "), maskedTensor.getSectionArray( Math.min(10,maskedTensor.getSize.toInt) ).mkString(", ") ) )
        val variable: nc2.Variable = writer.addVariable(null, varname, ma2.DataType.FLOAT, varDims.toList )
        varMetadata map { case (key, value) => variable.addAttribute(new Attribute(key, value)) }
        variable.addAttribute(new nc2.Attribute("missing_value", maskedTensor.getInvalid))
        dsetMetadata.foreach(attr => writer.addGroupAttribute(null, attr))
        ( variable, maskedTensor )
      }

      writer.create()

      for (newCoordVar <- newCoordVars) {
        newCoordVar match {
          case (coordVar, coordData) =>
            logger.info("Writing cvar %s: shape = [%s], dataType = %s".format(coordVar.getShortName, coordData.getShape.mkString(","), coordVar.getDataType.toString))
            writer.write(coordVar, coordData)
        }
      }
      variables.foreach { case (variable, maskedTensor) => {
        logger.info(" #V# Writing var %s: var shape = [%s], data Shape = %s".format(variable.getShortName, variable.getShape.mkString(","), maskedTensor.getShape.mkString(",") ))
        writer.write(variable, maskedTensor)
      } }
      logger.info("Done writing output to file %s".format(path))
    } catch {
      case ex: IOException =>
        logger.error("*** ERROR creating file %s%n%s".format(resultFile.getAbsolutePath, ex.getMessage()));
        ex.printStackTrace(logger.writer)
        ex.printStackTrace()
        throw ex
    }
    writer.close()
    optGridDest.foreach( _.close )
    path*/
}

class norm extends KernelImpl(Map.empty) {
  override val status = KernelStatus.developmental
  val inputs = List( WPSDataInput("input variable", 2, Integer.MAX_VALUE ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Normalize"
  val doesAxisElimination: Boolean = false
  val description = "Normalize and center all cells as independent timeseries"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = true

  override def mapRDD(input: CDRecordRDD, context: KernelContext ): CDRecordRDD = {
    EDASExecutionManager.checkIfAlive
    val aveK = new ave()
    val aveRDD = aveK.mapRDD( input, context.addConfig( "axes" -> "t" ) )
//    val diffK = new eDiff()
//    diffK.mapRDD( input. aveRDD )
    aveRDD
  }

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = {
    val input_arrays: List[ArraySpec] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArraySpec]())( _ ++ _ )
    val input_fastArrays: Array[FastMaskedArray] = input_arrays.map(_.toFastMaskedArray).toArray
    CDRecord(inputs.startTime, inputs.endTime, Map.empty, inputs.metadata )
  }
}


class cor extends KernelImpl(Map.empty) {
  override val status = KernelStatus.developmental
  val inputs = List( WPSDataInput("input variable", 2, Integer.MAX_VALUE ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Correlations"
  val doesAxisElimination: Boolean = false
  val description = "Computes unique values of correlation matrix"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = true

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = {
    val input_arrays: List[ArraySpec] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArraySpec]())( _ ++ _ )
    val input_fastArrays: Array[FastMaskedArray] = input_arrays.map(_.toFastMaskedArray).toArray
    CDRecord(inputs.startTime, inputs.endTime, Map.empty, inputs.metadata )
  }
}

class eAve extends KernelImpl(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 2, Integer.MAX_VALUE ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Ensemble Mean"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes ensemble averages over inputs withing specified ROI"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = true

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = {
    val axes: String = context.config("axes","")
    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
    val input_arrays: List[ArraySpec] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[ArraySpec]())( _ ++ _ )
    val groupOpt = input_arrays.headOption.flatMap( _.optGroup )
    val input_fastArrays: Array[FastMaskedArray] = input_arrays.map(_.toFastMaskedArray).toArray
    assert( input_fastArrays.size > 1, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
    logger.info(" -----> Executing Kernel %s, inputs = %s, input shapes = [ %s ]".format(name, context.operation.inputs.mkString(","), input_arrays.map( _.shape.mkString("(",",",")")).mkString(", ") ) )
    val input_array = input_arrays.head
    val ( resultArray, weightArray ) = FastMaskedArray.weightedSum( input_fastArrays )
    val elems: Map[String,ArraySpec] = Seq(
      context.operation.rid -> ArraySpec( input_array.missing, input_array.shape, input_array.origin, resultArray.getData, groupOpt ),
      context.operation.rid + "_WEIGHTS_" -> ArraySpec( input_array.missing, input_array.shape, input_array.origin, weightArray.getData, groupOpt )
    ).toMap
    CDRecord(inputs.startTime, inputs.endTime, elems, inputs.metadata )
  }
}

class min extends SingularRDDKernel(Map("mapreduceOp" -> "min")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Minimum"
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = false
  val description = "REDUCTION OPERATION: CComputes minimum element value from input variable data over specified axes and roi"
  override val initValue: Float = Float.MaxValue

}

class sum extends SingularRDDKernel(Map("mapreduceOp" -> "sum")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Sum"
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = false
  val description = "REDUCTION OPERATION: Computes sums of element values from input variable data over specified axes and roi"
  override val initValue: Float = 0f
}

class rmSum extends SingularRDDKernel(Map("mapreduceOp" -> "sum","postOp"->"rms")) {
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Sum"
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = true
  val description = "REDUCTION OPERATION: Computes root mean sum of input variable over specified axes and roi"
}

class rms extends SingularRDDKernel( Map("mapOp" -> "sqAdd", "reduceOp" -> "sum", "postOp"->"rms" ) ) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Square"
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = true
  val description = "REDUCTION OPERATION: Computes root mean square of input variable over specified axes and roi"
}

class ave extends SingularRDDKernel( Map( "mapOp" -> "avew", "reduceOp" -> "avew", "postOp"->"normw" ) ) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val doesAxisReduction: Boolean = true
  val weighted: Boolean = true
  val description = "REDUCTION OPERATION: Computes (weighted) means of element values from input variable data over specified axes and roi"

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = context.profiler.profile(s"ave.map(${KernelContext.getProcessAddress}):${inputs.toString}")(() => {
    val axes = context.config("axes","")
    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
    val elems = context.operation.inputs.flatMap( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array: FastMaskedArray = input_data.toFastMaskedArray
        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
          input_array.weightedSum(axisIndices,Some(weights))
        } else {
          input_array.weightedSum(axisIndices,None)
        }
        List( context.operation.rid -> ArraySpec(weighted_value_sum_masked.missing, weighted_value_sum_masked.shape, input_data.origin, weighted_value_sum_masked.getData, input_data.optGroup ),
              context.operation.rid + "_WEIGHTS_" -> ArraySpec(weights_sum_masked.missing, weights_sum_masked.shape, input_data.origin, weights_sum_masked.getData, input_data.optGroup ))
      case None =>
        throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
    })
//    logger.info("T[%.2f] @P@ Executed Kernel %s map op, input = %s, time = %.4f s".format(t0, name,  id, (t1 - t0) ))
//    context.addTimestamp( "Map Op complete" )
    val rv = CDRecord(inputs.startTime, inputs.endTime, inputs.elements ++ elems, inputs.metadata )
//    logger.info("Returning result value")
    rv
  } )
}

class noOp extends KernelImpl(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "NoOperation"
  val doesAxisReduction: Boolean = false
  val weighted = false
  override val description = "Returns the input data subset to the specified domain as the result"
  override def isDisposable( input: OperationInput ): Boolean = false
  override def elemFilter(rid: String) = (elemId: String) => true

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = { inputs }

  override def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val t0 = System.nanoTime
    val result = reduce( input, context, batchIndex )
    logger.info( s" noOp execution (reduce) time = ${(System.nanoTime-t0)/1e9} ")
    result
  }
}

class subset extends noOp {
  override val title = "Space/Time Subset"
  override val description = "Extracts a subset of element values from input variable data over the specified axes and roi"
}

class anomaly extends MultiKernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "Space/Time Anomaly"
  val doesAxisReduction: Boolean = false
  val description = "MultiKernel: Computes an anomaly of the input variable data using the CDSpark ave and eDiff kernels"
  val weighted: Boolean = true

  def getExpandedOperations( workflow: Workflow, operation: OperationContext ): List[OperationContext] = {
    val opId = UID( operation.identifier.split('-').last )
    val ( ave, eDiff, aveResult ) = ( "CDSpark.ave", "CDSpark.eDiff", workflow.request.id + "aveResult" )
    val opAve  = new OperationContext( opId + ave, ave, aveResult, operation.inputs, operation.getConfiguration )
    val opDiff = new OperationContext( opId + eDiff, eDiff, operation.rid, operation.inputs ++ opAve.outputs, Map.empty[String, String] )
    List( opAve, opDiff )
  }
}

class highpass extends MultiKernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "Space/Time Anomaly"
  val doesAxisReduction: Boolean = false
  val description = "MultiKernel: Computes an anomaly of the input variable data using the CDSpark ave and eDiff kernels"
  val weighted: Boolean = true

  def getExpandedOperations( workflow: Workflow, operation: OperationContext ): List[OperationContext] = {
    val opId = UID( operation.identifier.split('-').last )
    val ( lowpass, eDiff, lowpassResult ) = ( "CDSpark.lowpass", "CDSpark.eDiff", workflow.request.id + "lowpass" )
    val opLowpass  = new OperationContext( opId + lowpass, lowpass, lowpassResult, operation.inputs, operation.getConfiguration )
    val opDiff = new OperationContext( opId + eDiff, eDiff, operation.rid, operation.inputs ++ opLowpass.outputs, Map.empty[String, String] )
    List( opLowpass, opDiff )
  }
}

class stdDev extends MultiKernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "Space/Time Standard Deviation"
  val doesAxisReduction: Boolean = false
  val description = "MultiKernel: Computes an standard deviation of the input variable data using the CDSpark anomaly and rms kernels"
  val weighted: Boolean = true

  def getExpandedOperations( workflow: Workflow, operation: OperationContext ): List[OperationContext] = {
    val opId = UID( operation.identifier.split('-').last )
    val ( anomaly, rms, anomalyResult ) = ( "CDSpark.anomaly", "CDSpark.rms", workflow.request.id + "anomalyResult" )
    val opAnomaly  = new OperationContext( opId + anomaly, anomaly, anomalyResult, operation.inputs, operation.getConfiguration )
    val opRms = new OperationContext( opId + rms, rms, operation.rid, opAnomaly.outputs, operation.getConfiguration )
    List( opAnomaly, opRms )
  }
}




//class svd extends SingularRDDKernel(Map.empty) {
//  val inputs = List(WPSDataInput("input variable", 1, 1))
//  val outputs = List(WPSProcessOutput("operation result"))
//  val title = "Space/Time Mean"
//  val doesAxisElimination: Boolean = true
//  val description = "Computes a singular value decomposition of element values assumed to be structured with one record per timestep"
//
//  override def mapRDD(input: RDD[CDTimeSlice], context: KernelContext): RDD[CDTimeSlice] = {
//    logger.info("Executing map OP for Kernel " + id + ", OP = " + context.operation.identifier)
//    val elem_index = 0
//    val vectors: RDD[Vector] = input.map { case (key, record) => record.elements.toIndexedSeq(elem_index)._2.toVector }
//    val mat: RowMatrix = new RowMatrix(vectors)
//    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20)
//    val eigenvalues: Vector = svd.s
//    val eigenVectors: Matrix = svd.V
//    input
//  }
//
//}


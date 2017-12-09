package nasa.nccs.edas.modules.CDSpark

import nasa.nccs.cdapi.cdm.VariableRecord.missingVar
import nasa.nccs.cdapi.data.{RDDRecord, _}
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt

import nasa.nccs.cdapi.tensors.{CDFloatArray, CDIndexMap}
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels._
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.cdapi.cdm.VariableRecord
import org.apache.spark
import spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.immutable.{SortedMap, TreeMap}
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
  val doesAxisElimination: Boolean = true

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


class filter extends Kernel() {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "Filter"
  val doesAxisElimination: Boolean = false
  val description = "Filter data by cherry-picking slices, etc."

  override def map ( context: KernelContext ) ( inputs: RDDRecord ): RDDRecord = {
    val input_array_map: Map[String,HeapFltArray] = Map( context.operation.inputs.map( id => id -> inputs.findElements(id).head ):_*)
    val input_fastArray_map:  Map[String,FastMaskedArray] = input_array_map.mapValues(_.toFastMaskedArray)
    val levels: String = context.config("plev","")
    val inputId = context.operation.inputs.head
    val (ikey,input_data) = inputs.elements.find { case (key,value) => inputId.split(':').last.equals( key.split(':').last ) }.getOrElse( throw new Exception( s"Can't find input ${inputId} in 'compress' Kernel, inputs = ${inputs.elements.keys.mkString(",")}"))
    val t0 = System.nanoTime
    if( !levels.isEmpty ) {
      val levelValues = levels.split(',').map( _.toFloat )
      val ( axisIndex: Int, levelIndices: Array[Int] ) = context.grid.coordValuesToIndices( 'z', levelValues )
      val compressed_array_map: Map[String,FastMaskedArray] = input_fastArray_map.mapValues( _.compress( levelIndices, axisIndex ) )
      val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value")  ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements) )
      val elems: List[(String,HeapFltArray)] = compressed_array_map.map { case (id, carray) =>
        context.operation.rid -> HeapFltArray( carray.toCDFloatArray, input_data.origin, result_metadata, None ) }.toList
      logger.info("&MAP: Finished Kernel %s, inputs = %s, output = %s, time = %.4f s".format(name, context.operation.inputs.mkString(","), context.operation.rid, (System.nanoTime - t0)/1.0E9) )
      RDDRecord( TreeMap(elems:_*), inputs.metadata, inputs.partition )
    } else {
      logger.warn( "No operation performed in compress kernel")
      inputs
    }
  }
}


class eMin extends CombineRDDsKernel(Map("mapOp" -> "min")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Minimum"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise minimum values for input variables data over specified roi"
}

class eMax extends CombineRDDsKernel(Map("mapOp" -> "max")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise maximum values for input variables data over specified roi"
}

class eSum extends CombineRDDsKernel(Map("mapOp" -> "sum")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Maximum"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise sums for input variables data over specified roi"
}

class eDiff extends CombineRDDsKernel(Map("mapOp" -> "subt")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Difference"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise diffs for input variables over specified roi"
}

class eMult extends CombineRDDsKernel(Map("mapOp" -> "mult")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Product"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise products for input variables data over specified roi"
}

class eDiv extends CombineRDDsKernel(Map("mapOp" -> "divide")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 2, 2 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Division"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes element-wise divisions for input variables data over specified roi"
}

class eAve extends Kernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 2, Integer.MAX_VALUE ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Ensemble Mean"
  val doesAxisElimination: Boolean = false
  val description = "ENSEMBLE OPERATION: Computes ensemble averages over inputs withing specified ROI"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes: String = context.config("axes","")
    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
    val input_arrays: List[HeapFltArray] = context.operation.inputs.map( id => inputs.findElements(id) ).foldLeft(List[HeapFltArray]())( _ ++ _ )
    val input_fastArrays: Array[FastMaskedArray] = input_arrays.map(_.toFastMaskedArray).toArray
    assert( input_fastArrays.size > 1, "Missing input(s) to operation " + id + ": required inputs=(%s), available inputs=(%s)".format( context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",") ) )
    val missing = input_arrays.head.getMissing()
    val inputId = context.operation.inputs.head
    val input_data = input_arrays.head
    logger.info(" -----> Executing Kernel %s, inputs = %s, input shapes = [ %s ]".format(name, context.operation.inputs.mkString(","), input_arrays.map( _.shape.mkString("(",",",")")).mkString(", ") ) )

    val ( resultArray, weightArray ) = if( addWeights(context) ) {
      val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
      FastMaskedArray.weightedSum( input_fastArrays, Some(weights), axisIndices )
    } else {
      FastMaskedArray.weightedSum( input_fastArrays, None, axisIndices )
    }
    val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value")  ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axes.toUpperCase )
    val elem = context.operation.rid -> HeapFltArray(resultArray.toCDFloatArray, input_data.origin, result_metadata, Some(weightArray.toCDFloatArray.getArrayData()))

    logger.info("&MAP: Finished Kernel %s, output = %s, time = %.4f s".format(name, context.operation.rid, (System.nanoTime - t0)/1.0E9) )
    context.addTimestamp( "Map Op complete" )
    RDDRecord( TreeMap(elem), inputs.metadata, inputs.partition )
  }
  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}

class min extends SingularRDDKernel(Map("mapreduceOp" -> "min")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Minimum"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: CComputes minimum element value from input variable data over specified axes and roi"
  override val initValue: Float = Float.MaxValue

}

class sum extends SingularRDDKernel(Map("mapreduceOp" -> "sum")) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Sum"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: Computes sums of element values from input variable data over specified axes and roi"
  override val initValue: Float = 0f
}

class rmSum extends SingularRDDKernel(Map("mapreduceOp" -> "sum","postOp"->"rms")) {
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Sum"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: Computes root mean sum of input variable over specified axes and roi"
}

class rms extends SingularRDDKernel( Map("mapOp" -> "sqAdd", "reduceOp" -> "sum", "postOp"->"rms" ) ) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variables", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Element-wise Root Mean Square"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: Computes root mean square of input variable over specified axes and roi"
}

class ave extends SingularRDDKernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: Computes (weighted) means of element values from input variable data over specified axes and roi"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.config("axes","")
    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array: FastMaskedArray = input_data.toFastMaskedArray
        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
          input_array.weightedSum(axisIndices,Some(weights))
        } else {
          input_array.weightedSum(axisIndices,None)
        }
        val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_data.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axes.toUpperCase )
        context.operation.rid -> HeapFltArray(weighted_value_sum_masked.toCDFloatArray, input_data.origin, result_metadata, Some(weights_sum_masked.toCDFloatArray.getArrayData()))
      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
    })
    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
    context.addTimestamp( "Map Op complete" )
    val rv = RDDRecord( TreeMap( elems:_*), inputs.metadata, inputs.partition )
    logger.info("Returning result value")
    rv
  }
  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}

class CDAve( val undef: Float = Float.NaN ) extends UserDefinedAggregateFunction {
  import org.apache.spark.sql.types.{ DataType, FloatType, IntegerType }
  def inputSchema: StructType = StructType(StructField("inputColumn", ArrayType(FloatType,true)) :: Nil)
  def bufferSchema: StructType = { StructType(StructField("sum", FloatType) :: StructField("count", IntegerType ) :: Nil) }
  def dataType: DataType = FloatType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = { buffer(0) = 0f; buffer(1) = 0 }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit =  {
    val inputSeq = input.getSeq[Float](0)
    for( index <- inputSeq.indices; inputVal = inputSeq(index); if inputVal != undef ) {
      buffer(0) = buffer.getFloat(0) + inputVal
      buffer(1) = buffer.getInt(1) + 1
    }
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getFloat(0) + buffer2.getFloat(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }
  def evaluate(buffer: Row): Float = buffer.getFloat(0) / buffer.getInt(1)
}

class aveSSQL extends Kernel(Map.empty) {
  override val status = KernelStatus.restricted
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val doesAxisElimination: Boolean = true
  val description = "REDUCTION OPERATION: Computes (weighted) means of element values from input variable data over specified axes and roi"

  override def execute( workflow: Workflow, input: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
    val varId: String = context.operation.inputs.head
    val ( key0, rec0 ) = input.first()
    val fltArray: HeapFltArray = rec0.element(varId).getOrElse( missingVar( rec0, varId ))
    val missing: Float = fltArray.missing.getOrElse(Float.NaN)
    workflow.sparkSession.udf.register("cdave", new CDAve(missing) )
    val rdd: RDD[VariableRecord] = input.map { case (key,rec) => VariableRecord(key,rec,varId) }
    val df = workflow.sparkSession.createDataFrame(rdd)
    df.createOrReplaceTempView("records")
    val df_ave = workflow.sparkSession.sql("SELECT cdave(data) as average FROM records")
    val result: Array[Float] = df_ave.collect().map( _.getFloat(0) )
    val outOrigin: Array[Int] = rec0.getOrigin
    val outShape: Array[Int] = outOrigin.indices.map( i => if(i==0) result.length else 1 ).toArray
    ( key0, new RDDRecord( TreeMap[String,HeapFltArray]( varId -> new HeapFltArray(outShape,outOrigin,result,Some(missing),fltArray.gridSpec) ), rec0.metadata, rec0.partition ) )
  }
  def map(context: KernelContext )( rdd: RDDRecord ): RDDRecord = { rdd }
//  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
//  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}

class subset extends Kernel(Map.empty) {
  override val status = KernelStatus.public
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Subset"
  val description = "Extracts a subset of element values from input variable data over the specified axes and roi"
  val doesAxisElimination: Boolean = false

  def map(context: KernelContext )( rdd: RDDRecord ): RDDRecord = { rdd }
}

class anomaly extends SingularRDDKernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val doesAxisElimination: Boolean = true
  val description = "Computes an anomaly of the input variable data"

  override def mapReduce(input: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
    val binAveKernel = new binAve( )
    val mapReduceResult = binAveKernel.mapReduce(input,context,0)
    binAveKernel.finalize( mapReduceResult, context )
  }

//  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
//  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )

  //  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
  //    val t0 = System.nanoTime
  //    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
  //      case Some( input_data ) =>
  //        val input_array: FastMaskedArray = input_data.toFastMaskedArray
  //        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
  //          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
  //          input_array.weightedSum(axisIndices,Some(weights))
  //        } else {
  //          input_array.weightedSum(axisIndices,None)
  //        }
  //        val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_data.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axes.toUpperCase )
  //        context.operation.rid -> HeapFltArray(weighted_value_sum_masked.toCDFloatArray, input_data.origin, result_metadata, Some(weights_sum_masked.toCDFloatArray.getArrayData()))
  //      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
  //    })
  //    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
  //    context.addTimestamp( "Map Op complete" )
  //    val rv = RDDRecord( TreeMap( elems:_*), inputs.metadata )
  //    logger.info("Returning result value")
  //    rv
  //  }
  //  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
  //  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}


class binAve extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Binning"
  override val description = "Aggregates data into bins using specified reduce function and binning specifications"
  val doesAxisElimination: Boolean = false
  object BinKeyUtils {
    implicit object BinKeyOrdering extends Ordering[String] {
      def compare( k1: String, k2: String ) = k1.split('.').last.toInt - k2.split('.').last.toInt
    }
  }

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axes = context.config("axes","")
    val binParm = context.config( "bin", "month" )
    val startIndex = inputs.metadata.getOrElse("startIndex","0").toInt
    val elems = context.operation.inputs.flatMap( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array: FastMaskedArray = input_data.toFastMaskedArray
        val sorter = getSorter( input_data, context, startIndex )
        val result_arrays: (IndexedSeq[FastMaskedArray], IndexedSeq[FastMaskedArray]) = if( addWeights(context) ) {
          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
          input_array.weightedSumBin(sorter, Some(weights) )
        } else {
          input_array.weightedSumBin(sorter, None)
        }
        val NBins = result_arrays._1.length
        val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_data.metadata ++ List("uid" -> context.operation.rid, "bin" -> binParm, "gridfile" -> getCombinedGridfile(inputs.elements), "NBins" -> NBins.toString, "varAxis" -> sorter.getVaryingAxis.toString, "cycle" ->  context.config("cycle", "" ), "axes" -> axes.toUpperCase )
        result_arrays._1.indices.map( index => context.operation.rid + "." + index ->
          HeapFltArray( result_arrays._1(index).toCDFloatArray, input_data.origin, result_metadata, Some(result_arrays._2(index).toFloatArray) )
        )
      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
    })
    context.addTimestamp( "Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9), true )
    RDDRecord( TreeMap(elems:_*), inputs.metadata, inputs.partition )
  }
  override def combineRDD(context: KernelContext)( a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)( a0, a1 )
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )

  def getSorter( input_data: HeapFltArray, context: KernelContext, startIndex: Int  ): BinSorter =
    context.config("cycle", "" ) match {
      case x if !x.isEmpty  =>
        val cycle = context.config("cycle", "hour" )
        val bin = context.config("bin", "month" )
        new TimeCycleSorter( input_data, cycle, bin, startIndex )
      case x  =>
        val axes = context.config("axes", "" )
        new AnomalySorter( input_data, axes, context.grid, startIndex )
    }

  def getOp(context: KernelContext): ReduceOpFlt = {
    if ( mapCombineOp.isDefined ) { mapCombineOp.get }
    else {
      context.config("mapOp").fold (context.config("mapreduceOp")) (Some(_)) map ( CDFloatArray.getOp(_) ) getOrElse( throw new Exception( "Undefined Op in bin kernel" ))
    }
  }
}



class bin extends Kernel(Map.empty) {
  object BinKeyUtils {
    implicit object BinKeyOrdering extends Ordering[String] {
      def compare( k1: String, k2: String ) = k1.split('.').last.toInt - k2.split('.').last.toInt
    }
  }
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Binning"
  val doesAxisElimination: Boolean = false
  override val description = "Aggregates data into bins using specified reduce function and binning specifications"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    import BinKeyUtils.BinKeyOrdering
    val t0 = System.nanoTime
    val axes = context.config("axes","")
    val startIndex = inputs.metadata.getOrElse("startIndex","0").toInt
    val elems = context.operation.inputs.flatMap( inputId => inputs.element(inputId) match {
      case Some( input_data ) =>
        val input_array: FastMaskedArray = input_data.toFastMaskedArray
        val sorter = getSorter( input_data, context, startIndex )
        val result_arrays: IndexedSeq[FastMaskedArray] = input_array.bin( sorter, getOp(context), initValue )
        val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_data.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axes.toUpperCase )
        result_arrays.indices.map( index => context.operation.rid + "." + index ->
          HeapFltArray( result_arrays(index).toCDFloatArray, input_data.origin, result_metadata, None )
        )
      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
    })
    context.addTimestamp( "Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9), true )
    RDDRecord( TreeMap( elems:_*), inputs.metadata ++ List( "rid" -> context.operation.rid ), inputs.partition )
  }
  override def combineRDD(context: KernelContext)( a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)( a0, a1 )
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )

  def getSorter( input_data: HeapFltArray, context: KernelContext, startIndex: Int  ): BinSorter = {
    val cycle = context.config( "cycle", "hour" )
    val bin = context.config( "bin", "month" )
    new TimeCycleSorter( input_data, cycle, bin, startIndex )
  }

  def getOp(context: KernelContext): ReduceOpFlt = {
    if ( mapCombineOp.isDefined ) { mapCombineOp.get }
    else {
      context.config("mapOp").fold (context.config("mapreduceOp")) (Some(_)) map ( CDFloatArray.getOp ) getOrElse( throw new Exception( "Undefined Op in bin kernel" ))
    }
  }
}

class noOp extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "NoOperation"
  val doesAxisElimination: Boolean = false
  override val description = "Returns the input data subset to the specified domain as the result"

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val elems = context.operation.inputs.flatMap( inputId => inputs.element(inputId).map( array => inputId -> array ) )
    RDDRecord( TreeMap(elems:_*), inputs.metadata, inputs.partition )
  }
}
//class binAve extends SingularRDDKernel(Map.empty) {
//  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
//  val outputs = List( WPSProcessOutput( "operation result" ) )
//  val title = "Space/Time Mean"
//  val description = "Computes (weighted) means of element values in specified bins (e.g. day, month, year) from input variable data over specified axes and roi "
//
//  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
//    val t0 = System.nanoTime
//    val axes = context.config("axes","")
//    val axisIndices: Array[Int] = context.grid.getAxisIndices( axes ).getAxes.toArray
//    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
//      case Some( input_data ) =>
//        val input_array: FastMaskedArray = input_data.toFastMaskedArray
//        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
//          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
//          input_array.weightedSum(axisIndices,Some(weights))
//        } else {
//          input_array.weightedSum(axisIndices,None)
//        }
//        context.operation.rid -> HeapFltArray(weighted_value_sum_masked.toCDFloatArray, input_data.origin, arrayMdata(inputs, "value"), Some(weights_sum_masked.toCDFloatArray.getArrayData()))
//      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
//    })
//    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
//    context.addTimestamp( "Map Op complete" )
//    val rv = RDDRecord( Map( elems:_*), inputs.metadata ++ List( "rid" -> context.operation.rid, "axes" -> axes.toUpperCase ) )
//    logger.info("Returning result value")
//    rv
//  }
//  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
//  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
//}

class svd extends SingularRDDKernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Space/Time Mean"
  val doesAxisElimination: Boolean = true
  val description = "Computes a singular value decomposition of element values assumed to be structured with one record per timestep"

  override def mapRDD(input: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = {
    logger.info( "Executing map OP for Kernel " + id + ", OP = " + context.operation.identifier )
    val elem_index = 0
    val vectors: RDD[Vector] = input.map { case (key,record) => record.elements.toIndexedSeq(elem_index)._2.toVector }
    val mat: RowMatrix = new RowMatrix(vectors)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(20)
    val eigenvalues: Vector = svd.s
    val eigenVectors: Matrix = svd.V
    input
  }

//  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
//    val t0 = System.nanoTime
//    val elems = context.operation.inputs.map( inputId => inputs.element(inputId) match {
//      case Some( input_data ) =>
//        val input_array: FastMaskedArray = input_data.toFastMaskedArray
//        val (weighted_value_sum_masked, weights_sum_masked) =  if( addWeights(context) ) {
//          val weights: FastMaskedArray = FastMaskedArray(KernelUtilities.getWeights(inputId, context))
//          input_array.weightedSum(axisIndices,Some(weights))
//        } else {
//          input_array.weightedSum(axisIndices,None)
//        }
//        val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_data.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axes.toUpperCase )
//        context.operation.rid -> HeapFltArray(weighted_value_sum_masked.toCDFloatArray, input_data.origin, result_metadata, Some(weights_sum_masked.toCDFloatArray.getArrayData()))
//      case None => throw new Exception("Missing input to 'average' kernel: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(","))
//    })
//    logger.info("Executed Kernel %s map op, input = %s, time = %.4f s".format(name,  id, (System.nanoTime - t0) / 1.0E9))
//    context.addTimestamp( "Map Op complete" )
//    val rv = RDDRecord( TreeMap( elems:_*), inputs.metadata )
//    logger.info("Returning result value")
//    rv
//  }
//  override def combineRDD(context: KernelContext)(a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)(a0, a1)
//  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}


class timeBin extends Kernel(Map.empty) {
  val inputs = List( WPSDataInput("input variable", 1, 1 ) )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = "Time Binning"
  override val description = "Aggregates data into bins over time using specified reduce function and binning specifications"
  val doesAxisElimination: Boolean = false

  override def map ( context: KernelContext ) (inputs: RDDRecord  ): RDDRecord = {
    val t0 = System.nanoTime
    val axesStr = context.config("axes","")
    val axes: AxisIndices = context.grid.getAxisIndices( axesStr )
    val period = context.config("period", "1" ).toInt
    val mod = context.config("mod", "12" ).toInt
    val unit = context.config("unit", "month" )
    val offset = context.config("offset", "0" ).toInt
    val ( id, input_array ) = inputs.head
    val accumulation_index: CDIndexMap = input_array.toCDFloatArray.getIndex.getAccumulator( axes.args, List( getMontlyBinMap( id, context ) )  )  // TODO: Check range of getMontlyBinMap- subset by part?
    val (weighted_value_sum_masked, weights_sum_masked) = input_array.toCDFloatArray.weightedReduce( CDFloatArray.getOp("add"), 0f, accumulation_index )
    val result_metadata = inputs.metadata ++ arrayMdata(inputs, "value") ++ input_array.metadata ++ List("uid" -> context.operation.rid, "gridfile" -> getCombinedGridfile(inputs.elements), "axes" -> axesStr.toUpperCase )
    val result_array = HeapFltArray( weighted_value_sum_masked, input_array.origin, result_metadata, Some( weights_sum_masked.getArrayData() ) )
    logger.info("Executed Kernel %s map op, input = %s, index=%s, time = %.4f s".format(name, id, result_array.toCDFloatArray.getIndex.toString , (System.nanoTime - t0) / 1.0E9))
    context.addTimestamp( "Map Op complete" )
    RDDRecord( TreeMap( context.operation.rid -> result_array ), inputs.metadata, inputs.partition )
  }
  override def combineRDD(context: KernelContext)( a0: RDDRecord, a1: RDDRecord ): RDDRecord =  weightedValueSumRDDCombiner(context)( a0, a1 )
  override def postRDDOp(pre_result: RDDRecord, context: KernelContext ):  RDDRecord = weightedValueSumRDDPostOp( pre_result, context )
}


//  class anomaly extends SingularRDDKernel {
//    val inputs = List(Port("input fragment", "1"))
//    val outputs = List(Port("result", "1"))
//    override val description = "Anomaly over Input Fragment"
//
//    override def map( partIndex: Int, inputs: List[Option[DataFragment]], context: OperationContext ): Option[DataFragment] = {
//      inputs.head.map( dataFrag => {
//        val async = context.config("async", "false").toBoolean
//        val axes: AxisIndices = context.getAxisIndices(context.config("axes", ""))
//        val resultFragSpec = dataFrag.getReducedSpec(axes)
//        val t10 = System.nanoTime
//        val weighting_type = context.config("weights", if (context.config("axis", "").contains('y')) "cosine" else "")
//        val weightsOpt: Option[CDFloatArray] = weighting_type match {
//          case "" => None
//          case wtype => context.targetGrid.getAxisData( 'y', dataFrag.spec.roi ).map(axis_data => dataFrag.data.computeWeights(wtype, Map('y' -> axis_data)))
//        }
//        val anomaly_result: CDFloatArray = dataFrag.data.anomaly(axes.args, weightsOpt)
//        logger.info( "Partition[%d], generated anomaly result: %s".format(partIn
// dex, anomaly_result.toDataString ) )
//        val t11 = System.nanoTime
//        DataFragment(resultFragSpec, anomaly_result)
//      } )
//    }
//  }


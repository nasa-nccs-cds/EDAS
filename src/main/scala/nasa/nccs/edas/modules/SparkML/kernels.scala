package nasa.nccs.edas.modules.SparkML
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelImpl, KernelStatus}
import nasa.nccs.edas.rdd.{ArraySpec, CDRecord, CDRecordRDD, QueryResultCollection}
import nasa.nccs.edas.sources.netcdf.{CDTimeSliceConverter, CDTimeSlicesConverter, EDASOptions, RDDSimpleRecordsConverter}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.mllib.linalg.{Matrix, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

import scala.collection.immutable.Map

class svd extends KernelImpl {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SVD"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "Implement Singular Value Decomposition"

  override def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val t0 = System.nanoTime()
    val elem_id = context.operation.inputs.head
    val inputVectors = input.toVectorRDD( Seq( elem_id ) ): RDD[Vector]
    val topSlice: CDRecord = input.rdd.first
    val topElem = topSlice.elements.head._2
    val scaler = new StandardScaler( withMean = true, withStd = true ).fit( inputVectors )
    val scaling_result: RDD[Vector] = inputVectors.map( scaler.transform )
    logger.info( s"  ##### @SVD Input Vector Size: ${topElem.shape.mkString(", ")}, Num Input Vectors: ${inputVectors.count}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.mean.size} means: ${scaler.mean.toArray.slice(0,32).mkString(", ")}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.std.size} stDevs: ${scaler.std.toArray.slice(0,32).mkString(", ")}" )
    scaling_result.cache()
    val matrix = new RowMatrix( scaling_result )
    val nModes: Int = context.operation.getConfParm("modes").fold( 9 )( _.toInt )
    val svd = matrix.computeSVD( nModes, true )
    val lambdas = svd.s.toArray.mkString(",")
    val Uelems: Seq[(String, ArraySpec)] = CDRecord.rowMatrixCols2Arrays( svd.U ).zipWithIndex.map { case (udata, index) =>
      s"U$index" -> new ArraySpec(topElem.missing, Array(udata.length,1,1), topElem.origin, udata, topElem.optGroup )
    }
    val Velems: Seq[(String, ArraySpec)] = CDRecord.matrixCols2Arrays( svd.V ).zipWithIndex map { case (array, index) =>
      s"V$index" -> new ArraySpec(topElem.missing, topElem.shape, topElem.origin, array, topElem.optGroup)
    }
    val slice: CDRecord = new CDRecord( topSlice.startTime, topSlice.endTime, (Uelems ++ Velems).toMap, topSlice.metadata )
    logger.info( s"@SVD Created modes, nModes = ${Velems.length}, time = ${(System.nanoTime - t0) / 1.0E9}" )
    new QueryResultCollection( Array( slice ), input.metadata + ("lambdas" -> lambdas) )
  }

  def execute2(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[Row] = input.rdd.mapPartitions( iter => new CDTimeSlicesConverter( iter, options ) )
    val df: DataFrame = workflow.executionMgr.serverContext.spark.session.createDataFrame( rowRdd, CDTimeSliceConverter.defaultSchema )
    df.show( 3 )
    val avgCol = avg(col("value"))
    logger.info( "Computing ave" )
    df.select( avgCol.alias("Average") ).show(3)
    logger.info( "Finished computing ave" )
    QueryResultCollection.empty
  }

  def execute1(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[java.lang.Float] = input.rdd.mapPartitions( iter => new RDDSimpleRecordsConverter( iter, options ) )
    val df: Dataset[java.lang.Float] = workflow.executionMgr.serverContext.spark.session.createDataset( rowRdd )( Encoders.FLOAT )
    df.show( 3 )
    val avgCol = avg(col("value"))
    logger.info( "Computing ave" )
    df.select( avgCol.alias("Average") ).show(3)
    logger.info( "Finished computing ave" )
    QueryResultCollection.empty
  }

  def map(context: KernelContext )( rdd: CDRecord ): CDRecord = { rdd }   // Not used-> bypassed

}

class rescale extends KernelImpl {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SVD"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = false
  val description = "Implement Singular Value Decomposition"

  override def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val elem_id = context.operation.inputs.head
    val inputVectors = input.toVectorRDD( Seq( elem_id ) ): RDD[Vector]
    val topSlice: CDRecord = input.rdd.first
    val topElem = topSlice.elements.head._2
    val scaler = new StandardScaler( withMean = true, withStd = true ).fit( inputVectors )
    val scaling_result: RDD[Vector] = inputVectors.map( scaler.transform )
    logger.info( s"  ##### @SVD Input Vector Size: ${topElem.shape.mkString(", ")}, Num Input Vectors: ${inputVectors.count}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.mean.size} means: ${scaler.mean.toArray.slice(0,32).mkString(", ")}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.std.size} stDevs: ${scaler.std.toArray.slice(0,32).mkString(", ")}" )
    val results: RDD[CDRecord] = scaling_result.zip( input.rdd ).map { case ( vec, rec ) =>
      val headElem = rec.elements.values.head
      val elem = new ArraySpec( headElem.missing, headElem.shape, headElem.origin, vec.toArray.map(_.toFloat), headElem.optGroup )
      new CDRecord( rec.startTime, rec.endTime, Map( elem_id -> elem ), rec.metadata )
    }
    val rv = new QueryResultCollection( results.collect, input.metadata )
    val top_array = rv.records.head.elements.head._2
    logger.info( s"  ##### @SVD Rescale result with ${rv.records.length}, sliece elem shape: ${top_array.shape.mkString(", ")}, values: ${top_array.data.slice(0,32).mkString(", ")}" )
    rv
  }

  def map(context: KernelContext )( rdd: CDRecord ): CDRecord = { rdd }   // Not used-> bypassed

}


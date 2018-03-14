package nasa.nccs.edas.modules.SparkML
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelImpl, KernelStatus}
import nasa.nccs.edas.rdd.{ArraySpec, CDTimeSlice, TimeSliceCollection, TimeSliceRDD}
import nasa.nccs.edas.sources.netcdf.{CDTimeSliceConverter, CDTimeSlicesConverter, EDASOptions, RDDSimpleRecordsConverter}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
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

  override def execute(workflow: Workflow, input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    val matrix = input.toMatrix( context.operation.inputs )
    val nModes: Int = context.operation.getConfParm("modes").fold( 10 )( _.toInt )
    val topSlice: CDTimeSlice = input.rdd.first
    val topElem = topSlice.elements.head._2
    val svd = matrix.computeSVD( nModes, true )
    val ( ushape, udata ) = CDTimeSlice.rowMatrix2Array( svd.U )
    val ( vshape, vdata ) = CDTimeSlice.matrix2Array( svd.V )
    val uArray: ArraySpec  = new ArraySpec( topElem.missing, ushape, topElem.origin, udata, topElem.optGroup )
    val vArray: ArraySpec  = new ArraySpec( topElem.missing, vshape, topElem.origin, vdata, topElem.optGroup )
    val elements: Map[String, ArraySpec] = Map( "U" -> uArray, "V" ->vArray )
    val slice: CDTimeSlice = new CDTimeSlice( topSlice.startTime, topSlice.endTime, elements, topSlice.metadata )
    new TimeSliceCollection( Array( slice ), input.metadata )
  }

  def execute2(workflow: Workflow, input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[Row] = input.rdd.mapPartitions( iter => new CDTimeSlicesConverter( iter, options ) )
    val df: DataFrame = workflow.executionMgr.serverContext.spark.session.createDataFrame( rowRdd, CDTimeSliceConverter.defaultSchema )
    df.show( 3 )
    val avgCol = avg(col("value"))
    logger.info( "Computing ave" )
    df.select( avgCol.alias("Average") ).show(3)
    logger.info( "Finished computing ave" )
    TimeSliceCollection.empty
  }

  def execute1( workflow: Workflow, input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[java.lang.Float] = input.rdd.mapPartitions( iter => new RDDSimpleRecordsConverter( iter, options ) )
    val df: Dataset[java.lang.Float] = workflow.executionMgr.serverContext.spark.session.createDataset( rowRdd )( Encoders.FLOAT )
    df.show( 3 )
    val avgCol = avg(col("value"))
    logger.info( "Computing ave" )
    df.select( avgCol.alias("Average") ).show(3)
    logger.info( "Finished computing ave" )
    TimeSliceCollection.empty
  }

  def map(context: KernelContext )( rdd: CDTimeSlice ): CDTimeSlice = { rdd }   // Not used-> bypassed

}

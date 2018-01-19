package nasa.nccs.edas.kernels.sql

import nasa.nccs.edas.engine.{EDASExecutionManager, Workflow}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelStatus}
import nasa.nccs.edas.rdd.{CDTimeSlice, TimeSliceCollection, TimeSliceRDD}
import nasa.nccs.edas.sources.netcdf.{EDASOptions, RDDSimpleRecordsConverter}
import nasa.nccs.edas.utilities.runtime
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Column, Dataset, Encoders}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

class SQLKernel extends Kernel {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SQLKernel"
  val doesAxisElimination: Boolean = false
  val description = "Implement SparkSQL operations"

  override def execute(workflow: Workflow, input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[java.lang.Float] = input.rdd.mapPartitions( iter => new RDDSimpleRecordsConverter( iter, options ) )
    val dataset: Dataset[java.lang.Float] = workflow.executionMgr.serverContext.spark.session.createDataset( rowRdd )(Encoders.FLOAT)
    val aveCol: Column = avg( dataset.col("value") )
    logger.info( "Computed ave" )
    TimeSliceCollection.empty
  }

  def map(context: KernelContext )( rdd: CDTimeSlice ): CDTimeSlice = { rdd }   // Not used-> bypassed

}

//class DFKernel extends Kernel {
//  override val status = KernelStatus.restricted
//  val inputs = List(WPSDataInput("input variable", 1, 1))
//  val outputs = List(WPSProcessOutput("operation result"))
//  val title = "SQLKernel"
//  val doesAxisElimination: Boolean = false
//  val description = "Implement SparkSQL operations"
//
//  override def execute(workflow: Workflow, input: RDD[CDTimeSlice], context: KernelContext, batchIndex: Int ): CDTimeSlice = {
//    val options: EDASOptions = new EDASOptions( Array.empty )
//    val rowRdd: RDD[java.lang.Float] = input.mapPartitions( iter => new RDDSimpleRecordsConverter( iter, options ) )
//    val dataset: Dataset[java.lang.Float] = workflow.executionMgr.serverContext.spark.session.createDataset( rowRdd )(Encoders.FLOAT)
//    val aveCol: Column = avg( dataset.col("value") )
//    logger.info( "Computed ave" )
//    ( RecordKey.empty, CDTimeSlice.empty )
//  }
//
//  def map(context: KernelContext )( rdd: CDTimeSlice ): CDTimeSlice = { rdd }   // Not used-> bypassed
//
//}
//

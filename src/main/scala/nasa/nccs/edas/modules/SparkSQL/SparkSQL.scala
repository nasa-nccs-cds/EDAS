package nasa.nccs.edas.modules.SparkSQL
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelImpl, KernelStatus}
import nasa.nccs.edas.rdd.{CDRecord, QueryResultCollection, CDRecordRDD}
import nasa.nccs.edas.sources.netcdf.{CDTimeSliceConverter, CDTimeSlicesConverter, EDASOptions, RDDSimpleRecordsConverter}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}

class ave extends KernelImpl {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SQLKernel"
  val doesAxisReduction: Boolean = false
  val weighted: Boolean = true
  val description = "Implement SparkSQL operations"

  override def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
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

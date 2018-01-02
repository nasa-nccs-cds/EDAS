package nasa.nccs.edas.modules.SparkSQL
import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelStatus}
import nasa.nccs.edas.loaders.{EDASOptions, RDDRecordConverter, RDDRecordsConverter}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row}

class ave extends Kernel {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SQLKernel"
  val doesAxisElimination: Boolean = false
  val description = "Implement SparkSQL operations"

  override def execute( workflow: Workflow, input: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[Row] = input.mapPartitions( iter => new RDDRecordsConverter( iter, options ) )
    val df: DataFrame = workflow.executionMgr.serverContext.spark.session.createDataFrame( rowRdd, RDDRecordConverter.defaultSchema )
    val avgCol = avg(col("value"))
    logger.info( "Computing ave" )
    df.select( avgCol.alias("Average") ).show(1)
    logger.info( "Finished computing ave" )
    ( RecordKey.empty, RDDRecord.empty )
  }

  def map(context: KernelContext )( rdd: RDDRecord ): RDDRecord = { rdd }   // Not used-> bypassed

}

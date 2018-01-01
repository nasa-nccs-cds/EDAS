package nasa.nccs.edas.modules.SparkSQL

import java.lang.Float

import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.{Kernel, KernelContext, KernelStatus}
import nasa.nccs.edas.loaders.{EDASOptions, RDDSimpleRecordsConverter}
import nasa.nccs.wps.{WPSDataInput, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{Column, Dataset, Encoders}

class ave extends Kernel {
  override val status = KernelStatus.restricted
  val inputs = List(WPSDataInput("input variable", 1, 1))
  val outputs = List(WPSProcessOutput("operation result"))
  val title = "SQLKernel"
  val doesAxisElimination: Boolean = false
  val description = "Implement SparkSQL operations"

  override def execute( workflow: Workflow, input: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
    val options: EDASOptions = new EDASOptions( Array.empty )
    val rowRdd: RDD[Float] = input.mapPartitions( iter => new RDDSimpleRecordsConverter( iter, options ) )
    val dataset: Dataset[Float] = workflow.executionMgr.serverContext.spark.session.createDataset( rowRdd )(Encoders.FLOAT)
    val aveCol: Column = avg( dataset.col("value") )
    logger.info( "Computed ave" )
    ( RecordKey.empty, RDDRecord.empty )
  }

  def map(context: KernelContext )( rdd: RDDRecord ): RDDRecord = { rdd }   // Not used-> bypassed

}

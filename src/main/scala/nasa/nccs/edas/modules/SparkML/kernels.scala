package nasa.nccs.edas.modules.SparkML
import java.util

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
  def map(context: KernelContext )( rdd: CDRecord ): CDRecord = { rdd }   // Not used-> bypassed
  def getSelectedElemIds( rec: CDRecord, context: KernelContext ): Array[String] = rec.elements.keys.filter ( key => context.operation.inputs.exists( elem => key.split(':').last.endsWith( elem ) ) ).map(_.split('-').head).toArray

  override def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = {
    val t0 = System.nanoTime()
    val inputVectors: RDD[Vector] = input.toVectorRDD( context.operation.inputs.toSeq )
    val topSlice: CDRecord = input.rdd.first
    val startTime = topSlice.startTime
    val endTime = input.rdd.aggregate( -Long.MaxValue )( (etime,rec) => math.max( etime, rec.endTime), math.max )
    val topElem = topSlice.elements.head._2
    val elemIds: Array[String] = getSelectedElemIds( topSlice, context )
    val nElems = elemIds.size
    val doNorm: Boolean = context.operation.getConfParm("norm").fold( true )( _.toBoolean )
    val scaling_result: RDD[Vector] = if( doNorm ) {
      val scaler = new StandardScaler(withMean = true, withStd = true).fit(inputVectors)
      logger.info( s"  ##### @SVD Rescale inputs with ${scaler.mean.size} means: ${scaler.mean.toArray.slice(0,32).mkString(", ")}" )
      logger.info( s"  ##### @SVD Rescale inputs with ${scaler.std.size} stDevs: ${scaler.std.toArray.slice(0,32).mkString(", ")}" )
      inputVectors.map(scaler.transform)
    } else { inputVectors }

    logger.info( s"  ##### @SVD Input Vector Size: ${topElem.shape.mkString(", ")}, Num Input Vectors: ${inputVectors.count}, Num Input Slices: ${input.rdd.count}, Num input elems: $nElems" )
    scaling_result.cache()
    val matrix = new RowMatrix( scaling_result )
    val nModes: Int = context.operation.getConfParm("modes").fold( 9 )( _.toInt )
    val computeU: Boolean = context.operation.getConfParm("compu").fold( true )( _.toBoolean )
    val svd = matrix.computeSVD( nModes, true )
    val eigens = svd.s.toArray.map( l => (l*l)/(nElems-1) )
    val norm = eigens.foldLeft(0.0)( ( l2sum, l2 ) => l2sum + l2 )
    val PVEs = eigens.map( l2 => (l2*100)/norm )
    val array_size = topElem.shape.product
    val Velems: Seq[(String, ArraySpec)] = CDRecord.matrixCols2Arrays( svd.V ).zipWithIndex flatMap { case (array, index) =>
      logger.info( s"@SVD Creating V$index Array, data size = ${array.length}, array size = ${array_size}, input shape= [ ${topElem.shape.mkString(", ")} ]")
      for( iArray <- 0 until nElems; start = array_size*iArray ) yield {
        val subArray = util.Arrays.copyOfRange(array, start, start + array_size )
        s"V-${elemIds(iArray)}-$index" -> new ArraySpec(topElem.missing, topElem.shape, topElem.origin, subArray, topElem.optGroup)
      }
    }
    val elems = if( computeU ) {
      val Uelems: Seq[(String, ArraySpec)] = CDRecord.rowMatrixCols2Arrays( svd.U ).zipWithIndex.map { case (udata, index) =>
        val shape = if( topElem.shape.length == 4 ) { Array(udata.length,1,1,1) } else { Array(udata.length,1,1) }
        val PVE = "%.1f".format( PVEs(index) )
        s"U-${index}_PVE=$PVE" -> new ArraySpec(topElem.missing, shape, topElem.origin, udata, topElem.optGroup )
      }
      (Uelems ++ Velems).toMap
    } else { Velems.toMap }
    val slice: CDRecord = new CDRecord( startTime, endTime, elems, topSlice.metadata )
    logger.info( s"@SVD Created modes, nModes = ${Velems.length}, time = ${(System.nanoTime - t0) / 1.0E9}" )
    new QueryResultCollection( Array( slice ), input.metadata )
  }
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
    val inputVectors = input.toVectorRDD( context.operation.inputs.toSeq ): RDD[Vector]
    val topSlice: CDRecord = input.rdd.first
    val topElem = topSlice.elements.head._2
    val rid = context.operation.inputs.mkString("-")
    val scaler = new StandardScaler( withMean = true, withStd = true ).fit( inputVectors )
    val scaling_result: RDD[Vector] = inputVectors.map( scaler.transform )
    logger.info( s"  ##### @SVD Input Vector Size: ${topElem.shape.mkString(", ")}, Num Input Vectors: ${inputVectors.count}, Num input elems: ${topSlice.elements.size}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.mean.size} means: ${scaler.mean.toArray.slice(0,32).mkString(", ")}" )
    logger.info( s"  ##### @SVD Rescale inputs with ${scaler.std.size} stDevs: ${scaler.std.toArray.slice(0,32).mkString(", ")}" )
    val results: RDD[CDRecord] = scaling_result.zip( input.rdd ).map { case ( vec, rec ) =>
      val headElem = rec.elements.values.head
      val elem = new ArraySpec( headElem.missing, headElem.shape, headElem.origin, vec.toArray.map(_.toFloat), headElem.optGroup )
      new CDRecord( rec.startTime, rec.endTime, Map( rid -> elem ), rec.metadata )
    }
    val rv = new QueryResultCollection( results.collect, input.metadata )
    val top_array = rv.records.head.elements.head._2
    logger.info( s"  ##### @SVD Rescale result with ${rv.records.length}, sliece elem shape: ${top_array.shape.mkString(", ")}, values: ${top_array.data.slice(0,32).mkString(", ")}" )
    rv
  }

  def map(context: KernelContext )( rdd: CDRecord ): CDRecord = { rdd }   // Not used-> bypassed

}


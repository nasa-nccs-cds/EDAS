package nasa.nccs.sparkSql
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.cdapi.cdm.{NetcdfDatasetMgr, VariableRecord}
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.utilities.Loggable
import org.apache.spark
import org.apache.spark.SparkConf
import spark.rdd.RDD
import ucar.ma2
import org.apache.spark.sql.expressions.{ UserDefinedAggregateFunction, MutableAggregationBuffer }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, Row}
import org.apache.spark.sql.expressions.Aggregator

class SparkDatasetMgr( val dataPath: String ) {

  def getSections( varName: String, partSize: Int = 1, roiOpt: Option[ma2.Section] = None ): Seq[ma2.Section] = {
    val dataset = NetcdfDatasetMgr.openCollection( varName, dataPath )
    val variable = Option( dataset.findVariable( varName ) ) getOrElse ( throw new Exception( "Can't find variable " + varName ) )
    val globalSection: ma2.Section = roiOpt.fold( variable.getShapeAsSection ) ( roi => variable.getShapeAsSection.intersect(roi) )
    val timeRange = globalSection.getRange(0)
    val nParts = math.ceil( timeRange.length / partSize.toFloat ).toInt
    val sections = for( iPart <- 0 until nParts; start= timeRange.first+iPart*partSize; partRange= new ma2.Range(start,start+partSize-1) ) yield {
      new ma2.Section(globalSection.getRanges.drop(1)).insertRange(0,partRange).intersect( globalSection )
    }
    sections
  }
}

class CDAve( val undef: Float = Float.NaN ) extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(StructField("inputColumn", ArrayType(FloatType,true)) :: Nil)
  def bufferSchema: StructType = { StructType(StructField("sum", FloatType) :: StructField("count", IntegerType ) :: Nil) }
  def dataType: DataType = FloatType
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = { buffer(0) = 0f; buffer(1) = 0 }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit =  {
    val inputSeq = input.getSeq[Float](0)
    for( index <- 0 until input.length; inputVal = inputSeq(index); if inputVal != undef ) {
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

case class Average(var sum: Float, var count: Int)

class CDAveST( val undef: Float = Float.NaN ) extends Aggregator[VariableRecord, Average, Float] {
  def zero: Average = Average(0f, 0)

  def reduce(buffer: Average, varRec: VariableRecord): Average = {
    for( index <- 0 until varRec.length; if varRec.data(index) != undef ) {
      buffer.sum += varRec.data(index)
      buffer.count += 1
    }
    buffer
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  def finish(reduction: Average): Float = reduction.sum / reduction.count
  def bufferEncoder: Encoder[Average] = Encoders.product
  def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

object SparkSQLTest extends Loggable {
    def main(args: Array[String]): Unit = {
      import org.apache.spark.sql.functions._
      val conf: SparkConf = CDSparkContext.getSparkConf("EDAS", true, false)
      val spark = SparkSession.builder().appName("EDAS").config(conf).getOrCreate()
      import spark.implicits._
      try {
        val t0 = System.nanoTime()
        val dataPath = args(0)
        val datasetMgr = new SparkDatasetMgr(dataPath)
        val varName = args(1)
        val roi = new ma2.Section( Array( 0, 6, 50, 50 ), Array( 10, 1, 20, 20 ) )
        val metaData = NetcdfDatasetMgr.createVariableMetadataRecord(varName, dataPath)
        spark.udf.register("cdave", new CDAve( metaData.missing ) )
        val sections = datasetMgr.getSections( varName, 1, Some(roi) )
        val rdd: RDD[VariableRecord] = spark.sparkContext.parallelize(sections).map(partSection => NetcdfDatasetMgr.createVariableDataRecord(varName, dataPath, partSection))
        rdd.count()
        val t1 = System.nanoTime()
        val df = spark.createDataFrame(rdd)
        df.createOrReplaceTempView("records")
        //      val averageValue = CDAveST.toColumn.name("average")
        //      val result = df.select(averageValue)
        val df_ave = spark.sql("SELECT cdave(data) as average FROM records")
        val t2 = System.nanoTime()
        df_ave.show()
        print( "\n Completed, Read Time: %.4f, Compute Time: %.4f \n".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
      } finally {
        spark.stop()
      }
    }
}

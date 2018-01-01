package nasa.nccs.edas.loaders
import nasa.nccs.cdapi.data.{FastMaskedArray, HeapFltArray, RDDRecord}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.KernelContext
import nasa.nccs.utilities.Loggable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import java.lang.Float
import scala.collection.immutable.TreeMap
import org.apache.spark.sql.types.DataTypes

case class EDASOptions( inputs: Array[String] ) {}

object RDDRecordConverter {
  def apply( keyVal: (RecordKey,RDDRecord), options: EDASOptions ) = new RDDRecordConverter( keyVal._2, options )
  def genericSchema: StructType = new StructType( Array( new StructField("index",DataTypes.IntegerType,false), new StructField("value",DataTypes.FloatType,false) ) )
}

class RDDRecordConverter( record: RDDRecord, options: EDASOptions ) extends Iterator[GenericInternalRow] with Loggable {
  val schema: StructType= inferSchema( record )
  private val row = new GenericInternalRow(schema.length)
  private val input_arrays: Seq[(String,HeapFltArray)] = options.inputs.map( id => id -> record.findElements(id).head )
  private val inputs:  Seq[(String,FastMaskedArray)] = input_arrays.map { case (id,heapArray) => (id,heapArray.toFastMaskedArray) }
  private val missing = input_arrays.head._2.getMissing()
  val shape: Array[Int] = inputs.head._2.array.getShape
  val dataSize: Int = shape.product
  private var rowIndex = 0

  def hasNext : scala.Boolean = {
    rowIndex == dataSize
  }

  def next() : GenericInternalRow = {
    val value = inputs.head._2.array.getFloat(rowIndex)
    row(1) = if(value == missing) null else value
    row(0) = rowIndex
    rowIndex = rowIndex + 1
    row
  }

  def inferSchema( rec: RDDRecord ): StructType = new StructType( Array( new StructField("index",DataTypes.IntegerType,false), new StructField("value",DataTypes.FloatType,false) ) ) // { FloatType, IntegerType, ShortType, ArrayType, ByteType, DateType, StringType, TimestampType }
}

object RDDSimpleRecordConverter {
  def apply( keyVal: (RecordKey,RDDRecord), options: EDASOptions ) = new RDDSimpleRecordConverter( keyVal._2, options )
  def genericSchema: StructType = new StructType( Array( new StructField("value",DataTypes.FloatType,false) ) )
}

class RDDSimpleRecordConverter( record: RDDRecord, options: EDASOptions ) extends Iterator[Float] with Loggable {
  val schema: StructType= inferSchema( record )
  private val row = new GenericInternalRow(schema.length)
  private val input_arrays: Seq[(String,HeapFltArray)] = options.inputs.map( id => id -> record.findElements(id).head )
  private val inputs:  Seq[(String,FastMaskedArray)] = input_arrays.map { case (id,heapArray) => (id,heapArray.toFastMaskedArray) }
  private val missing = input_arrays.head._2.getMissing()
  val shape: Array[Int] = inputs.head._2.array.getShape
  val dataSize: Int = shape.product
  private var rowIndex = 0

  def hasNext : scala.Boolean = {
    rowIndex == dataSize
  }

  def next() : Float = {
    val value = inputs.head._2.array.getFloat(rowIndex)
    if(value == missing) null else value
  }

  def inferSchema( rec: RDDRecord ): StructType = new StructType( Array( new StructField("value",DataTypes.FloatType,false) ) ) // { FloatType, IntegerType, ShortType, ArrayType, ByteType, DateType, StringType, TimestampType }
}


class RDDRecordsConverter( inputs: Iterator[(RecordKey,RDDRecord)], options: EDASOptions ) extends Iterator[GenericInternalRow] with Loggable {
  val iterator = inputs.foldLeft(Iterator[GenericInternalRow]()) { case ( baseIter, newKeyVal ) => baseIter ++ RDDRecordConverter(newKeyVal,options) }
  def hasNext : scala.Boolean = iterator.hasNext
  def next() : GenericInternalRow = iterator.next

}


class RDDSimpleRecordsConverter( inputs: Iterator[(RecordKey,RDDRecord)], options: EDASOptions ) extends Iterator[Float] with Loggable {
  val iterator = inputs.foldLeft(Iterator[Float]()) { case ( baseIter, newKeyVal ) => baseIter ++ RDDSimpleRecordConverter(newKeyVal,options) }
  def hasNext : scala.Boolean = iterator.hasNext
  def next() : Float = iterator.next

}


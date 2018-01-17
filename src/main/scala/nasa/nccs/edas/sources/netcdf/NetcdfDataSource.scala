package nasa.nccs.edas.sources.netcdf

import nasa.nccs.cdapi.data.{FastMaskedArray, HeapFltArray, CDTimeSlice}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.utilities.Loggable
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataTypes, FloatType, IntegerType, _}

class TempRow( val values: Seq[Any] ) extends Row {
  override def length: Int = values.size
  override def get(i: Int): Any = values(i)
  override def toSeq: Seq[Any] = values
  override def copy(): TempRow = this
}

case class EDASOptions( inputs: Array[String] ) {}

object CDTimeSliceConverter {
  def apply( keyVal: CDTimeSlice, options: EDASOptions ) = new CDTimeSliceConverter( keyVal._2, options )
  def defaultSchema: StructType = new StructType( Array( new StructField("index",IntegerType,false), new StructField("value",DataTypes.FloatType,true) ) )
}

class CDTimeSliceConverter( record: CDTimeSlice, options: EDASOptions ) extends Iterator[Row] with Loggable {
  val schema: StructType= inferSchema( record )
  private val input_arrays: Seq[(String,HeapFltArray)] = record.elements.iterator.toSeq // options.inputs.map( id => id -> record.findElements(id).head )
  private val inputs:  Seq[(String,FastMaskedArray)] = input_arrays.map { case (id,heapArray) => (id,heapArray.toFastMaskedArray) }
  private val missing: java.lang.Float = input_arrays.head._2.getMissing()
  val shape: Array[Int] = inputs.head._2.array.getShape
  val dataSize: Int = shape.product
  private var rowIndex = 0

  def hasNext : scala.Boolean = {
    rowIndex < dataSize
  }

  def next() : Row = {
    val value: java.lang.Float = inputs.head._2.array.getFloat(rowIndex)
    val row = Row(  rowIndex, { if(value == missing) null else value } )
    rowIndex = rowIndex + 1
    row
  }

//  def next1() : Row = {
//    val value: java.lang.Float = inputs.head._2.array.getFloat(rowIndex)
//    row(1) = if(value == missing) null else value
//    row(0) = rowIndex
//    rowIndex = rowIndex + 1
//    row.asInstanceOf[Row]
//  }

  def inferSchema( rec: CDTimeSlice ): StructType = new StructType( Array( new StructField("index",IntegerType,false), new StructField("value",DataTypes.FloatType,true) ) ) // { FloatType, IntegerType, ShortType, ArrayType, ByteType, DateType, StringType, TimestampType }
}

object RDDSimpleRecordConverter {
  def apply( keyVal: CDTimeSlice, options: EDASOptions ) = new RDDSimpleRecordConverter( keyVal._2, options )
  def genericSchema: StructType = new StructType( Array( new StructField("value",FloatType,true) ) )
}

class RDDSimpleRecordConverter( record: CDTimeSlice, options: EDASOptions ) extends Iterator[java.lang.Float] with Loggable {
  val schema: StructType= inferSchema( record )
  private val row = new GenericInternalRow(schema.length)
  private val input_arrays: Seq[(String,HeapFltArray)] = record.elements.iterator.toSeq //  options.inputs.map( id => id -> record.findElements(id).head )
  private val inputs:  Seq[(String,FastMaskedArray)] = input_arrays.map { case (id,heapArray) => (id,heapArray.toFastMaskedArray) }
  private val missing: java.lang.Float = input_arrays.head._2.getMissing()
  val shape: Array[Int] = inputs.head._2.array.getShape
  val dataSize: Int = shape.product
  private var rowIndex = 0

  def hasNext : scala.Boolean = {
    rowIndex < dataSize
  }

  def next() : java.lang.Float = {
    val value: java.lang.Float = inputs.head._2.array.getFloat(rowIndex)
    rowIndex = rowIndex + 1
    if(value == missing) null else value
  }

  def inferSchema( rec: CDTimeSlice ): StructType = new StructType( Array( new StructField("value",DataTypes.FloatType,true) ) ) // { FloatType, IntegerType, ShortType, ArrayType, ByteType, DateType, StringType, TimestampType }
}


class CDTimeSlicesConverter( inputs: Iterator[CDTimeSlice], options: EDASOptions ) extends Iterator[Row] with Loggable {
  val iterator = inputs.foldLeft(Iterator[Row]()) { case ( baseIter, newKeyVal ) => baseIter ++ CDTimeSliceConverter(newKeyVal,options) }
  def hasNext : scala.Boolean = iterator.hasNext
  def next() : Row = iterator.next

}


class RDDSimpleRecordsConverter( inputs: Iterator[CDTimeSlice], options: EDASOptions ) extends Iterator[java.lang.Float] with Loggable {
  val iterator = inputs.foldLeft(Iterator[java.lang.Float]()) { case ( baseIter, newKeyVal ) => baseIter ++ RDDSimpleRecordConverter(newKeyVal,options) }
  def hasNext : scala.Boolean = iterator.hasNext
  def next() : java.lang.Float = iterator.next

}


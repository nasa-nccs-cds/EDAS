package nasa.nccs.cdapi.data

import nasa.nccs.caching.{CachePartition, Partition, RegularPartition}
import nasa.nccs.cdapi.cdm.{RemapElem, TimeConversionSpec}
import nasa.nccs.cdapi.tensors.{CDFloatArray, _}
import nasa.nccs.edas.engine.spark.{RangePartitioner, RecordKey}
import nasa.nccs.edas.workers.TransVar
import nasa.nccs.esgf.process.{CDSection, GridContext, RequestContext, TargetGrid}
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.spark.rdd.RDD
import ucar.nc2.constants.AxisType
import ucar.ma2
import java.nio
import java.nio.FloatBuffer
import java.util.Formatter

import nasa.nccs.cdapi.data.FastMaskedArray.join
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import org.apache.spark.mllib.linalg.DenseVector

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.edas.kernels.KernelContext
import nasa.nccs.edas.portal.TestReadApplication.logger
import nasa.nccs.edas.rdd.CDTimeSlice
import nasa.nccs.edas.sources.{Aggregation, Collection, Collections}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import ucar.ma2.{ArrayFloat, Index, IndexIterator}
import ucar.nc2.dataset.{CoordinateAxis1DTime, NetcdfDataset}
import ucar.nc2.time.{CalendarDate, CalendarPeriod}

import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag

// Developer API for integrating various data management and IO frameworks such as SIA-IO and EDAS-Cache.
// It is intended to be deployed on the master node of the analytics server (this is not a client API).

object MetadataOps {
  def mergeMetadata(opName: String)( metadata0: Map[String, String], metadata1: Map[String, String]): Map[String, String] = {
    metadata0.map { case (key, value) =>
      metadata1.get(key) match {
        case Some(value1) =>
          if (value == value1) (key, value)
          else if( key == "roi")  ( key, CDSection.merge(value,value1) )
          else (key, opName + "(" + value + "," + value1 + ")")
        case None => (key, value)
      }
    }
  }
  def mergeMetadata( opName: String, metadata: Iterable[Map[String, String]] ): Map[String, String] = {
    metadata.foldLeft( metadata.head )( mergeMetadata(opName) )
  }
}

abstract class MetadataCarrier( val metadata: Map[String,String] = Map.empty ) extends Serializable with Loggable {
  def mergeMetadata( opName: String, other: MetadataCarrier ): Map[String,String] = MetadataOps.mergeMetadata( opName )( metadata, other.metadata )
  def toXml: xml.Elem
  def attr(id:String): String = metadata.getOrElse(id,"")
  def mdata(): java.util.Map[String,String] = metadata

  implicit def pimp(elem:xml.Elem) = new {
    def %(attrs:Map[String,String]) = {
      val seq = for( (n:String,v:String) <- attrs ) yield new xml.UnprefixedAttribute(n,v,xml.Null)
      (elem /: seq) ( _ % _ )
    }
  }

}

trait RDDataManager {

  def getDatasets(): Set[String]
  def getDatasetMetadata( dsid: String ): Map[String,String]

  def getVariables( dsid: String ): Set[String]
  def getVariableMetadata( vid: String ): Map[String,String]

  def getDataProducts(): Set[String] = Set.empty
  def getDataProductMetadata( pid: String ): Map[String,String] = Map.empty

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[CDTimeSlice]

}

trait BinSorter {
  def setCurrentCoords( coords: Array[Int] ): Unit
  def getReducedShape( shape: Array[Int] ): Array[Int]
  def getNumBins: Int
  def getBinIndex: Int
  def getItemIndex: Int
  def getVaryingAxis: Int = -1
}

object TimeCycleSorter {
  val Undef = -1
  val Diurnal = 0
  val Monthly = 1
  val Seasonal = 2

  val Month = 1
  val MonthOfYear = 2
  val Year = 3
}

class TimeSpecs( val input_data: HeapFltArray, val startIndex: Int ) extends Loggable {
  logger.info( "\n    *************>>>>>>>>>>> Opening gridSpec: " + input_data.gridSpec )
  private val __gridDS: NetcdfDataset = NetcdfDatasetMgr.aquireFile( input_data.gridSpec, 12.toString )
  val timeAxis = CoordinateAxis1DTime.factory( __gridDS, __gridDS.findCoordinateAxis(AxisType.Time), new Formatter())
  val dateList: IndexedSeq[CalendarDate] = timeAxis.section( new ma2.Range( startIndex, startIndex + input_data.shape(0)-1 ) ).getCalendarDates.toIndexedSeq
  val dateRange = ( timeAxis.getCalendarDate(0), timeAxis.getCalendarDate( timeAxis.getSize.toInt-1 ) )
  __gridDS.close()
}

class TimeCycleSorter(val input_data: HeapFltArray, val cycleParm: String, val binParm: String, val startIndex: Int ) extends BinSorter with Loggable {
  import TimeCycleSorter._
  val timeSpecs = new TimeSpecs( input_data, startIndex )
  val cycle = cycleParm match {
    case x if (x == "diurnal") || x.startsWith("hour")  => Diurnal
    case x if x.startsWith("month") => Monthly
    case x if x.startsWith("season") => Seasonal
  }
  val bin = binParm match {
    case x if x.startsWith("month") => Month
    case x if x.startsWith("monthof") => MonthOfYear
    case x if x.startsWith("year") => Year
    case x => Undef
  }
  private var _startBinIndex = -1
  private var _currentDate: CalendarDate = CalendarDate.of(0L)
  val _startMonth = timeSpecs.dateRange._1.getFieldValue( CalendarPeriod.Field.Month )
  val _startYear = timeSpecs.dateRange._1.getFieldValue( CalendarPeriod.Field.Year )
  lazy val yearRange = timeSpecs.dateList.last.getFieldValue( CalendarPeriod.Field.Year ) - timeSpecs.dateList.head.getFieldValue( CalendarPeriod.Field.Year )
  lazy val monthRange = timeSpecs.dateList.last.getFieldValue( CalendarPeriod.Field.Month ) - timeSpecs.dateList.head.getFieldValue( CalendarPeriod.Field.Month )
  lazy val fullYearRange = timeSpecs.dateRange._2.getFieldValue( CalendarPeriod.Field.Year ) - timeSpecs.dateRange._1.getFieldValue( CalendarPeriod.Field.Year )
  lazy val fullMonthRange = timeSpecs.dateRange._2.getFieldValue( CalendarPeriod.Field.Month ) - timeSpecs.dateRange._1.getFieldValue( CalendarPeriod.Field.Month )

  def getNumBins: Int = nBins
  def getSeason( monthIndex: Int ): Int = ( monthIndex - 2 ) / 3

  val nBins: Int = cycle match {
    case Diurnal => 24
    case Monthly => 12
    case Seasonal => 4
  }

  val nItems: Int = bin match {
    case Month => monthRange + 1 + 12*yearRange
    case MonthOfYear => 12
    case Year => yearRange + 1
    case Undef => 1
  }

  val nTotalItems: Int = bin match {
    case Month => fullMonthRange + 1 + 12*fullYearRange
    case MonthOfYear => 12
    case Year => fullYearRange + 1
    case Undef => 1
  }

  def getReducedShape( shape: Array[Int]  ): Array[Int] = {
    var newshape = Array.fill[Int](shape.length)(1)
    newshape(0) = nTotalItems
    newshape
  }

  def setCurrentCoords( coords: Array[Int] ): Unit = {
    _currentDate = timeSpecs.dateList( coords(0) )
  }

  def getBinIndex: Int = cycle match {
    case Diurnal => _currentDate.getHourOfDay
    case Monthly => _currentDate.getFieldValue( CalendarPeriod.Field.Month ) - 1
    case Seasonal => getSeason( _currentDate.getFieldValue( CalendarPeriod.Field.Month ) - 1 )
  }

  def getItemIndex: Int = bin match {
    case Month =>         _currentDate.getFieldValue( CalendarPeriod.Field.Month ) - _startMonth + ( _currentDate.getFieldValue( CalendarPeriod.Field.Year ) - _startYear ) * 12
    case MonthOfYear =>   _currentDate.getFieldValue( CalendarPeriod.Field.Month ) - _startMonth
    case Year =>          _currentDate.getFieldValue( CalendarPeriod.Field.Year ) - _startYear
    case Undef => 0
  }
}


class AnomalySorter(val input_data: HeapFltArray, val axesParm: String, grid: GridContext, val startIndex: Int ) extends BinSorter with Loggable {
  import TimeCycleSorter._
  val axes = axesParm.toLowerCase
  val timeSpecs = new TimeSpecs( input_data, startIndex )
  val yIndex: Int = grid.getAxisIndices( "y" ).getAxes(0)
  val (cycle, nBins) = if( axes.contains('t') ) ( Monthly,  12 ) else ( Undef, 1 )
  val removeYvar = axes.contains('y')
  private var _currentDate: CalendarDate = CalendarDate.of(0L)
  private var _currentCoords: Array[Int] = Array.emptyIntArray

  override def getVaryingAxis: Int = if( removeYvar ) { yIndex } else { -1 }

  def getNumBins: Int = nBins

  def getReducedShape( shape: Array[Int]  ): Array[Int] = {
    var newshape = Array.fill[Int](shape.length)(1)
    if( removeYvar ) { newshape(yIndex) = shape(yIndex) }
    newshape
  }

  def setCurrentCoords( coords: Array[Int] ): Unit = {
    _currentDate = timeSpecs.dateList( coords(0) )
    _currentCoords = coords
  }

  def getBinIndex: Int = cycle match {
    case Monthly => _currentDate.getFieldValue( CalendarPeriod.Field.Month ) - 1
    case Undef => 0
  }

  def getItemIndex: Int = if( removeYvar ) { _currentCoords(yIndex) } else { 0 }
}

object FastMaskedArray {
  type ReduceOp = (Float,Float)=>Float
  def apply( array: ma2.Array, missing: Float ): FastMaskedArray = new FastMaskedArray( array, missing )
  def apply( shape: Array[Int], data:  Array[Float], missing: Float ): FastMaskedArray = new FastMaskedArray( ma2.Array.factory( ma2.DataType.FLOAT, shape, data ), missing )
  def apply( shape: Array[Int], init_value: Float, missing: Float ): FastMaskedArray = new FastMaskedArray( ma2.Array.factory( ma2.DataType.FLOAT, shape, Array.fill[Float](shape.product)(init_value) ), missing )
  def apply( fltArray: CDFloatArray ): FastMaskedArray = new FastMaskedArray( ma2.Array.factory( ma2.DataType.FLOAT, fltArray.getShape, fltArray.getArrayData() ), fltArray.getInvalid )

  def weightedSum( arrays: Array[FastMaskedArray], wtsOpt: Option[FastMaskedArray] = None, axes: Array[Int] = Array.emptyIntArray ): ( FastMaskedArray, FastMaskedArray ) = {
    val input0: FastMaskedArray = arrays.head
    wtsOpt match {
      case Some( wts ) => if( !wts.shape.sameElements(input0.shape) ) { throw new Exception( s"Weights shape [${wts.shape.mkString(",")}] does not match data shape [${input0.shape.mkString(",")}]") }
      case None => Unit
    }
    val missing = input0.missing
    val iters: Array[IndexIterator] = arrays.map( _.array.getIndexIterator )
    val vsum_array = FastMaskedArray( input0.shape, 0.0f, input0.missing )
    val vsum_iter: IndexIterator = vsum_array.array.getIndexIterator
    val wsum_array = FastMaskedArray( input0.shape, 0.0f, input0.missing )
    val wsum_iter: IndexIterator = wsum_array.array.getIndexIterator
    val wtsIterOpt = wtsOpt.map( _.array.getIndexIterator )

    while ( vsum_iter.hasNext ) {
      var vsum = 0f
      var wsum = 0f
      for (i <- 0 until arrays.length) {
        val fval = iters(i).getFloatNext
        if ((fval != missing) && !fval.isNaN) {
          vsum = vsum + fval
          wsum = wsum + 1.0f
        }
      }
      vsum_iter.setFloatNext( if( axes.isEmpty ) { vsum } else { vsum/wsum }  )
      wsum_iter.setFloatNext( wsum  )
    }
    if( axes.isEmpty )    ( vsum_array, wsum_array )
    else   {
      val wts: FastMaskedArray = wtsOpt match {
        case Some(aveWts) => wsum_array * aveWts
        case None => wsum_array
      }
      vsum_array.weightedSum( axes, Some(wts) )
    }
  }

  def join( farrays: Array[FastMaskedArray], axis: Int ): FastMaskedArray = {
    val t0 = System.nanoTime()
    val nArrays = farrays.length
    val in_shape: Array[Int] = farrays(0).array.getShape
    val missing: Float = farrays(0).missing
    val (front, back) = in_shape.splitAt(axis)
    var out_shape = front ++ Array[Int](nArrays) ++ back
    val out_array: ArrayFloat = ucar.ma2.Array.factory(ucar.ma2.DataType.FLOAT, out_shape).asInstanceOf[ArrayFloat]
    val out_buffer: Any = out_array.getStorage
    val out_size = out_array.getSize.toInt
    val in_size = farrays(0).array.getSize.toInt
    val n0 = out_shape(0)
    val stride0 = out_size/n0

    if( axis == 0 ) {
      val slice_size = in_size
      for (i0 <- farrays.indices; farray = farrays(i0)) {
        for (ielem0 <- 0 until slice_size) {
          val f0 = farray.array.getFloat(ielem0)
          val elem1 = i0 * stride0 + ielem0
          out_array.setFloat(elem1, f0)
        }
      }
    } else if( axis == 1 ) {
      val n1 = out_shape(1)
      val stride1 = stride0/n1
      val slice_stride0 = in_size / n0
      for (i1 <- farrays.indices; farray = farrays(i1)) {
        for (i0 <- 0 until n0) {
          for (i23 <- 0 until slice_stride0) {
            val elem0 = i0 * slice_stride0 + i23
            val f0 = farray.array.getFloat(elem0)
            val elem1 = i0 * stride0 + i1 * stride1 + i23
            out_array.setFloat(elem1, f0)
          }
        }
      }
    } else if( axis == 2 ) {
      val n1 = out_shape(1)
      val n2 = out_shape(2)
      val n3 = out_shape(3)
      val stride1 = stride0/n1
      val stride2 = stride1/n2
      val slice_stride0 = in_size /  n0
      val slice_stride1 = slice_stride0 / n1
      for (i2 <- farrays.indices; farray = farrays(i2)) {
        for (i0 <- 0 until n0) {
          for (i1 <- 0 until n1) {
            for (i3 <- 0 until n3) {
              val elem0 = i0 * slice_stride0 + i1 * slice_stride1 + i3
              val f0 = farray.array.getFloat(elem0)
              val elem1 = i0 * stride0 + i1 * stride1 + i2 * stride2 + i3
              out_array.setFloat(elem1, f0)
            }
          }
        }
      }
    } else  {
      throw new Exception( "Operation not yet implemented: axis = " + axis.toString )
    }
    val t1 = System.nanoTime()
    logger.info(s"Completed array join operation, time = %.4f sec".format( (t1 - t0) / 1.0E9))
    new FastMaskedArray( out_array, missing )
  }
}

class FastMaskedArray(val array: ma2.Array, val missing: Float ) extends Loggable {

  def getData: Array[Float] = array.getStorage.asInstanceOf[Array[Float]]

  def getReductionAxes( s0: Array[Int], s1: Array[Int] ): Array[Int] = {
    var axes = ListBuffer.empty[Int]
    for ( index <- s0.indices; v0 = s0(index); v1 = s1(index); if (v0 != v1) ) {
      if( v1 == 1 ) axes += index
      else { throw new Exception( s"Incommensurate shapes in dual array operation: [${s0.mkString(",")}] --  [${s1.mkString(",")}] ") }
    }
    axes.toArray
  }

  def compress( indices: Array[Int], axis: Int ): FastMaskedArray = {
    val slices = indices.map( index => new FastMaskedArray( array.slice(axis,index).copy, missing ) )
    FastMaskedArray.join( slices, axis )
  }

  def reduce(op: FastMaskedArray.ReduceOp, axes: Array[Int], initVal: Float = 0f ): FastMaskedArray = {
    val rank = array.getRank
    val iter: IndexIterator = array.getIndexIterator()
    if( axes.length == rank ) {
      var result = initVal
      var result_shape = Array.fill[Int](rank)(1)
      while ( iter.hasNext ) {
        val fval = iter.getFloatNext
        if( ( fval != missing ) && !fval.isNaN  ) {
          result = op( result, fval )
        }
      }
      FastMaskedArray(result_shape,Array(result),missing)
    } else {
      val target_shape: Array[Int] = getReducedShape( axes )
      val target_array = FastMaskedArray( target_shape, initVal, missing )
      val targ_index: Index =	target_array.array.getIndex()
      while ( iter.hasNext ) {
        val fval = iter.getFloatNext
        if( ( fval != missing ) && !fval.isNaN  ) {
          val current_index = getReducedFlatIndex( targ_index, axes, iter )
          target_array.array.setFloat( current_index, op( target_array.array.getFloat(current_index), fval ) )
        }
      }
      target_array
    }
  }

  def compareShapes( other_shape: Array[Int] ): ( Int, Option[Array[Int]] ) = {
    val p0 = array.getShape.product
    val p1 = other_shape.product
    if( p0 == p1 ) ( 0, None )
    else if( p0 > p1 )  {
      val axes = getReductionAxes( array.getShape, other_shape )
      ( 1, Some(axes) )
    } else {
      val axes = getReductionAxes( other_shape, array.getShape )
      ( -1, Some(axes) )
    }
  }

  def merge(other: FastMaskedArray, op: FastMaskedArray.ReduceOp ): FastMaskedArray = {
    val ( shape_comparison, axes ) = compareShapes( other.array.getShape )
    if( shape_comparison == 0 ) {
      val vTot: Array[Float] = new Array(array.getSize.toInt)
      ( 0 until vTot.size ) foreach (index => {
        val uv0 = array.getFloat(index)
        val uv1 = other.array.getFloat(index)
        vTot( index ) = if( (uv0 == missing) || uv0.isNaN || (uv1 == other.missing) || uv1.isNaN ) { missing } else {  op( uv0, uv1 ) }
      })
      FastMaskedArray( shape, vTot, missing )
    } else {
      val base_array = if( shape_comparison > 0 ) { array } else { other.array }
      val reduced_array = if( shape_comparison > 0 ) { other.array } else { array }
      val target_array = FastMaskedArray( base_array.getShape, 0.0f, missing )
      val base_iter: IndexIterator = base_array.getIndexIterator
      val result_iter: IndexIterator = target_array.array.getIndexIterator
      val reduced_index: Index =	reduced_array.getIndex
      while ( base_iter.hasNext ) {
        val v0: Float = base_iter.getFloatNext
        if( ( v0 != missing ) && !v0.isNaN ) {
          val reduced_flat_index: Int = getReducedFlatIndex( reduced_index, axes.get, base_iter )
          val v1: Float = reduced_array.getFloat( reduced_flat_index )
          if( ( v1 != missing ) && !v1.isNaN ) {
            result_iter.setFloatNext( op(v0, v1) )
          } else {
            result_iter.setFloatNext( missing )
          }
        } else {
          result_iter.setFloatNext( missing )
        }
      }
      target_array
    }
  }

  def +( other: FastMaskedArray ): FastMaskedArray = {
    assert ( other.shape.sameElements(shape), s"Error, attempt to add arrays with different shapes: {${other.shape.mkString(",")}} -- {${shape.mkString(",")}}")
    val vTot = new ma2.ArrayFloat( array.getShape )
    (0 until array.getSize.toInt ) foreach ( index => {
      val uv0: Float = array.getFloat(index)
      val uv1: Float = other.array.getFloat(index)
      if( (uv0==missing) || uv0.isNaN || (uv1==other.missing) || uv1.isNaN ) { missing }
      else {  vTot.setFloat(index, uv0 + uv1)  }
    } )
    FastMaskedArray( vTot, missing )
  }

  def *( other: FastMaskedArray ): FastMaskedArray = {
    assert ( other.shape.sameElements(shape), s"Error, attempt to add arrays with different shapes: {${other.shape.mkString(",")}} -- {${shape.mkString(",")}}")
    val vTot = new ma2.ArrayFloat( array.getShape )
    (0 until array.getSize.toInt ) foreach ( index => {
      val uv0: Float = array.getFloat(index)
      val uv1: Float = other.array.getFloat(index)
      if( (uv0==missing) || uv0.isNaN || (uv1==other.missing) || uv1.isNaN ) { missing }
      else {  vTot.setFloat(index, uv0 * uv1)  }
    } )
    FastMaskedArray( vTot, missing )
  }

  def /( other: FastMaskedArray ): FastMaskedArray = {
    assert ( other.shape.sameElements(shape), s"Error, attempt to add arrays with different shapes: {${other.shape.mkString(",")}} -- {${shape.mkString(",")}}")
    val vTot = new ma2.ArrayFloat( array.getShape )
    (0 until array.getSize.toInt ) foreach ( index => {
      val uv0: Float = array.getFloat(index)
      val uv1: Float = other.array.getFloat(index)
      if( (uv0==missing) || uv0.isNaN || (uv1==other.missing) || (uv1==0f) || uv1.isNaN ) { vTot.setFloat(index, missing ) }
      else {  vTot.setFloat(index, uv0 / uv1)  }
    } )
    FastMaskedArray( vTot, missing )
  }

  def shape = array.getShape
  def toCDFloatArray = CDFloatArray.factory(array,missing)
  def toFloatArray = CDFloatArray.factory(array,missing).getArrayData()

  def weightedSum( op_axes: Array[Int], wtsOpt: Option[FastMaskedArray] ): ( FastMaskedArray, FastMaskedArray ) = {
    val wtsIterOpt = wtsOpt.map( _.array.getIndexIterator )
    val ( shape_comparison, broadcast_axes ) = wtsOpt match {
      case Some( wts ) =>   compareShapes( wts.array.getShape )
      case None =>          ( 0, Array.emptyIntArray )
    }
    val rank = array.getRank
    val iter: IndexIterator = array.getIndexIterator()
    if( shape_comparison == 0  ) {
      if( op_axes.length == rank ) {
        var result = 0f
        var count = 0f
        var result_shape = Array.fill[Int](rank)(1)
        while ( iter.hasNext ) {
          val fval = iter.getFloatNext
          if( ( fval != missing ) && !fval.isNaN ) {
            wtsIterOpt match {
              case Some( wtsIter ) =>
                val wtval = wtsIter.getFloatNext
                result = result + fval * wtval
                count = count + wtval
              case None =>
                result = result + fval
                count = count + 1f
            }
          }
        }
        ( FastMaskedArray(result_shape,Array(result),missing), FastMaskedArray(result_shape,Array(count),missing) )
      } else {
        val target_shape: Array[Int] = getReducedShape( op_axes )
        val target_array = FastMaskedArray( target_shape, 0.0f, missing )
        val weights_array = FastMaskedArray( target_shape, 0.0f, missing )
        val targ_index: Index =	target_array.array.getIndex()
        while ( iter.hasNext ) {
          val fval = iter.getFloatNext
          if( ( fval != missing ) && !fval.isNaN ) {
            val current_index = getReducedFlatIndex( targ_index, op_axes, iter )
            wtsIterOpt match {
              case Some(wtsIter) =>
                val wtval = wtsIter.getFloatNext
                target_array.array.setFloat(current_index, target_array.array.getFloat(current_index) + fval*wtval )
                weights_array.array.setFloat(current_index, weights_array.array.getFloat(current_index) + wtval )
              case None =>
                target_array.array.setFloat( current_index, target_array.array.getFloat(current_index) + fval )
                weights_array.array.setFloat( current_index, weights_array.array.getFloat(current_index) + 1.0f )
            }
          }
        }
        ( target_array, weights_array )
      }
    } else {
      val target_shape: Array[Int] = getReducedShape( op_axes )
      val target_array = FastMaskedArray( target_shape, 0.0f, missing )
      val weights_array = FastMaskedArray( target_shape, 0.0f, missing )
      val targ_index: Index =	target_array.array.getIndex()
      val wtsIndexOpt: Option[Index] = wtsOpt.map( _.array.getIndex )
      while ( iter.hasNext ) {
        val fval = iter.getFloatNext
        val wts: ma2.Array = wtsOpt.get.array
        if( ( fval != missing ) && !fval.isNaN ) {
          val current_index = getReducedFlatIndex( targ_index, op_axes, iter )
          wtsIndexOpt match {
            case Some(wtsIndex) =>
              val reduced_flat_index: Int = getReducedFlatIndex( wtsIndex, broadcast_axes, iter )
              val wtval: Float = wts.getFloat( reduced_flat_index )
              target_array.array.setFloat(current_index, target_array.array.getFloat(current_index) + fval*wtval )
              weights_array.array.setFloat(current_index, weights_array.array.getFloat(current_index) + wtval )
            case None =>
              target_array.array.setFloat( current_index, target_array.array.getFloat(current_index) + fval )
              weights_array.array.setFloat( current_index, weights_array.array.getFloat(current_index) + 1.0f )
          }
        }
      }
      ( target_array, weights_array )
    }
  }

  def bin( sorter: BinSorter, op: FastMaskedArray.ReduceOp,  initVal: Float = 0f ): IndexedSeq[FastMaskedArray] = {
    val rank = array.getRank
    val iter: IndexIterator = array.getIndexIterator
    val target_shape: Array[Int] = sorter.getReducedShape( array.getShape )
    val nBins: Int = sorter.getNumBins
    val target_arrays = ( 0 until nBins ) map ( index => FastMaskedArray( target_shape, initVal, missing ) )
    while ( iter.hasNext ) {
      val fval = iter.getFloatNext
      if( ( fval != missing ) && !fval.isNaN  ) {
        var coords: Array[Int] = iter.getCurrentCounter
        sorter.setCurrentCoords( coords )
        val binIndex: Int = sorter.getBinIndex
        val target_array = target_arrays( binIndex )
        val itemIndex: Int = sorter.getItemIndex
        val tval = target_array.array.getFloat(itemIndex)
        logger.info( s"BIN: binIndex: ${binIndex}, itemIndex: ${itemIndex}, fval: ${fval}, tval: ${tval}")
        target_array.array.setFloat( itemIndex, op( tval, fval ) )
      }
    }
    target_arrays
  }

  def weightedSumBin( sorter: BinSorter, wtsOpt: Option[FastMaskedArray] ): ( IndexedSeq[FastMaskedArray], IndexedSeq[FastMaskedArray] ) = {
    val rank = array.getRank
    val wtsIterOpt = wtsOpt.map( _.array.getIndexIterator )
    wtsOpt match {
      case Some( wts ) => if( !wts.array.getShape.sameElements(array.getShape) ) { throw new Exception( s"Weights shape [${wts.array.getShape().mkString(",")}] does not match data shape [${array.getShape.mkString(",")}]") }
      case None => Unit
    }
    val iter: IndexIterator = array.getIndexIterator
    val target_shape: Array[Int] = sorter.getReducedShape( array.getShape )
    val nBins: Int = sorter.getNumBins
    val target_arrays = ( 0 until nBins ) map ( index => FastMaskedArray( target_shape, 0f, missing ) )
    val weight_arrays = ( 0 until nBins ) map ( index => FastMaskedArray( target_shape, 0f, missing ) )
    while ( iter.hasNext ) {
      val fval = iter.getFloatNext
      if( ( fval != missing ) && !fval.isNaN  ) {
        var coords: Array[Int] = iter.getCurrentCounter
        sorter.setCurrentCoords( coords )
        val binIndex: Int = sorter.getBinIndex
        val target_array = target_arrays( binIndex )
        val weight_array = weight_arrays( binIndex )
        val itemIndex: Int = sorter.getItemIndex
        val currVal = target_array.array.getFloat(itemIndex)
        val currWt = weight_array.array.getFloat(itemIndex)
        val wtVal = wtsIterOpt match {
          case Some(wtsIter) =>
            val wt = wtsIter.getFloatNext
            target_array.array.setFloat( itemIndex, currVal + fval*wt )
            wt
          case None =>
            target_array.array.setFloat( itemIndex, currVal + fval )
            1f
        }
        weight_array.array.setFloat( itemIndex, weight_array.array.getFloat(itemIndex) + wtVal )
      }
    }
    ( target_arrays, weight_arrays )
  }

  def binMerge( sorter: BinSorter, binnedData: Map[Int,FastMaskedArray], varyingAxisIndex: Int, reduceOp: ReduceOpFlt ):  FastMaskedArray = {
    val iter: IndexIterator = array.getIndexIterator
    val target_array = FastMaskedArray( array.getShape, 0f, missing )
    val target_array_iter: IndexIterator = target_array.array.getIndexIterator
    while ( iter.hasNext ) {
      val fval = iter.getFloatNext
      if( ( fval == missing ) || fval.isNaN  ) {
        target_array_iter.setFloatNext( missing )
      } else {
        var coords: Array[Int] = iter.getCurrentCounter
        sorter.setCurrentCoords( coords )
        val binIndex: Int = sorter.getBinIndex
        val otherDataArray = binnedData(binIndex)
        val otherItemIndex = if( varyingAxisIndex >= 0 ) { coords( varyingAxisIndex ) } else { 0 }
        val otherVal = otherDataArray.array.getFloat( otherItemIndex )
        target_array_iter.setFloatNext( reduceOp(fval,otherVal) )
      }
    }
    target_array
  }

  def getReducedFlatIndex( reduced_index: Index, reduction_axes: Array[Int], iter: IndexIterator ): Int = {
    var coords: Array[Int] = iter.getCurrentCounter
    reduction_axes.length match {
      case 1 => coords( reduction_axes(0) ) = 0
      case 2 => coords( reduction_axes(0) ) = 0; coords( reduction_axes(1) ) = 0
      case 3 => coords( reduction_axes(0) ) = 0; coords( reduction_axes(1) ) = 0; coords( reduction_axes(2) ) = 0
      case 4 => coords( reduction_axes(0) ) = 0; coords( reduction_axes(1) ) = 0; coords( reduction_axes(2) ) = 0; coords( reduction_axes(3) ) = 0
      case 5 => coords( reduction_axes(0) ) = 0; coords( reduction_axes(1) ) = 0; coords( reduction_axes(2) ) = 0; coords( reduction_axes(3) ) = 0; coords( reduction_axes(4) ) = 0
      case x: Int  => throw new Exception( s"Unsupported number of axes in reduction: ${reduction_axes.length}")
    }
    reduced_index.set( coords )
    reduced_index.currentElement()
  }

  def getReducedShape( reduction_axes: Array[Int] ): Array[Int] = {
    val reduced_shape: Array[Int] = array.getShape.clone
    reduction_axes.length match {
      case 1 => reduced_shape( reduction_axes(0) ) = 1
      case 2 => reduced_shape( reduction_axes(0) ) = 1; reduced_shape( reduction_axes(1) ) = 1
      case 3 => reduced_shape( reduction_axes(0) ) = 1; reduced_shape( reduction_axes(1) ) = 1; reduced_shape( reduction_axes(2) ) = 1
      case 4 => reduced_shape( reduction_axes(0) ) = 1; reduced_shape( reduction_axes(1) ) = 1; reduced_shape( reduction_axes(2) ) = 1; reduced_shape( reduction_axes(3) ) = 1
      case 5 => reduced_shape( reduction_axes(0) ) = 1; reduced_shape( reduction_axes(1) ) = 1; reduced_shape( reduction_axes(2) ) = 1; reduced_shape( reduction_axes(3) ) = 1; reduced_shape( reduction_axes(4) ) = 1
      case x: Int  => throw new Exception( s"Unsupported number of axes in reduction: ${reduction_axes.length}")
    }
    reduced_shape
  }

  def getReducedShape( reduction_axis: Int, reduced_value: Int ): Array[Int] = {
    val reduced_shape: Array[Int] = array.getShape.clone
    reduced_shape( reduction_axis ) = reduced_value
    reduced_shape
  }


}

abstract class ArrayBase[T <: AnyVal]( val shape: Array[Int]=Array.emptyIntArray, val origin: Array[Int]=Array.emptyIntArray, val missing: Option[T]=None, metadata: Map[String,String]=Map.empty, val indexMaps: List[CDCoordMap] = List.empty ) extends MetadataCarrier(metadata) with Serializable {

  def data:  Array[T]
  def toCDFloatArray: CDFloatArray
  def toCDDoubleArray: CDDoubleArray
  def toCDLongArray: CDLongArray
  def toFastMaskedArray: FastMaskedArray
  def toUcarFloatArray: ucar.ma2.Array = toCDFloatArray
  def toUcarDoubleArray: ucar.ma2.Array = toCDDoubleArray
  def toCDWeightsArray: Option[CDFloatArray] = None
  def toMa2WeightsArray: Option[FastMaskedArray] = None
  def getMetadataStr = metadata map { case ( key, value ) => key + ":" + value } mkString (";")
  def getSampleData( size: Int, start: Int): Array[Float] = toCDFloatArray.getSampleData( size, start )
  def getSampleDataStr( size: Int, start: Int): String = toCDFloatArray.getSampleData( size, start ).mkString( "[ ",", "," ]")
  def uid: String = metadata.getOrElse("uid", metadata.getOrElse("collection","") + ":" + metadata.getOrElse("name",""))
  def size: Long = data.length
  override def toString = "<array shape=(%s)> %s </array>".format( shape.mkString(","), metadata.mkString(",") )

}

class HeapFltArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val data:  Array[Float]=Array.emptyFloatArray, _missing: Option[Float]=None,
                    val gridSpec: String = "", metadata: Map[String,String]=Map.empty, private val _optWeights: Option[Array[Float]]=None, indexMaps: List[CDCoordMap] = List.empty ) extends ArrayBase[Float](shape,origin,_missing,metadata,indexMaps) with Loggable {
  def weights: Option[Array[Float]] = _optWeights
  override def toCDWeightsArray: Option[CDFloatArray] = _optWeights.map( CDFloatArray( shape, _, getMissing() ) )
  override def toMa2WeightsArray: Option[FastMaskedArray] = _optWeights.map( FastMaskedArray( shape, _, getMissing() ) )
  def getMissing( default: Float = Float.MaxValue ): Float = _missing.getOrElse(default)
  def sameGrid( other: HeapFltArray): Boolean = gridFilePath.equals( other.gridFilePath )
  def hasData: Boolean = (data.length > 0)
  def toVector: DenseVector = new DenseVector( data.map(_.toDouble ) )
  val gridFilePath: String = NetcdfDatasetMgr.cleanPath(gridSpec)
  override def size: Long = shape.foldLeft(1L)(_ * _)

  def section( new_section: ma2.Section ): HeapFltArray = {
    val current_section = new ma2.Section(origin,shape)
    logger.info( s"HeapFltArray.section: current_section = ${current_section.toString}, new_section = ${new_section.toString}" )
    if( new_section.contains( current_section ) ) { this } else {
      print( s" Intersect Sections: ${new_section.toString} <-> ${current_section.toString}")
      val sub_section = new_section.intersect(current_section).shiftOrigin(current_section)
      val ucarArray: ucar.ma2.Array = toUcarFloatArray.sectionNoReduce( sub_section.getRanges )
      HeapFltArray(CDArray(ucarArray, getMissing()), new_section.getOrigin, gridSpec, metadata, None)
    }
  }
  def reinterp( weights: Map[Int,RemapElem], origin_mapper: Array[Int] => Array[Int] ): HeapFltArray = {
    val reinterpArray = toCDFloatArray.reinterp(weights)
    new HeapFltArray( reinterpArray.getShape, origin_mapper(origin), reinterpArray.getArrayData(), Some(reinterpArray.getInvalid), gridSpec, metadata )
  }
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data, getMissing(), indexMaps )
  def toUcarArray: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, shape, data )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), getMissing() )
  def toFastMaskedArray: FastMaskedArray = FastMaskedArray( shape, data, getMissing() )
  def toCDLongArray: CDLongArray = CDLongArray( shape, data.map(_.toLong) )
  def verifyGrids( other: HeapFltArray ) = if( !sameGrid(other) ) throw new Exception( s"Error, attempt to combine arrays with different grids: $gridSpec vs ${other.gridSpec}")
  def append( other: HeapFltArray ): HeapFltArray = {
    verifyGrids( other )
//    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    HeapFltArray(toCDFloatArray.append(other.toCDFloatArray), origin, gridSpec, mergeMetadata("merge", other), toCDWeightsArray.map(_.append(other.toCDWeightsArray.get)))
  }
  def flex_append( other: HeapFltArray, checkContiguous: Boolean = false ): HeapFltArray = {
    verifyGrids( other )
//    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if( origin(0) < other.origin(0) ) {
      if( checkContiguous && (origin(0) + shape(0) != other.origin(0)) ) throw new Exception( "Appending non-contiguous arrays" )
      HeapFltArray(toCDFloatArray.append(other.toCDFloatArray), origin, gridSpec, mergeMetadata("merge", other), toCDWeightsArray.map(_.append(other.toCDWeightsArray.get)))
    } else {
      if( checkContiguous && (other.origin(0) + other.shape(0) != origin(0)) ) throw new Exception( "Appending non-contiguous arrays" )
      HeapFltArray(other.toCDFloatArray.append(toCDFloatArray), other.origin, gridSpec, mergeMetadata("merge", other), other.toCDWeightsArray.map(_.append(toCDWeightsArray.get)))
    }
  }
  def split( index_offset: Int ): ( HeapFltArray, HeapFltArray ) = toCDFloatArray.split(index_offset) match {
      case (a0, a1) =>
        val ( fa0, fa1 ) = ( CDFloatArray(a0), CDFloatArray(a1) )
        val origin1 = origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) ( origin(0) + index_offset ) else o }
        ( new HeapFltArray( fa0.getShape, origin, fa0.getArrayData(), Some(a0.getInvalid), gridSpec, metadata, _optWeights, fa0.getCoordMaps ),
          new HeapFltArray( fa1.getShape, origin1, fa1.getArrayData(), Some(a1.getInvalid), gridSpec, metadata, _optWeights, fa1.getCoordMaps ) )    // TODO: split weights and coord maps?
    }

  def slice( startIndex: Int, size: Int ): HeapFltArray = {
    logger.debug( s"HeapFltArray: slice --> startIndex:{${startIndex}} size:{${size}} ")
    val fa = CDFloatArray(toCDFloatArray.slice(0,startIndex,size))
    val origin1 = origin.zipWithIndex.map{ case (o,i) => if( i == 0 ) ( origin(0) + startIndex ) else o }
    new HeapFltArray( fa.getShape, origin, fa.getArrayData(), Some(fa.getInvalid), gridSpec, metadata, _optWeights, fa.getCoordMaps ) // TODO: split weights and coord maps?
  }

  def toByteArray() = {
    val mval = missing.getOrElse(Float.MaxValue)
    HeapFltArray.bb.putFloat( 0, mval )
    toUcarFloatArray.getDataAsByteBuffer().array() ++ HeapFltArray.bb.array()
  }
//  def combine( combineOp: CDArray.ReduceOp[Float], other: HeapFltArray ): HeapFltArray = {
//    verifyGrids( other )
//    val result = toFastMaskedArray.merge( other.toFastMaskedArray, combineOp )
//    HeapFltArray( result.toCDFloatArray, origin, gridSpec, mergeMetadata("merge",other), toCDWeightsArray.map( _.append( other.toCDWeightsArray.get ) ) )
//  }
  def findValue( value: Float, match_required: Boolean=true, eps: Float = 0.0001f ): Option[Int] = { val seps = eps*value; data.indexWhere( x => Math.abs(x-value) < seps ) }  match {
    case -1 => if(match_required) throw new Exception(s"Failed to find a match in array for value ${value}, array values = ${data.map(_.toString).mkString(",")}"); None
    case x => Some(x)
  }
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString}> { data.mkString(",")} </array> % metadata
}

object HeapFltArray extends Loggable {
  val bb = java.nio.ByteBuffer.allocate(4)

  def apply( cdarray: CDFloatArray, origin: Array[Int], metadata: Map[String,String], optWeights: Option[Array[Float]] ): HeapFltArray = {
    val gridSpec = metadata.get( "gridfile" ).map( "file://" + _ ).getOrElse("")
    new HeapFltArray(cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), gridSpec, metadata, optWeights, cdarray.getCoordMaps)
  }
  def apply( shape: Array[Int], origin: Array[Int], metadata: Map[String,String] ): HeapFltArray = {
    val gridSpec = metadata.get( "gridfile" ).map( "file:/" + _ ).getOrElse("")
    new HeapFltArray( shape, origin, Array.emptyFloatArray, None, gridSpec, metadata )
  }
  def apply( cdarray: CDFloatArray, origin: Array[Int], gridSpec: String, metadata: Map[String,String], optWeights: Option[CDFloatArray] ): HeapFltArray = {
    new HeapFltArray(cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), gridSpec, metadata, optWeights.map(_.getArrayData()), cdarray.getCoordMaps)
  }
  def apply( heaparray: HeapFltArray, weights: CDFloatArray ): HeapFltArray = {
    new HeapFltArray( heaparray.shape, heaparray.origin, heaparray.data, Some(heaparray.getMissing()), heaparray.gridSpec, heaparray.metadata + ("wshape" -> weights.getShape.mkString(",")), Some(weights.getArrayData()), heaparray.indexMaps )
  }
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], gridSpec: String, metadata: Map[String,String], missing: Float ): HeapFltArray = HeapFltArray( CDArray(ucarray,missing), origin, gridSpec, metadata, None )

  def apply( tvar: TransVar, _gridSpec: Option[String] = None, _origin: Option[Array[Int]] = None ): HeapFltArray = {
    val buffer = tvar.getDataBuffer()
    val buff_size = buffer.capacity()
    val undef = buffer.getFloat(buff_size-4)
    val data_buffer = nio.ByteBuffer.wrap( buffer.array(), 0, buff_size-4 )
    logger.info( "Creating Array, num floats in buff = " + ((buff_size-4)/4).toString + ", shape = " + tvar.getShape.mkString(",") )
    val ucarray: ma2.Array = ma2.Array.factory( ma2.DataType.FLOAT, tvar.getShape, data_buffer )
    val floatArray: CDFloatArray = CDFloatArray.cdArrayConverter(CDArray[Float](ucarray, undef ) )
    val gridSpec = _gridSpec.getOrElse("file:/" + tvar.getMetaData.get("gridfile"))
    val origin = _origin.getOrElse(tvar.getOrigin)
    new HeapFltArray( tvar.getShape, origin, floatArray.getStorageArray, Some(undef), gridSpec, tvar.getMetaData.asScala.toMap )
  }
  def empty(rank:Int) = HeapFltArray( CDFloatArray.empty, Array.fill(rank)(0), "", Map.empty[String,String], None )
  def toHeapFloatArray( fltBaseArray: ArrayBase[Float] ): HeapFltArray = fltBaseArray match {
    case heapFltArray: HeapFltArray => heapFltArray
    case wtf => throw new Exception( "HeapFltArray cast error from ArrayBase[Float]" )
  }
}

class HeapDblArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Double]=Array.emptyDoubleArray, _missing: Option[Double]=None, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Double](shape,origin,_missing,metadata) {
  def data: Array[Double] = _data_
  override def size: Long = shape.foldLeft(1L)(_ * _)

  def getMissing(default: Double = Double.MaxValue): Double = _missing.getOrElse(default)

  def toCDFloatArray: CDFloatArray = CDFloatArray(shape, data.map(_.toFloat), getMissing().toFloat)

  def toFastMaskedArray: FastMaskedArray = FastMaskedArray(shape, data.map(_.toFloat), getMissing().toFloat)

  def toCDLongArray: CDLongArray = CDLongArray(shape, data.map(_.toLong))

  def toCDDoubleArray: CDDoubleArray = CDDoubleArray(shape, data, getMissing())

  def append(other: ArrayBase[Double]): ArrayBase[Double] = {
    //    logger.debug( "sAppending array: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if (origin(0) < other.origin(0)) {
      assert(origin(0) + shape(0) == other.origin(0), "Appending non-contiguous arrays")
      HeapDblArray(toCDDoubleArray.append(other.toCDDoubleArray), origin, mergeMetadata("merge", other))
    } else {
      assert(other.origin(0) + other.shape(0) == origin(0), "Appending non-contiguous arrays")
      HeapDblArray(other.toCDDoubleArray.append(toCDDoubleArray), other.origin, mergeMetadata("merge", other))
    }
  }

  def combine(combineOp: CDArray.ReduceOp[Double], other: ArrayBase[Double]): ArrayBase[Double] = HeapDblArray(CDDoubleArray.combine(combineOp, toCDDoubleArray, other.toCDDoubleArray), origin, mergeMetadata("merge", other))

  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString}>
    {_data_.mkString(",")}
  </array> % metadata

  def findValue(value: Double, match_required: Boolean = true, eps: Float = 0.0001f): Option[Int] = { _data_.indexWhere(x => Math.abs(x - value) < eps * value) } match {
    case -1 => if (match_required) throw new Exception(s"Failed to find a match in array for value ${value}, array values = ${_data_.map(_.toString).mkString(",")}"); None
    case x => Some(x)
  }
}

object HeapDblArray {
  def apply( cdarray: CDDoubleArray, origin: Array[Int], metadata: Map[String,String] ): HeapDblArray = new HeapDblArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapDblArray = HeapDblArray( CDDoubleArray.factory(ucarray,missing), origin, metadata )
}

class HeapLongArray( shape: Array[Int]=Array.emptyIntArray, origin: Array[Int]=Array.emptyIntArray, val _data_ : Array[Long]=Array.emptyLongArray, _missing: Option[Long]=None, metadata: Map[String,String]=Map.empty ) extends ArrayBase[Long](shape,origin,_missing,metadata)  {
  def data: Array[Long] = _data_
  def getMissing( default: Long = Long.MaxValue ): Long = _missing.getOrElse(default)
  def toCDFloatArray: CDFloatArray = CDFloatArray( shape, data.map(_.toFloat), Float.MaxValue )
  def toFastMaskedArray: FastMaskedArray = FastMaskedArray( shape, data.map(_.toFloat), Float.MaxValue )
  def toCDLongArray: CDLongArray = CDLongArray( shape, data )
  def toCDDoubleArray: CDDoubleArray = CDDoubleArray( shape, data.map(_.toDouble), Double.MaxValue )
  override def size: Long = shape.foldLeft(1L)(_ * _)

  def append( other: ArrayBase[Long] ): ArrayBase[Long]  = {
//    logger.debug( "Appending arrays: {o:(%s), s:(%s)} + {o:(%s), s:(%s)} ".format( origin.mkString(","), shape.mkString(","), other.origin.mkString(","), other.shape.mkString(",")))
    if( origin(0) < other.origin(0) ) {
      assert( origin(0) + shape(0) == other.origin(0), "Appending non-contiguous arrays" )
      HeapLongArray(toCDLongArray.append(other.toCDLongArray), origin, mergeMetadata("merge", other))
    } else {
      assert( other.origin(0) + other.shape(0) == origin(0), "Appending non-contiguous arrays" )
      HeapLongArray(other.toCDLongArray.append(toCDLongArray), other.origin, mergeMetadata("merge", other))
    }
  }
  def toXml: xml.Elem = <array shape={shape.mkString(",")} missing={getMissing().toString} > {_data_.mkString(",")} </array> % metadata
  def findValue( value: Long, match_required: Boolean = true ): Option[Int] = _data_.indexOf(value) match {
    case -1 => if(match_required) throw new Exception(s"Failed to find a match in array for value ${value}, array values = ${data.map(_.toString).mkString(",")}"); None
    case x => Some(x)
  }
}
object HeapLongArray {
  def apply( cdarray: CDLongArray, origin: Array[Int], metadata: Map[String,String] ): HeapLongArray = new HeapLongArray( cdarray.getShape, origin, cdarray.getArrayData(), Some(cdarray.getInvalid), metadata )
  def apply( ucarray: ucar.ma2.Array, origin: Array[Int], metadata: Map[String,String], missing: Float ): HeapLongArray = HeapLongArray( CDLongArray.factory(ucarray), origin, metadata )
}

//class CDTimeSlice(val elements: SortedMap[String,HeapFltArray], metadata: Map[String,String], val partition: Partition ) extends MetadataCarrier(metadata) {
//  def ++( other: CDTimeSlice ): CDTimeSlice = {
//    new CDTimeSlice( elements ++ other.elements, metadata ++ other.metadata, partition )
//  }
//  def release( keys: Iterable[String] ): CDTimeSlice  = {
//    val new_elements = elements.filterKeys( key => !keys.contains(key) )
//    new CDTimeSlice( new_elements, metadata, partition )
//  }
//  def clear: CDTimeSlice  = new CDTimeSlice( SortedMap.empty[String,HeapFltArray], metadata, partition )
//
//  def size: Long = elements.values.foldLeft(0L)( (size,elem) => size + elem.size )
//
//  def hasMultiGrids: Boolean = {
//    if( elements.size == 0 ) return false
//    val head_shape = elements.head._2.shape
//    elements.exists( item => !item._2.shape.sameElements(head_shape) )     // TODO: Compare axes as well?
//  }
//  def reinterp( conversionMap: Map[Int,TimeConversionSpec] ): CDTimeSlice = {
//    val new_elements = elements.mapValues( array => conversionMap.get(array.shape(0)) match {
//      case Some( conversionSpec ) => array.reinterp( conversionSpec.weights, conversionSpec.mapOrigin )
//      case None =>
//        if( array.shape(0) != conversionMap.values.head.toSize )  throw new Exception( s"Unexpected time conversion input size: ${array.shape(0)} vs ${conversionMap.values.head.toSize}" )
//        array
//    })
//    new CDTimeSlice( new_elements, metadata, partition )
//  }
//
//  def slice( startIndex: Int, size: Int ): CDTimeSlice = {
//    logger.info( s"RDDPartition: slice --> nElems:{${elements.size}} startIndex:{${startIndex}} size:{${size}} ")
//    val new_elems = elements.mapValues( _.slice(startIndex,size) )
//    new CDTimeSlice( new_elems, metadata, partition )
//  }
//  def section( optSection: Option[CDSection] ): CDTimeSlice = optSection match {
//    case Some( section ) =>
//      logger.info( s"CDTimeSlice-section[${section.toString()}]: elsems = ${elements.keys.mkString(", ")}")
//      val new_elements = elements.mapValues( _.section( section.toSection ) )
//      CDTimeSlice( new_elements, metadata, partition )
//    case None =>
//      this
//  }
//
//  def hasMultiTimeScales( trsOpt: Option[String]=None ): Boolean = {
//    if( elements.isEmpty ) return false
//    val ntimesteps = elements.values.head.shape(0)
//    elements.exists( item => !(item._2.shape(0)==ntimesteps) )
//  }
//  def append( other: CDTimeSlice ): CDTimeSlice = {
//    val commonElems = elements.keySet.union( other.elements.keySet )
//    val appendedElems: Set[(String,HeapFltArray)] = commonElems flatMap ( key =>
//      other.elements.get(key).fold  (elements.get(key) map (e => key -> e))  (e1 => Some( key-> elements.get(key).fold (e1) (e0 => e0.append(e1)))))
//    new CDTimeSlice( TreeMap(appendedElems.toSeq:_*), metadata ++ other.metadata, partition )
//  }
//  def extend( vSpec: DirectRDDVariableSpec ): CDTimeSlice = {
//    if (elements.contains(vSpec.uid)) {
//      this
//    } else {
//      val new_element = vSpec.toHeapArray(partition)
//      val newRec = new CDTimeSlice(elements + (vSpec.uid -> new_element), metadata ++ vSpec.metadata, partition)
////      print( s"\n ********* Extend with vSpec ${vSpec.uid}, elements = [ ${elements.keys.mkString(", ")} ], part=${partition.index}, result nelems = ${newRec.elements.size}\n\n" )
//      newRec
//    }
//  }
//
////  def split( index: Int ): (RDDPartition,RDDPartition) = { }
//  def getShape = elements.head._2.shape
//  def getOrigin = elements.head._2.origin
//  def elems = elements.keys
//  def element( id: String ): Option[HeapFltArray] = ( elements find { case (key,array) => key.split(':')(0).equals(id) } ) map ( _._2 )
//  def findElements( id: String ): Iterable[HeapFltArray] = ( elements filter { case (key,array) => key.split(':').last.equals(id) } ) values
//  def empty( id: String ) = { element(id).isEmpty }
//  def head: ( String, HeapFltArray ) = elements.head
//  def toXml: xml.Elem = {
//    val values: Iterable[xml.Node] = elements.values.map(_.toXml)
//    <partition> {values} </partition>  % metadata
//  }
//  def configure( key: String, value: String ): CDTimeSlice = new CDTimeSlice( elements, metadata + ( key -> value ), partition )
//}
//
//object CDTimeSlice {
//  def apply ( elements: SortedMap[String,HeapFltArray],  metadata: Map[String,String], partition: Partition ) = new CDTimeSlice( elements, metadata, partition )
//  def apply ( rdd: CDTimeSlice ) = new CDTimeSlice( rdd.elements, rdd.metadata, rdd.partition )
//  def merge( rdd_parts: Seq[CDTimeSlice] ) = rdd_parts.foldLeft( CDTimeSlice.empty )( _ ++ _ )
//  def empty: CDTimeSlice = { new CDTimeSlice( TreeMap.empty[String,HeapFltArray], Map.empty[String,String], RegularPartition.empty ) }
//}

//object RDDPartSpec {
//  def apply( partition: CachePartition, tgrid: TargetGrid, varSpecs: List[ RDDVariableSpec ] ): RDDPartSpec = new RDDPartSpec( partition, partition.getPartitionRecordKey(tgrid,"RDDPartSpec"), varSpecs )
//}
//
//class RDDPartSpec(val partition: CachePartition, val timeRange: RecordKey, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable with Loggable {
//
////  def getRDDPartition(kernelContext: KernelContext, batchIndex: Int): CDTimeSlice = {
////    val t0 = System.nanoTime()
////    val elements =  TreeMap( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toHeapArray(partition)) ): _* )
////    val rv = CDTimeSlice( elements, Map( "partRange" -> partition.partRange.toString), partition )
////    val dt = (System.nanoTime() - t0) / 1.0E9
////    logger.debug( "RDDPartSpec{ partition = %s }: completed data input in %.4f sec".format( partition.toString, dt) )
////    kernelContext.addTimestamp( "Created input RDD { partition = %s, batch = %d  } in %.4f sec".format( partition.toString, batchIndex, dt ) )
////    rv
////  }
//
////  def getRDDMetaPartition: CDTimeSlice = {
////    val t0 = System.nanoTime()
////    val elements =  TreeMap( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toMetaArray(partition)) ): _* )
////    val rv = CDTimeSlice( elements, Map.empty, partition )
////    logger.debug( "RDDPartSpec{ partition = %s }: completed data input in %.4f sec".format( partition.toString, (System.nanoTime() - t0) / 1.0E9) )
////    rv
////  }
//
//
////  def getPartitionKey( partitioner: RangePartitioner ): RecordKey = partitioner.newPartitionKey( timeRange.center )
//
//  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
//    case Some( varSpec ) => varSpec.empty
//    case None => true
//  }
//
//}

object DirectRDDPartSpec {
  def apply(partition: Partition, tgrid: TargetGrid, varSpecs: Iterable[ DirectRDDVariableSpec ] = Iterable.empty ): DirectRDDPartSpec = new DirectRDDPartSpec( partition, partition.getPartitionRecordKey(tgrid,"DirectRDDPartSpec"), varSpecs )
}

class DirectRDDPartSpec(val partition: Partition, val timeRange: RecordKey, val varSpecs: Iterable[ DirectRDDVariableSpec ] ) extends Serializable with Loggable {
  val dbgIndex = 0

  def getCDTimeSliceSpecs(): IndexedSeq[DirectCDTimeSliceSpec] =
    ( 0 until partition.nRecords ) map ( DirectCDTimeSliceSpec( this, _ ) )

  def index = partition.index

  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

object DirectCDTimeSliceSpec {
  def apply( partSpec: DirectRDDPartSpec, iRecord: Int ): DirectCDTimeSliceSpec = new DirectCDTimeSliceSpec( partSpec.partition, iRecord, partSpec.timeRange, partSpec.varSpecs )
}

class DirectCDTimeSliceSpec(val partition: Partition, iRecord: Int, val timeRange: RecordKey, val varSpecs: Iterable[ DirectRDDVariableSpec ] ) extends Serializable with Loggable {

//  def getRDDPartition( batchIndex: Int ): CDTimeSlice = {
//    val t0 = System.nanoTime()
//    val elements =  TreeMap( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toHeapArray(partition)) ).toSeq: _* )
//    val rv = CDTimeSlice( elements, Map( "partIndex" -> partition.index.toString, "startIndex" -> timeRange.elemStart.toString, "recIndex" -> iRecord.toString, "batchIndex" -> batchIndex.toString ), partition )
//    val dt = (System.nanoTime() - t0) / 1.0E9
//    logger.debug( "DirectCDTimeSliceSpec{ partition = %s, record = %d, nelems=%d }: completed data input in %.4f sec".format( partition.toString, iRecord, elements.size, dt) )
//    rv
//  }

  def index = partition.index
  override def toString() = s"RDD-Record[${iRecord.toString}]{ ${partition.toString}, ${timeRange.toString} }"

  def empty( uid: String ): Boolean = varSpecs.find( _.uid == uid ) match {
    case Some( varSpec ) => varSpec.empty
    case None => true
  }

}

//object ExtRDDPartSpec {
//  def apply(timeRange: RecordKey, varSpecs: List[ RDDVariableSpec ] ): ExtRDDPartSpec = new ExtRDDPartSpec( timeRange, varSpecs )
//}
//
//class ExtRDDPartSpec(val timeRange: RecordKey, val varSpecs: List[ RDDVariableSpec ] ) extends Serializable with Loggable {
//
//  def getRDDPartition(kernelContext: KernelContext, partition: Partition, batchIndex: Int): CDTimeSlice = {
//    val t0 = System.nanoTime()
//    val elements =  TreeMap( varSpecs.flatMap( vSpec => if(vSpec.empty) None else Some(vSpec.uid, vSpec.toMetaArray ) ): _* )
//    val rv = CDTimeSlice( elements, Map.empty, partition )
//    val dt = (System.nanoTime() - t0) / 1.0E9
//    logger.debug( "RDDPartSpec: completed data input in %.4f sec".format( dt) )
//    kernelContext.addTimestamp( "Created input RDD { varSpecs = (%s), batchIndex = %d } in %.4f sec".format( varSpecs.map(_.uid).mkString(","), batchIndex, dt ) )
//    rv
//  }
//
//}

class DirectRDDVariableSpec( uid: String, metadata: Map[String,String], missing: Float, section: CDSection, val varShortName: String, val collectionId: String  ) extends RDDVariableSpec( uid, metadata, missing, section  ) with Loggable {

  def getAggregation(): Aggregation = {
    val collection: Collection = Collections.findCollection(collectionId) getOrElse { throw new Exception( s"Can't find collection ${collectionId}") }
    collection.getAggregation(varShortName) getOrElse { throw new Exception( s"Can't find aggregation for variable ${varShortName} in collection ${collectionId}" ) }
  }
}

class RDDVariableSpec( val uid: String, val metadata: Map[String,String], val missing: Float, val section: CDSection  ) extends Serializable with Loggable {

  def toHeapArray( partition: CachePartition ) = {
    val rv = HeapFltArray( partition.dataSection(section, missing), section.getOrigin, metadata, None )
    logger.debug( "toHeapArray: %s, part[%d]: dim=%d, range=(%d:%d), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, partition.startIndex, partition.endIndex, partition.shape.mkString(",") ) )
    rv
  }


  def toMetaArray = {
    val rv = HeapFltArray( section.getShape, section.getOrigin, metadata )
    logger.debug( "toHeapArray: %s".format( section.toString() ) )
    rv
  }
  def toMetaArray( partition: Partition ) = {
    val rv = HeapFltArray( partition.partSection( section.toSection( partition.partitionOrigin ) ).getShape, section.getOrigin, metadata )
    logger.debug( "toHeapArray: %s, part[%d]: dim=%d, range=(%d:%d), shape=[%s]".format( section.toString(), partition.index, partition.dimIndex, partition.startIndex, partition.endIndex, partition.shape.mkString(",") ) )
    rv
  }
  def empty = section.getShape.contains(0)
  def rank = section.getShape.length
}

//class RDDRegen(val source: RDD[(PartitionKey,RDDPartition)], val sourceGrid: TargetGrid, val resultGrid: TargetGrid, node: WorkflowNode, kernelContext: KernelContext ) extends Loggable {
//  private val regen: Boolean = !sourceGrid.equals(resultGrid)
//  lazy val regridKernel = getRegridKernel
////  def getRDD(): RDD[(Int,RDDPartition)] = if(regen) {
////    source
////    node.map( source, kernelContext, regridKernel )
////  } else source
//
//  def getRegridKernel(): zmqPythonKernel = node.workflow.executionMgr.getKernel( "python.cdmsmodule", "regrid"  ) match {
//    case pyKernel: zmqPythonKernel => pyKernel
//    case x => throw new Exception( "Unexpected Kernel class for regrid module: " + x.getClass.getName)
//  }
//}



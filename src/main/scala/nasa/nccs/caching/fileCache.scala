package nasa.nccs.caching

import java.io._
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, PathMatcher, Paths}
import java.nio.{ByteBuffer, FloatBuffer, MappedByteBuffer}
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Comparator}

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, EntryWeigher, EvictionListener}
import nasa.nccs.caching.EDASPartitioner.{partitionSize, recordSize}
import nasa.nccs.edas.utilities.{GeoTools, appParameters, runtime}
import nasa.nccs.cdapi.cdm.{PartitionedFragment, _}
import nasa.nccs.cdapi.tensors.{CDByteArray, CDFloatArray}
import nasa.nccs.edas.engine.{EDASExecutionManager, WorkflowNode}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.TransientFragment
import nasa.nccs.edas.rdd.{CDTimeSlice, TimeSliceCollection}
import nasa.nccs.edas.sources.Masks
import nasa.nccs.esgf.process.{DataFragmentKey, _}
import nasa.nccs.esgf.wps.edasServiceProvider
import nasa.nccs.utilities.{Loggable, cdsutils}
import org.apache.commons.io.{FileUtils, IOUtils}
import ucar.ma2.Range
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.time.CalendarDate
import ucar.nc2.time.CalendarPeriod.Field.{Month, Year}
import ucar.nc2.units.TimeUnit
import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

object MaskKey {
  def apply(bounds: Array[Double],
            mask_shape: Array[Int],
            spatial_axis_indices: Array[Int]): MaskKey = {
    new MaskKey(bounds,
                Array(mask_shape(spatial_axis_indices(0)),
                      mask_shape(spatial_axis_indices(1))))
  }
}
class MaskKey(bounds: Array[Double], dimensions: Array[Int]) {}

class CacheChunk(val offset: Int,
                 val elemSize: Int,
                 val shape: Array[Int],
                 val buffer: ByteBuffer) {
  def size: Long = shape.foldLeft(1L)(_ * _)
  def data: Array[Byte] = buffer.array
  def byteSize: Long = size * elemSize
  def byteOffset = offset * elemSize
}

object BatchSpec extends Loggable {
  lazy val serverContext = edasServiceProvider.cds2ExecutionManager.serverContext
  lazy val nCores = appParameters( "parts.per.node", "1" ).toInt
  lazy val nNodes = appParameters( "num.cluster.nodes", "1" ).toInt
  lazy val nParts = appParameters( "num.partitions" ).fold( nCores * nNodes * 2 )( _.toInt )
  def apply( index: Int ): BatchSpec = { new BatchSpec( index*nParts, nParts ) }
}

case class BatchSpec( iStartPart: Int, nParts: Int ) {
  def included( part_index: Int ): Boolean = (part_index >= iStartPart ) && ( nParts > (part_index-iStartPart)  )
  override def toString(): String = "Batch{start: %d, size: %d}".format( iStartPart, nParts )
}

class CachePartitions( val id: String, private val _section: ma2.Section, val parts: Array[CachePartition]) extends Loggable {
  private val baseShape = _section.getShape
  def getShape = baseShape
  def getPart(partId: Int): CachePartition = parts(partId)
  def getPartData(partId: Int, missing_value: Float): CDFloatArray = parts(partId).data(missing_value)
  def roi: ma2.Section = new ma2.Section(_section.getRanges)
  def delete = parts.map(_.delete)

  def getBatch( batchIndex: Int ): Array[CachePartition] = {
    val batch = BatchSpec(batchIndex)
    val rv = parts.filter( p => batch.included(p.index) )
    logger.info( "Get [%d]%s, selection size = %d".format( batchIndex, batch.toString, rv.length ) )
    rv
  }
}

object Partitions {

}

class Partitions( private val _section: ma2.Section, val parts: Array[Partition]) {
  private val baseShape = _section.getShape
  def getShape = baseShape
  def getPart(partId: Int): Partition = parts(partId)
  def roi: ma2.Section = new ma2.Section(_section.getRanges)

  def getBatch( batchIndex: Int ): Array[Partition] = {
    val batch = BatchSpec(batchIndex)
    parts.filter( p => batch.included(p.index) )
  }

  def hasBatch( batchIndex: Int ): Boolean = {
    val batch = BatchSpec(batchIndex)
    parts.exists( p => batch.included(p.index) )
  }
}

object CachePartition {
  def apply(index: Int, path: String, dimIndex: Int, startIndex: Int, partSize: Int, start_date: Long, end_date: Long, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], fragShape: Array[Int]): CachePartition = {
    val partShape = getPartitionShape(partSize, fragShape)
    new CachePartition(index, path, dimIndex, startIndex, partSize, start_date, end_date, recordSize, sliceMemorySize, origin, partShape)
  }
  def apply(path: String, partition: RegularPartition ): CachePartition = {
    val partShape = getPartitionShape( partition.partSize, partition.shape )
    new CachePartition( partition.index, path, partition.dimIndex, partition.startIndex, partition.partSize, partition.start_time, partition.end_time, partition.recordSize, partition.sliceMemorySize, partition.origin, partShape )
  }
  def getPartitionShape(partSize: Int, fragShape: Array[Int]): Array[Int] = {
    var shape = fragShape.clone(); shape(0) = partSize; shape
  }
}

class CachePartition( index: Int, val path: String, dimIndex: Int, startIndex: Int, partSize: Int, start_time: Long, end_time: Long, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], shape: Array[Int]) extends RegularPartition(index, dimIndex, startIndex, partSize, start_time, end_time, recordSize, sliceMemorySize, origin, shape) {

  def data(missing_value: Float): CDFloatArray = {
    val file = new RandomAccessFile(path, "r")
    val channel: FileChannel = file.getChannel()
    logger.debug(s" *** Mapping channel for Partition-$index with partSize=$partSize startIndex=$startIndex, recordSize=$recordSize, sliceMemorySize=$sliceMemorySize, shape=(%s), path=%s".format( shape.mkString(","), path ))
//    if( index == 0 ) { logger.debug( "\n    " + Thread.currentThread().getStackTrace.mkString("\n    ")) }
    val buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, partSize * sliceMemorySize)
    channel.close(); file.close()
    runtime.printMemoryUsage(logger)
    new CDFloatArray(shape, buffer.asFloatBuffer, missing_value)
  }
  def delete = { FileUtils.deleteQuietly(new File(path)) }

  def dataSection(section: CDSection, missing_value: Float): CDFloatArray =
    try {
      val partData = data(missing_value)
      logger.info( " &&& PartSection: section = {s:%s||o:%s}, partOrigin = {%s}".format( section.getShape.mkString(","), section.getOrigin.mkString(","), partitionOrigin.mkString(",") ))
      val partSection = section.toSection( partitionOrigin )
      partData.section( partSection )
    } catch {
      case ex: AssertionError =>
        logger.error(" Error in dataSection, section origin = %s, shape = %s".format( section.getOrigin.mkString(","), section.getShape.mkString(",")) )
        throw ex
    }
}

abstract class Partition(val index: Int, val dimIndex: Int, val startIndex: Int, val partSize: Int, val start_time: Long, val end_time: Long, val sliceMemorySize: Long, val origin: Array[Int], val shape: Array[Int] ) extends Loggable with Serializable {
  val partitionOrigin: Array[Int] = origin.zipWithIndex map { case (value, ival) => if( ival== 0 ) value + startIndex else value }
  val endIndex: Int = startIndex + partSize - 1
  def start_date: CalendarDate = CalendarDate.of(start_time)
  def end_date: CalendarDate = CalendarDate.of(end_time)

  def recordSection( section: ma2.Section, iRecord: Int, timeAxis: CoordinateAxis1DTime, start_time: Long, end_time: Long ): ma2.Section = {
    val start_index = timeAxis.findTimeIndexFromCalendarDate(start_date)
    val time_resolution: Double = timeAxis.getTimeResolution.getValueInSeconds
    val time_range = end_date.getDifferenceInMsecs(start_date)/1000.0
    val section_size = Math.round(time_range/time_resolution).toInt
    val end_index = start_index + section_size - 1
    val rv = new ma2.Section(section.getRanges).replaceRange(0, new Range(start_index,end_index) )
    logger.info( " *** RecordSection[%d]: dim=%d, range=[ %d, %d ]: %s -> %s ".format(iRecord,0,start_index,end_index, section.toString, rv.toString ) )
    rv
  }
  def recordSection( section: ma2.Section, iRecord: Int ): ma2.Section = {
    val rec_range = recordRange(iRecord)
    val rv = new ma2.Section(section.getRanges).replaceRange(dimIndex, rec_range ).intersect(section)
    logger.info( " *** RecordSection[%d]: dim=%d, range=[ %d, %d ]: %s -> %s ".format(iRecord,dimIndex,rec_range.first,rec_range.last, section.toString, rv.toString ) )
    rv
  }
  override def toString() = s"Part[$index]{ start=${start_date.toString}, end=${end_date.toString} startIndex=$startIndex, size=$partSize, origin=(${origin.mkString(",")}), shape=(${shape.mkString(",")}) ]"

  def partSection(section: ma2.Section): ma2.Section = {
    new ma2.Section(section.getRanges).replaceRange(0, partRange)
  }
  def partRange: ma2.Range = { new ma2.Range( origin(0)+startIndex, origin(0)+endIndex) }

  def recordRange(iRecord: Int): ma2.Range
  def nRecords: Int
  def recordStartIndex(iRecord: Int): Int

  def getPartitionRecordKey(grid: TargetGrid, context: String ): RecordKey = {
    val start = origin(0)+startIndex
    val startDate = grid.getCalendarDate(start, context + "-start")
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( start+partSize, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end, context + "-end" )
    val endDateStr = endDate.toString
    val endTime =  endDate.getMillis/1000
    RecordKey( startTime, endTime, startIndex, partSize )
  }

  def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey

  def getRelativeSection(global_section: ma2.Section): ma2.Section = {
    val relative_ranges = for (ir <- global_section.getRanges.indices; r = global_section.getRange(ir)) yield {
      if (ir == dimIndex) { r.shiftOrigin(startIndex) } else r
    }
    new ma2.Section(relative_ranges)
  }
}

object RegularPartition {
  def apply(index: Int, dimIndex: Int, startIndex: Int, partSize: Int, start_date: Long, end_date: Long, recordSize: Int, sliceMemorySize: Long, origin: Array[Int], fragShape: Array[Int]): RegularPartition = {
    val partShape = getPartitionShape(partSize, fragShape)
    new RegularPartition( index, dimIndex, startIndex, partSize, start_date, end_date, recordSize, sliceMemorySize, origin, partShape )
  }
  def getPartitionShape(partSize: Int, fragShape: Array[Int]): Array[Int] = {
    var shape = fragShape.clone(); shape(0) = partSize; shape
  }
  val empty = new RegularPartition( -1, -1, -1, -1, -1, -1, -1, -1, Array.empty[Int], Array.empty[Int] )
}

class RegularPartition( index: Int,  dimIndex: Int,  startIndex: Int,  partSize: Int, start_date: Long, end_date: Long,  val recordSize: Int,  sliceMemorySize: Long,  origin: Array[Int],  shape: Array[Int]) extends Partition(index, dimIndex, startIndex, partSize, start_date, end_date, sliceMemorySize, origin, shape) {

  override val toString: String = s"Part[$index]{dim:$dimIndex start:$startIndex partSize:$partSize recordSize:$recordSize sliceMemorySize:$sliceMemorySize origin:${origin.mkString(",")} shape:${shape.mkString(",")})"
  override def nRecords: Int = math.ceil(partSize / recordSize.toDouble).toInt


  override def recordRange(iRecord: Int): ma2.Range = {
    val start = recordStartIndex(iRecord);
    new ma2.Range(start, origin(0)+Math.min(start + recordSize - 1, endIndex))
  }

  override def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey = {
    val start = recordStartIndex(iRecord)
    val startDate = grid.getCalendarDate(start,"RegularPartition-getRecordKey")
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( start+recordSize, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end,"RegularPartition-getRecordKey")
    val endDateStr = endDate.toString
    val endTime =  grid.getCalendarDate(end,"RegularPartition-getRecordKey").getMillis/1000
    RecordKey( startTime, endTime, startIndex, recordSize )
  }

  def recordStartIndex(iRecord: Int) = { origin(0) + iRecord * recordSize + startIndex }
  def recordIndexArray: IndexedSeq[Int] = (0 until nRecords)
}

class FilteredPartition(index: Int, dimIndex: Int, startIndex: Int, partSize: Int, start_date: Long, end_date: Long, sliceMemorySize: Long, origin: Array[Int], shape: Array[Int], val records: IndexedSeq[FilteredRecordSpec] ) extends Partition( index, dimIndex, startIndex, partSize, start_date, end_date, sliceMemorySize, origin, shape ) {
  override def recordStartIndex( iRecord: Int ) = records(iRecord).first
  override def recordRange(iRecord: Int): ma2.Range = {
    val start = recordStartIndex(iRecord);
    new ma2.Range( start, origin(0)+records(iRecord).last )
  }
  override def nRecords: Int = records.length
  override val toString: String = s"Part[$index]{dim:$dimIndex start:$startIndex partSize:$partSize sliceMemorySize:$sliceMemorySize origin:${origin.mkString(",")} shape:${shape.mkString(",")} )"

  override def getRecordKey( iRecord: Int, grid: TargetGrid ): RecordKey = {
    val record = records(iRecord)
    val startDate = grid.getCalendarDate(record.first,"FilteredPartition-getRecordKey")
    val startDateStr = startDate.toString
    val startTime = startDate.getMillis/1000
    val end = Math.min( record.last+1, grid.shape(0)-1 )
    val endDate = grid.getCalendarDate(end,"FilteredPartition-getRecordKey")
    val endDateStr = endDate.toString
    val endTime =  grid.getCalendarDate(end,"FilteredPartition-getRecordKey").getMillis/1000
    RecordKey( startTime, endTime, record.first, record.length )
  }
}

import ucar.nc2.time.{CalendarDate, CalendarDateRange, CalendarPeriod}

object TimePeriod {
  object RelativePosition extends Enumeration { val Before, After, FirstHalf, SecondHalf = Value }
  def apply(  first: CalendarDate, last: CalendarDate ): TimePeriod = new TimePeriod( CalendarDateRange.of(first,last) )
}

class TimePeriod( val dateRange: CalendarDateRange ){
  import TimePeriod._
  val durationMS = (dateRange.getEnd.getMillis - dateRange.getStart.getMillis).toDouble

  def relativePosition( date: CalendarDate ): RelativePosition.Value = {
    if( date.isBefore( dateRange.getStart ) ) { RelativePosition.Before }
    else if( date.isBefore( dateRange.getEnd ) ) { RelativePosition.After }
    else {
      val pos = ( date.getMillis - dateRange.getStart.getMillis ) / durationMS
      if( pos < 0.5 ) RelativePosition.FirstHalf else  RelativePosition.SecondHalf
    }
    RelativePosition.Before
  }

}


//class CacheFileReader( val datasetFile: String, val varName: String, val sectionOpt: Option[ma2.Section] = None, val cacheType: String = "fragment" ) extends XmlResource {
//  private val netcdfDataset = NetcdfDataset.openDataset( datasetFile )
// private val ncVariable = netcdfDataset.findVariable(varName)

object EDASPartitioner {
  implicit def int2String(x: Int): String = x.toString
  val M = 1024 * 1024
  val maxRecordSize = 200*M
  val defaultRecordSize = 200*M
  val defaultPartSize = 1000*M
  val secPerDay: Float = ( 60 * 60 * 24 ).toFloat
  val secPeryear: Float  = ( secPerDay * 356 )
  val secPerMonth: Float = secPerMonth / 12
  val recordSize: Float = math.min( cdsutils.parseMemsize( appParameters( "record.size", defaultRecordSize ) ), maxRecordSize ).toFloat
  val partitionSize: Float = math.max( cdsutils.parseMemsize( appParameters( "partition.size", defaultPartSize) ), recordSize )
  val maxProductSize: Float = cdsutils.parseMemsize( appParameters( "max.product.size", "1g" ) ).toFloat
  val maxInputSize: Float = cdsutils.parseMemsize( appParameters( "max.input.size", "1g" ) ).toFloat
  val nCoresPerPart = 1
}

class EDASCachePartitioner( uid: String,  val cache_id: String, _section: ma2.Section, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode], timeAxisOpt: Option[CoordinateAxis1DTime], numDataFiles: Int, numElem: Int, regridSpec: RegridSpec, dataType: ma2.DataType = ma2.DataType.FLOAT, cacheType: String = "fragment")
  extends EDASPartitioner(uid,_section,partsConfig,timeAxisOpt,numDataFiles,numElem,regridSpec,dataType,cacheType) {

  def getCacheFilePath(partIndex: Int): String = {
    val cache_file = cache_id + "-" + partIndex.toString
    DiskCacheFileMgr.getDiskCacheFilePath(cacheType, cache_file)
  }

  override def getPartition(partIndex: Int): CachePartition = CachePartition( getCacheFilePath(partIndex), spec.getParition(partIndex).asInstanceOf[RegularPartition] )
  def getCachePartitions: Array[CachePartition] = (0 until spec.getNPartitions).map(getPartition(_)).toArray
}

object SeasonFilter {
  val months = "JFMAMJJASONDJFMAMJJASOND"
  def get(filterSpec: String): Option[SeasonFilter] = months.indexOfSlice(filterSpec) match {
    case -1 => None
    case index => Some(new SeasonFilter( (index until (index+filterSpec.length)) map ( i => ( ((i-1)%12)+1, (i-1)/12 ) ) ) )
  }
}

class SeasonFilter( val months: IndexedSeq[(Int,Int)] ) extends Loggable {
  val length = months.length
  private val _records = new java.util.TreeMap[Int,FilteredRecordSpec]()
  def pass( iMonth: Int ): Option[(Int,Int)] = months.find( _._1 == iMonth )
  def getRecordSpec( iYear: Int ): FilteredRecordSpec = _records.getOrElseUpdate( iYear, new FilteredRecordSpec )
  def getRecords = _records.values()
  def getPartitionRecords( startIndex: Int, partSize: Int ) = _records.values().slice( startIndex, startIndex+partSize ).filter( _.length == length )
  def getNRecords = _records.size()
  def processTimestep( timeIndex: Int, date: CalendarDate ): Unit = {
    val iYear = date.getFieldValue(Year)
    val iMonth = date.getFieldValue(Month)
    pass(iMonth) match {
      case Some( (iMonth, iYearOffset) ) => getRecordSpec(iYear-iYearOffset).add( timeIndex )
      case None => Unit
    }
  }
}

class FilteredRecordSpec {
  var first: Int = Int.MaxValue
  var last: Int = -1
  def add( timeIndex: Int ): Unit = {
    first = Math.min( first, timeIndex )
    last = Math.max( last, timeIndex )
  }
  def length = last - first + 1
  override def toString = s"FilteredRecordSpec( first:$first last:$last length:$length )"
}

class EDASPartitionSpec( val _partitions: IndexedSeq[Partition] ) {
  def getParition( index: Int ): Partition = { _partitions(index) }
  def getNPartitions: Int = _partitions.length
}

class PartitionSpecs( val nPartitions: Int, val nSlicesPerRecord: Int ) { }

class CustomPartitionSpecs( nPartitions: Int, val parts: Array[ma2.Range], nSlicesPerRecord: Int ) extends PartitionSpecs(nPartitions,nSlicesPerRecord) { }

class RegularPartitionSpecs( nPartitions: Int, val partMemorySize: Long, nSlicesPerRecord: Int, val recordMemorySize: Long, val nRecordsPerPart: Int, val nSlicesPerPart: Int ) extends PartitionSpecs(nPartitions,nSlicesPerRecord) { }

case class PartitionConstraints( partsConfig: Map[String,String] ) {
  val numDataFiles: Int = partsConfig.getOrElse("numDataFiles","0").toInt
  val numParts: Int = partsConfig.getOrElse("numParts","0").toInt
  val period = partsConfig.getOrElse("period","")
  val nSlicesPerRecord: Int = partsConfig.getOrElse("numSlicesPerRecord","0").toInt
  val oneRecPerSlice: Boolean = partsConfig.getOrElse("oneRecPerSlice","false").toBoolean
//  val numParts: Int = if( _numParts > 0 ) { _numParts } else {
//    val _numfilesPerPart: Double = math.ceil( _numDataFiles / BatchSpec.nParts.toFloat )
//    if ( _numfilesPerPart == 0.0 ) 0 else math.ceil( _numDataFiles / _numfilesPerPart ).toInt
//  }
  override val toString = { s"{ numDataFiles: ${numDataFiles.toString}, numParts: ${numParts.toString}, period: ${period}, nSlicesPerRecord: ${nSlicesPerRecord.toString}, oneRecPerSlice: ${oneRecPerSlice.toString} }"}
}

class EDASPartitioner( val uid: String, private val _section: ma2.Section, val partsConfig: Map[String,String], timeAxisOpt: Option[CoordinateAxis1DTime], val numDataFiles: Int, val numElements: Int, val regridSpec: RegridSpec, dataType: ma2.DataType = ma2.DataType.FLOAT, val cacheType: String = "fragment") extends Loggable {
  import EDASPartitioner._

  val elemSize = dataType.getSize
  val baseShape = _section.getShape
  val sectionMemorySize = getMemorySize()
  val sliceMemorySize: Long = getMemorySize(1)
  val filters = partsConfig.getOrElse("filter","").split(",").filter( !_.isEmpty )
  val sectionRange: ma2.Range = _section.getRange(0)
  lazy val spec = computeRecordSizes()
  lazy val partitions = new Partitions( _section, getPartitions )
  def nPartitions = partitions.parts.size

  def timeAxis: CoordinateAxis1DTime = {
    val fullAxis = timeAxisOpt getOrElse( throw new Exception( "Missing time axis in Partitioner") )
    fullAxis
  }
  def getCalDateBounds( time_index: Int ): Array[CalendarDate] = timeAxis.getCoordBoundsDate(time_index)
  def getCalDate( time_index: Int ): CalendarDate = timeAxis.getCalendarDate( time_index )
  def getBoundsStartDate( time_index: Int ): CalendarDate = {
    val date0 = timeAxis.getCalendarDate(time_index)
    logger.info( "getBoundsStartDate: index = " + time_index.toString + ", data[index] = " + date0.toString )
    getCalDateBounds(time_index)(0)
  }
  def getBoundsEndDate( time_index: Int ): CalendarDate =  getCalDateBounds( time_index )(1)

//  val sectionCalendarRange: CalendarDateRange = CalendarDateRange.of( getBoundsStartDate( sectionRange.first ), getBoundsEndDate( sectionRange.last ) )
//  val secondsPerSection: Long = sectionCalendarRange.getDurationInSecs
//  val monthsPerSection: Float = secondsPerSection / secPerMonth
//  val daysPerSection: Float = secondsPerSection / secPerDay
//  val resolutionInDays: Float = daysPerSection / baseShape(0)
//  val monthMemorySize: Float = sectionMemorySize / monthsPerSection
//  val slicesPerDay: Float = daysPerSection / baseShape(0)
//  val yearsPerSection: Float = secondsPerSection / secPeryear

  def getPeriodRanges( periodValues: IndexedSeq[Int] ): Array[Range] = {
    var periodValue = -1
    var partStartIndex = -1
    var ranges = ListBuffer.empty[ma2.Range]
    periodValues.zipWithIndex foreach { case ( iP, index ) => if( iP != periodValue ) {
      if( partStartIndex != -1 ) { ranges += new Range( partStartIndex, index-1 ) }
      partStartIndex = index
      periodValue = iP
    }}
    ranges.toArray
  }

  def getPartitionSpecs( constraints: PartitionConstraints ): PartitionSpecs = {
    if( constraints.period.isEmpty ) {
      val timeAxisCoverageFraction = sectionRange.length() / timeAxis.getShape(0).toFloat
      val nFiles = constraints.numDataFiles * timeAxisCoverageFraction
      val desiredPartSize = if (2 > nFiles) { partitionSize } else { math.min( partitionSize, sectionMemorySize / math.min( nFiles, BatchSpec.nNodes ) ).toInt }
      logger.info(  s"\n--> %E% getPartitionSpecs: timeAxisCoverageFraction=${timeAxisCoverageFraction}, nFiles=${nFiles}, nNodes=${BatchSpec.nNodes}, sectionMemorySize=${sectionMemorySize}, desiredPartSize=${desiredPartSize}, section.len=${sectionRange.length()}, timeAxis.len=${timeAxis.getShape(0)}")
      val currentPartitionSize: Float = math.max(desiredPartSize, sliceMemorySize)
      val currentRecordSize: Float = if (constraints.oneRecPerSlice) { sliceMemorySize } else {
        if (constraints.nSlicesPerRecord == 0) {
          math.min(recordSize, currentPartitionSize)
        } else {
          constraints.nSlicesPerRecord * sliceMemorySize
        }
      }
      val _nSlicesPerRecord: Int = math.max( currentRecordSize / sliceMemorySize, 1.0).round.toInt
      val _recordMemorySize: Long = getMemorySize(_nSlicesPerRecord)
      val _nRecordsPerPart: Int = math.max(currentPartitionSize / _recordMemorySize, 1.0).round.toInt
      val _partMemorySize: Long = _nRecordsPerPart * _recordMemorySize
      var _nSlicesPerPart: Int = _nRecordsPerPart * _nSlicesPerRecord
      val _nPartitions: Int = math.ceil(sectionMemorySize / _partMemorySize.toFloat).toInt
      new RegularPartitionSpecs(_nPartitions, _partMemorySize, _nSlicesPerRecord, _recordMemorySize, _nRecordsPerPart, _nSlicesPerPart)
    } else {
      if ( constraints.period.toLowerCase.startsWith("month") ) {
        val months: IndexedSeq[Int] = timeAxis.section(sectionRange).getCalendarDates.map( _.getFieldValue(CalendarPeriod.Field.Month) ).toIndexedSeq
        val parts: Array[Range] = getPeriodRanges( months )
        val _nSlicesPerRecord: Int = if (constraints.nSlicesPerRecord == 0) { math.max( recordSize/sliceMemorySize, 1.0).round.toInt } else { constraints.nSlicesPerRecord }
//        logger.info( s"Generating custom parts, period=${constraints.period}, nparts=${parts.length.toString}, parts = [ ${parts.map(_.toString).mkString(", ")} ]")
        new CustomPartitionSpecs( parts.length, parts, _nSlicesPerRecord )
      } else {
        throw new Exception( "Unrecognized partition period: " + constraints.period )
      }
    }
  }

  def getForceNParts(): Int = {
    val forceNParts = partsConfig.getOrElse("numParts","0").toInt
    if( forceNParts > 0 ) { forceNParts }
    else {
      forceNParts
//      val minNumSmallParts = partsConfig.getOrElse("minNumSmallParts","10").toInt
//      val minNParts = math.min( minNumSmallParts, numDataFiles )
//      if( minNParts * partitionSize > sectionMemorySize ) { minNParts }     // TODO: debug this enhancement.
//      else { 0 }
    }
  }

  def computeRecordSizes( ): EDASPartitionSpec = {
    val sparkConfig = BatchSpec.serverContext.spark.sparkContext.getConf.getAll map { case (key, value ) =>  key + " -> " + value } mkString( "\n\t")
    if( filters.isEmpty ) {
      val constraints = PartitionConstraints( partsConfig ++ Map( "numDataFiles" -> numDataFiles.toString ) )
      val time_section_start_index = _section.getOrigin(0)
      val partitions: IndexedSeq[Partition] = getPartitionSpecs( constraints ) match {
        case pSpecs: RegularPartitionSpecs => {
          val parts = (0 until pSpecs.nPartitions) map (partIndex => {
            val relStartIndex = partIndex * pSpecs.nSlicesPerPart
            val partSize = Math.min (pSpecs.nSlicesPerPart, baseShape(0) - relStartIndex )
            val start_date: Long = timeAxis.getCalendarDate( time_section_start_index + relStartIndex ).getMillis
            val ts_ms = timeAxis.getTimeResolution.getValueInSeconds * 1000.0
            val end_date: Long = ( start_date + partSize * ts_ms ).toLong
            RegularPartition(partIndex, 0, relStartIndex, partSize, start_date, end_date, pSpecs.nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape)
          })
          logger.info(  s"\n---------------------------------------------\n %E% Generating regular batched partitions: numDataFiles: ${numDataFiles}, sectionMemorySize: ${sectionMemorySize/M.toFloat} M, maxInputSize: ${EDASPartitioner.maxInputSize/M.toFloat} M, sliceMemorySize: ${sliceMemorySize/M.toFloat} M, nSlicesPerRecord: ${pSpecs.nSlicesPerRecord}, recordMemorySize: ${pSpecs.recordMemorySize/M.toFloat} M, nRecordsPerPart: ${pSpecs.nRecordsPerPart}, partMemorySize: ${pSpecs.partMemorySize/M.toFloat} M, nPartitions: ${parts.length}, constraints: ${constraints.toString} \n---------------------------------------------\n")
          parts
        }
        case cpSpecs: CustomPartitionSpecs => {
          val parts = for (partIndex <- 0 until cpSpecs.nPartitions; pRange = cpSpecs.parts(partIndex)) yield {
            val nSlicesPerRec = math.min(cpSpecs.nSlicesPerRecord, pRange.length)
            val start_date: Long = timeAxis.getCalendarDate(pRange.first).getMillis
            val end_date: Long = timeAxis.getCalendarDate(pRange.first+pRange.length).getMillis
            RegularPartition(partIndex, 0, pRange.first, pRange.length, start_date, end_date, nSlicesPerRec, sliceMemorySize, _section.getOrigin, baseShape)
          }
          logger.info(  s"\n---------------------------------------------\n ~~~~ Generating custom batched partitions: numDataFiles: ${numDataFiles}, sectionMemorySize: ${sectionMemorySize/M.toFloat} M, sliceMemorySize: ${sliceMemorySize/M.toFloat} M, nSlicesPerRecord: ${cpSpecs.nSlicesPerRecord}, nPartitions: ${parts.length}, constraints: ${constraints.toString} \n---------------------------------------------\n")
          parts
        }
        case x => throw new Exception( "Unrecognized partition class: " + x.getClass.getName )
      }
//      logger.info(  s"\n---------------------------------------------\n %P% PARTITIONS: \n\t:${partitions.map( p => s"PART[${p.index}]: start: ${p.start_time} end: ${p.end_time}").mkString("\n\t")}" )
      new EDASPartitionSpec( partitions )
    } else {
      val seasonFilters = filters.flatMap( SeasonFilter.get )
      if( seasonFilters.length < filters.length ) throw new Exception ( "Unrecognized filter: " + filters.mkString(",") )
      val timeSteps: List[CalendarDate] = timeAxis.section(sectionRange).getCalendarDates.toList
      for( (timeStep, timeIndex) <- timeSteps.zipWithIndex; seasonFilter <- seasonFilters ) { seasonFilter.processTimestep( timeIndex, timeStep  ) }
      val all_records = for( seasonFilter <- seasonFilters; record <- seasonFilter.getRecords ) yield { record }
      val partitions: IndexedSeq[Partition] = if( all_records.length <= BatchSpec.nParts ) {
        for( ( record, partIndex ) <- all_records.zipWithIndex ) yield {
          val start_date: Long = timeAxis.getCalendarDate(record.first).getMillis
          val end_date: Long = timeAxis.getCalendarDate(record.first+record.length).getMillis
          new FilteredPartition(partIndex, 0, record.first, record.length, start_date, end_date, sliceMemorySize, _section.getOrigin, baseShape, Array(record))
        }
      } else {
        val nRecs = seasonFilters(0).getNRecords
        val seasonRecsPerPartition = Math.ceil( nRecs / BatchSpec.nParts ).toInt
        val nParts = Math.ceil( nRecs / seasonRecsPerPartition.toFloat ).toInt
        val partSize = Math.ceil( baseShape(0)/nParts.toFloat ).toInt
        ( 0 until nParts ) map ( partIndex => {
          val iRecStart = partIndex*seasonRecsPerPartition
          val records = seasonFilters flatMap ( _.getPartitionRecords(iRecStart,seasonRecsPerPartition) )
          val start_date: Long = timeAxis.getCalendarDate(records.head.first).getMillis
          val end_date: Long = timeAxis.getCalendarDate(records.head.first+partSize).getMillis
          new FilteredPartition(partIndex, 0, records.head.first, partSize, start_date, end_date, sliceMemorySize, _section.getOrigin, baseShape, records )
        } )
      }
      logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions for ${BatchSpec.nParts} procs: \n ${partitions.map( _.toString ).mkString( "\n\t" )}  \n---------------------------------------------\n")
      new EDASPartitionSpec( partitions )
    }
  }

//  def computeRecordSizes1( ): EDASPartitionSpec = {
//    val sparkConfig = BatchSpec.serverContext.spark.sparkContext.getConf.getAll map { case (key, value ) =>  key + " -> " + value } mkString( "\n\t")
//    val _preferredNParts = math.ceil( sectionMemorySize / partitionSize.toFloat ).toInt
//    if( _preferredNParts > BatchSpec.nProcessors * 1.5 ) {
//      val _nSlicesPerRecord: Int = math.max(recordSize.toFloat / sliceMemorySize, 1.0).round.toInt
//      val _recordMemorySize: Long = getMemorySize(_nSlicesPerRecord)
//      val _nRecordsPerPart: Int = math.max(partitionSize.toFloat / _recordMemorySize, 1.0).round.toInt
//      val _partMemorySize: Long = _nRecordsPerPart * _recordMemorySize
//      val _nSlicesPerPart: Int = _nRecordsPerPart * _nSlicesPerRecord
//      val _nPartitions: Int = math.ceil(sectionMemorySize / _partMemorySize.toFloat).toInt
//      val partitions = (0 until _nPartitions) map ( partIndex => {
//        val startIndex = partIndex * _nSlicesPerPart
//        val partSize = Math.min(_nSlicesPerPart, baseShape(0) - startIndex)
//        RegularPartition(partIndex, 0, startIndex, partSize, _nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape)
//      })
//      logger.info(  s"\n---------------------------------------------\n ~~~~ Generating batched partitions: preferredNParts: ${_preferredNParts}, sectionMemorySize: $sectionMemorySize, sliceMemorySize: $sliceMemorySize, nSlicesPerRecord: ${_nSlicesPerRecord}, recordMemorySize: ${_recordMemorySize}, nRecordsPerPart: ${_nRecordsPerPart}, partMemorySize: ${_partMemorySize}, nPartitions: ${partitions.length} \n---------------------------------------------\n")
//      new EDASPartitionSpec( partitions )
//    } else {
//      if( filters.isEmpty ) {
//        val __nPartitions: Int = BatchSpec.nProcessors
//        val __nSlicesPerPart: Int = math.ceil(baseShape(0) / __nPartitions.toFloat).toInt
//        val __maxSlicesPerRecord: Int = math.max(math.round(recordSize / sliceMemorySize.toFloat), 1)
//        val __nSlicesPerRecord: Int = math.min( __maxSlicesPerRecord, __nSlicesPerPart )
//        val __nRecordsPerPart: Int = math.ceil(__nSlicesPerPart / __nSlicesPerRecord.toFloat).toInt
//        val __recordMemorySize: Long = getMemorySize(__nSlicesPerRecord)
//        val __partMemorySize: Long = __recordMemorySize * __nRecordsPerPart
//        val partitions = (0 until __nPartitions) flatMap ( partIndex => {
//          val startIndex = partIndex * __nSlicesPerPart
//          val partSize = Math.min(__nSlicesPerPart, baseShape(0) - startIndex)
//          if(partSize>0) { Some( RegularPartition(partIndex, 0, startIndex, partSize, __nSlicesPerRecord, sliceMemorySize, _section.getOrigin, baseShape ) ) } else { None }
//        })
//        logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions: nAvailCores: ${__nPartitions}, sectionMemorySize: $sectionMemorySize, sliceMemorySize: $sliceMemorySize, nSlicesPerRecord: ${__nSlicesPerRecord}, recordMemorySize: ${__recordMemorySize}, nRecordsPerPart: ${__nRecordsPerPart}, partMemorySize: ${__partMemorySize}, nPartitions: ${partitions.length} \n---------------------------------------------\n")
//        new EDASPartitionSpec( partitions )
//      } else {
//        val seasonFilters = filters.flatMap( SeasonFilter.get )
//        if( seasonFilters.length < filters.length ) throw new Exception ( "Unrecognized filter: " + filters.mkString(",") )
//        val timeSteps: List[CalendarDate] = timeAxis.section(sectionRange).getCalendarDates.toList
//        for( (timeStep, timeIndex) <- timeSteps.zipWithIndex; seasonFilter <- seasonFilters ) { seasonFilter.processTimestep( timeIndex, timeStep  ) }
//        val all_records = for( seasonFilter <- seasonFilters; record <- seasonFilter.getRecords ) yield { record }
//        val partitions: IndexedSeq[Partition] = if( all_records.length < BatchSpec.nProcessors ) {
//          for( ( record, partIndex ) <- all_records.zipWithIndex ) yield {
//            new FilteredPartition(partIndex, 0, record.first, record.length, sliceMemorySize, _section.getOrigin, baseShape, Array(record))
//          }
//        } else {
//          val nRecs = seasonFilters(0).getNRecords
//          val seasonRecsPerPartition = Math.ceil( nRecs / ( BatchSpec.nProcessors - 1f ) ).toInt
//          val nParts = Math.ceil( nRecs / seasonRecsPerPartition.toFloat ).toInt
//          val partSize = Math.ceil( baseShape(0)/nParts.toFloat ).toInt
//          ( 0 until nParts ) map ( partIndex => {
//            val iRecStart = partIndex*seasonRecsPerPartition
//            val records = seasonFilters flatMap ( _.getPartitionRecords(iRecStart,seasonRecsPerPartition) )
//            new FilteredPartition(partIndex, 0, records.head.first, partSize, sliceMemorySize, _section.getOrigin, baseShape, records )
//          } )
//        }
//        logger.info(  s"\n---------------------------------------------\n ~~~~ Generating partitions for ${BatchSpec.nProcessors} procs: \n ${partitions.map( _.toString ).mkString( "\n\t" )}  \n---------------------------------------------\n")
//        new EDASPartitionSpec( partitions )
//      }
//    }
//  }

  def getShape = baseShape
  def roi: ma2.Section = new ma2.Section(_section.getRanges)

  def getPartition(partIndex: Int): Partition = spec.getParition( partIndex )

  def getPartitions: Array[Partition] = ( 0 until spec.getNPartitions ).map( getPartition ).toArray
  def getMemorySize(nSlices: Int = -1): Long = {
    var full_shape = baseShape.clone()
    if (nSlices > 0) { full_shape(0) = nSlices }
    val memorySize = full_shape.foldLeft(4L)(_ * _)  * numElements
    if( memorySize > EDASPartitioner.maxInputSize ) {
      throw new Exception( s"Must be authorized to execute a request this large (request input size = ${memorySize.toFloat}, max unauthorized input size = ${EDASPartitioner.maxInputSize})")
    }
    memorySize
  }

//  def computePartitions = {
//      if (resolutionInDays > 1.0) {
//          val daysPerChunk = daysPerSection / nChunksPerSection
//          val slicesPerChunk = Math.floor( slicesPerDay * daysPerChunk )
//
//          } else {
//          val monthMemSize = // getMemorySize(356f/12f)
//          val chunksPerMonth = nChunksPerSection / monthsPerSection
//          if (chunksPerMonth < 1.0) {
//              1
//            } else {
//              val nParts = 1
//            }
//        }
//    }

  private def getPeriodRanges( period: CalendarPeriod.Field ): Map[Int,TimePeriod] = {
    var currentPeriod = -1
    var startDate: CalendarDate = CalendarDate.present()
    var currDate: CalendarDate = startDate
    val periodRanges = scala.collection.mutable.HashMap.empty[Int,TimePeriod]
    timeAxis.getCalendarDates.toList.zipWithIndex foreach { case ( date, index ) =>
      val periodIndex = date.getFieldValue( period )
      if( currentPeriod < 0 ) { startDate = date }
      else if( periodIndex != currentPeriod ) {
        periodRanges.put( periodIndex, TimePeriod( startDate, currDate ) )
        startDate = date
      }
      currDate = date
      currentPeriod = periodIndex
    }
    periodRanges.toMap
  }

  private def getNearestMonthBoundary( time_index: Int  ): Int = {
    val startDate = getCalDate( time_index )
    val dom = startDate.getDayOfMonth()
    val month = if( dom > 15 ) startDate
    time_index
  }
}

class FileToCacheStream(val fragmentSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode], val maskOpt: Option[CDByteArray], val cacheType: String = "fragment") extends Loggable {
  val attributes = fragmentSpec.getVariableMetadata
  val _section = fragmentSpec.roi
  val missing_value: Float = getAttributeValue("missing_value", "") match { case "" => Float.MaxValue; case x => x.toFloat }
  protected val baseShape = _section.getShape
  val cacheId = "a" + System.nanoTime.toHexString
  val sType = getAttributeValue("dtype", "FLOAT")
  val dType = ma2.DataType.getType( sType )
  def roi: ma2.Section = new ma2.Section(_section.getRanges)
  val partitioner = new EDASCachePartitioner(fragmentSpec.uid, cacheId, roi, partsConfig, workflowNodeOpt, fragmentSpec.getTimeCoordinateAxis, fragmentSpec.numDataFiles, 1, RegridSpec(fragmentSpec), dType )
  def getAttributeValue(key: String, default_value: String) =
    attributes.get(key) match {
      case Some(attr_val) => attr_val.toString.split('=').last.replace('"',' ').trim
      case None => default_value
    }

  def getReadBuffer(cache_id: String): (FileChannel, MappedByteBuffer) = {
    val channel = new FileInputStream(cache_id).getChannel
    val size = math.min(channel.size, Int.MaxValue).toInt
    ( channel, channel.map(FileChannel.MapMode.READ_ONLY, 0, size))
  }

  def cacheFloatData: CachePartitions = {
    assert(dType == ma2.DataType.FLOAT,  "Attempting to cache %s data as float".format(dType.toString))
    execute(missing_value)
  }

  def execute(missing_value: Float): CachePartitions = {
    val t0 = System.nanoTime()
    val future_partitions: IndexedSeq[Future[CachePartition]] = for ( pIndices <- ( 0 until partitioner.spec.getNPartitions ) ) yield Future { processChunkedPartitions(cacheId, pIndices, missing_value) }
    val partitions: Array[CachePartition] = Await.result(Future.sequence(future_partitions.toList), Duration.Inf).toArray
    logger.info("\n ********** Completed Cache Op, total time = %.3f min  ********** \n".format((System.nanoTime() - t0) / 6.0E10))
    new CachePartitions( cacheId, roi, partitions)
  }

  def processChunkedPartitions(cache_id: String, partIndex: Int, missing_value: Float): CachePartition = {
    logger.info( "Process Chunked Partitions(%s): %d".format( cache_id, partIndex ) )
    val partition: CachePartition = partitioner.getPartition(partIndex);
    val outStr = new BufferedOutputStream( new FileOutputStream(new File(partition.path)))
    cachePartition(partition, outStr)
    outStr.close
    partition
  }

  def cacheRecord(partition: RegularPartition, iRecord: Int, outStr: BufferedOutputStream) = {
    logger.info( "CacheRecord: part=%d, record=%d".format(partition.index, iRecord))
    val subsection: ma2.Section = partition.recordSection(roi,iRecord)
    val t0 = System.nanoTime()
    logger.info( " ---> Reading data record %d, part %d, startTimIndex = %d, shape [%s], subsection [%s:%s], nElems = %d ".format(iRecord, partition.index, partition.startIndex, getAttributeValue("shape", ""), subsection.getOrigin.mkString(","), subsection.getShape.mkString(","), subsection.getShape.foldLeft(1L)(_ * _)))
    val data = fragmentSpec.readData( subsection )
    val chunkShape = data.getShape
    val dataBuffer = data.getDataAsByteBuffer
    val t1 = System.nanoTime()
    logger.info( "Finished Reading data record %d, shape = [%s], buffer capacity = %.2f M in time %.2f ".format(iRecord, chunkShape.mkString(","), dataBuffer.capacity() / 1.0E6, (t1 - t0) / 1.0E9))
    val t2 = System.nanoTime()
    IOUtils.write(dataBuffer.array(), outStr)
    val t3 = System.nanoTime()
    logger.info( " -----> Writing record %d, write time = %.3f " .format(iRecord, (t3 - t2) / 1.0E9))
    val t4 = System.nanoTime()
    logger.info( s"Persisted record %d, write time = %.2f " .format(iRecord, (t4 - t3) / 1.0E9))
    runtime.printMemoryUsage(logger)
  }

  def cachePartition(partition: RegularPartition, stream: BufferedOutputStream) = {
    logger.info( "Caching Partition(%d): chunk start indices: (%s), roi: %s".format( partition.index, partition.recordIndexArray.map(partition.recordStartIndex).mkString(","), baseShape.mkString(",")))
    for (iRecord <- partition.recordIndexArray; startLoc = partition.recordStartIndex(iRecord); if startLoc <= _section.getRange(0).last())
      yield cacheRecord(partition, iRecord, stream)
  }
}

object FragmentPersistence extends DiskCachable with FragSpecKeySet {
  private val fragmentIdCache: Cache[String, String] = new PersistentCache("CacheIdMap", "fragment" )
  val M = 1000000

  def getCacheType = "fragment"
  def keys: Set[String] = fragmentIdCache.keys
  def keyObjects: Set[String] = fragmentIdCache.keys
  def values: Iterable[Future[String]] = fragmentIdCache.values

//  def persist(fragSpec: DataFragmentSpec, frag: PartitionedFragment): Future[String] = {
//    val keyStr =  fragSpec.getKey.toStrRep
//    fragmentIdCache.get(keyStr) match {
//      case Some(fragIdFut) => fragIdFut
//      case None => fragmentIdCache(keyStr) {
//        val fragIdFut = promiseCacheId(frag) _
//        fragmentIdCache.persist()
//        fragIdFut
//      }
//    }
//  }

  def expandKey(fragKey: String): String = {
    val toks = fragKey.split('|')
    "variable= %s; origin= (%s); shape= (%s); coll= %s; maxParts=%s".format(toks(0), toks(2), toks(3), toks(1), toks(4))
  }

  def expandKeyXml(fragKey: String): xml.Elem = {
    val toks = fragKey.split('|')
    <fragment variable={toks(0)} origin={toks(2)} shape={toks(3)} coll={toks(1)} maxParts={toks(4)}/> // { getBounds(fragKey) } </fragment>
  }

  def contractKey(fragDescription: String): String = {
    val tok = fragDescription
      .split(';')
      .map(_.split('=')(1).trim.stripPrefix("(").stripSuffix(")"))
    Array(tok(0), tok(3), tok(1), tok(2), tok(4)).mkString("|")
  }

  def fragKeyLT(fragKey1: String, fragKey2: String): Boolean = {
    val toks1 = fragKey1.split('|')
    val toks2 = fragKey2.split('|')
    (toks1(1) + toks1(0)) < (toks2(1) + toks2(0))
  }

  def getFragmentListXml(): xml.Elem =
    <fragments> { for(fkey <- fragmentIdCache.keys.toIndexedSeq.sortWith(fragKeyLT) ) yield expandKeyXml(fkey) } </fragments>
  def getFragmentIdList(): Array[String] = fragmentIdCache.keys.toArray
  def getFragmentList(): Array[String] =
    fragmentIdCache.keys.map(k => expandKey(k)).toArray
  def put(key: DataFragmentKey, cache_id: String) = {
    fragmentIdCache.put(key.toStrRep, cache_id); fragmentIdCache.persist()
  }
  def getEntries: Seq[(String, String)] = fragmentIdCache.getEntries

  def close(): Unit =
    Await.result(Future.sequence(fragmentIdCache.values), Duration.Inf)

  def clearCache(): Set[String] = fragmentIdCache.clear()
  def deleteEnclosing(fragSpec: DataFragmentSpec) = delete(findEnclosingFragSpecs(fragmentIdCache.keys, fragSpec.getKey))
  def findEnclosingFragmentData(fragSpec: DataFragmentSpec): Option[String] = findEnclosingFragSpecs(fragmentIdCache.keys, fragSpec.getKey).headOption

  def delete( fragKeys: Iterable[String] ) = {
    for (fragKey <- fragKeys; cacheFragKey <- fragmentIdCache.keys; if cacheFragKey.startsWith(fragKey); cache_id_future = fragmentIdCache.get(cacheFragKey).get) {
      val path = DiskCacheFileMgr.getDiskCacheFilePath(getCacheType, Await.result(cache_id_future, Duration.Inf))
      fragmentIdCache.remove(cacheFragKey)
      val matcher: java.nio.file.PathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + path + "*")
      val fileFilter: java.io.FileFilter = new FileFilter() { override def accept(pathname: File): Boolean = {matcher.matches(pathname.toPath) } }
      val parent = new File(path).getParentFile
      for (file <- parent.listFiles(fileFilter)) {
        if (file.delete) logger.info(s"Deleting persisted fragment file " + file.getAbsolutePath + ", frag: " + cacheFragKey)
        else logger.warn(s"Failed to delete persisted fragment file " + file.getAbsolutePath)
      }
    }
    fragmentIdCache.persist()
  }
}

trait FragSpecKeySet extends nasa.nccs.utilities.Loggable {

  def getFragSpecsForVariable(keys: Set[String], collection: String, varName: String): Set[DataFragmentKey] =
    keys.filter( _ match {
        case fkey: String =>
          DataFragmentKey(fkey).sameVariable(collection, varName)
        case x =>
          logger.warn("Unexpected fragment key type: " + x.getClass.getName);
          false
      }).map(_ match { case fkey: String => DataFragmentKey(fkey) })

  def findEnclosingFragSpecs(keys: Set[String],
                             fkey: DataFragmentKey,
                             admitEquality: Boolean = true): List[String] = {
    val variableFrags = getFragSpecsForVariable(keys, fkey.collId, fkey.varname)
    variableFrags
      .filter(fkeyParent => fkeyParent.contains(fkey, admitEquality))
      .toList
      .sortWith(_.getSize < _.getSize)
      .map(_.toStrRep)
  }

  def findEnclosedFragSpecs(keys: Set[String],
                            fkeyParent: DataFragmentKey,
                            admitEquality: Boolean = false): Set[String] = {
    val variableFrags =
      getFragSpecsForVariable(keys, fkeyParent.collId, fkeyParent.varname)
    logger.info(
      "Searching variable frags: \n\t --> " + variableFrags.mkString(
        "\n\t --> "))
    variableFrags
      .filter(fkey => fkeyParent.contains(fkey, admitEquality))
      .map(_.toStrRep)
  }

  def findEnclosingFragSpec(keys: Set[String],
                            fkeyChild: DataFragmentKey,
                            selectionCriteria: FragmentSelectionCriteria.Value,
                            admitEquality: Boolean = true): Option[String] = {
    val enclosingFragments =
      findEnclosingFragSpecs(keys, fkeyChild, admitEquality)
    if (enclosingFragments.isEmpty) None
    else
      Some(selectionCriteria match {
        case FragmentSelectionCriteria.Smallest =>
          enclosingFragments.minBy(DataFragmentKey(_).getRoi.computeSize())
        case FragmentSelectionCriteria.Largest =>
          enclosingFragments.maxBy(DataFragmentKey(_).getRoi.computeSize())
      })
  }
}

class JobRecord(val id: String) {
  override def toString: String = s"ExecutionRecord[id=$id]"
  val creationTime: Long = System.nanoTime()
  def toXml: xml.Elem = <job id={id} elapsed={elapsed.toString} />
  def elapsed: Int = ((System.nanoTime() - creationTime)/1.0e9).toInt
}

class RDDTransientVariable(val result: TimeSliceCollection,
                           val operation: OperationContext,
                           val request: RequestContext) {
  val timeFormatter = new SimpleDateFormat("MM/dd HH:mm:ss")
  def timestamp = Calendar.getInstance().getTime
  def getTimestamp = timeFormatter.format(timestamp)
}

class TransientDataCacheMgr extends Loggable {
  private val transientFragmentCache: Cache[String, TransientFragment] = new FutureCache("Store", "result" )

  def putResult(resultId: String,
                resultFut: Future[Option[TransientFragment]]) =
    resultFut.onSuccess {
      case resultOpt =>
        resultOpt.map(result => transientFragmentCache.put(resultId, result))
    }

  def getResultListXml(): xml.Elem =
    <results> { for( rkey <- transientFragmentCache.keys ) yield <result type="fragment" id={rkey} /> } </results>
  def getResultIdList = transientFragmentCache.keys

  def deleteResult(resultId: String): Option[Future[TransientFragment]] =
    transientFragmentCache.remove(resultId)
  def getExistingResult(resultId: String): Option[Future[TransientFragment]] = {
    val result: Option[Future[TransientFragment]] =
      transientFragmentCache.get(resultId)
    logger.info(
      ">>>>>>>>>>>>>>>> Get result from cache: search key = " + resultId + ", existing keys = " + transientFragmentCache.keys.toArray
        .mkString("[", ",", "]") + ", Success = " + result.isDefined.toString)
    result
  }
}
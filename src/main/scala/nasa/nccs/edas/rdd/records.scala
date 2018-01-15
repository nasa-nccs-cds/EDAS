package nasa.nccs.edas.rdd

import nasa.nccs.edas.engine.TestProcess
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.sources.{Aggregation, FileInput}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.esgf.process.{CDSection, DataContainer, DomainContainer, TaskRequest}
import nasa.nccs.utilities.{EDASLogManager, Loggable}
import nasa.nccs.wps.WPSMergedEventReport
import org.apache.spark.rdd.RDD
import ucar.ma2
import ucar.ma2.IndexIterator
import ucar.nc2.Variable
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.time.CalendarDate

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

// case class ArraySpec(  )
// case class ArraySpecs( arrays: scala.collection.Map[String,ArraySpec]  )

case class ArraySpec( missing: Float, shape: Array[Int], data: Array[Float] )
case class CDTimeSlice( timestamp: java.sql.Timestamp, arrays: scala.collection.Map[String,ArraySpec] ) {

  def ave: (Float, Int, Float) = {
    val t0 = System.nanoTime()
    val arraySpec = arrays.head._2
    val fltArray = arraySpec.data
    val ma2Array =  ma2.Array.factory( ma2.DataType.FLOAT, Array( fltArray.length ), fltArray )
    val rank = ma2Array.getRank
    val iter: IndexIterator = ma2Array.getIndexIterator()
    var result = 0f
    var count = 0
    var result_shape = Array.fill[Int](rank)(1)
    while ( iter.hasNext ) {
      val fval = iter.getFloatNext
      if( ( fval != arraySpec.missing ) && !fval.isNaN ) {
        result = result + fval
        count = count + 1
      }
    }
    val t1 = System.nanoTime()
    ( result, count, (t1-t0)/1.0E9f )
  }
}

object TimeSliceMultiIterator {
  def apply( varId: String, varName: String, section: String, tslice: String ) ( files: Iterator[FileInput] ): TimeSliceMultiIterator = {
    new TimeSliceMultiIterator( varId, varName, section, tslice, files )
  }
}

class TimeSliceMultiIterator( val varId: String, val varName: String, val section: String, val tslice: String, val files: Iterator[FileInput]) extends Iterator[CDTimeSlice] with Loggable {
  private var _optSliceIterator: Iterator[CDTimeSlice] = if( files.hasNext ) { getSliceIterator( files.next() ) } else { Iterator.empty }
  private def getSliceIterator( fileInput: FileInput ): TimeSliceIterator = new TimeSliceIterator( varId, varName, section, tslice, fileInput )
  val t0 = System.nanoTime()

  def hasNext: Boolean = { !( _optSliceIterator.isEmpty && files.isEmpty ) }

  def next(): CDTimeSlice = {
    if( _optSliceIterator.isEmpty ) { _optSliceIterator = getSliceIterator( files.next() ) }
    val result = _optSliceIterator.next()
    result
  }
}

class TimeSliceIterator( val varId: String, val varName: String, val section: String, val tslice: String, val fileInput: FileInput ) extends Iterator[CDTimeSlice] with Loggable {
  import ucar.nc2.time.CalendarPeriod.Field._
  private var _dateStack = new mutable.ArrayStack[(CalendarDate,Int)]()
  private var _sliceStack = new mutable.ArrayStack[CDTimeSlice]()
  val millisPerMin = 1000*60
  val filePath: String = fileInput.path
  _sliceStack ++= getSlices

  def hasNext: Boolean = _sliceStack.nonEmpty

  def next(): CDTimeSlice =  _sliceStack.pop

  private def getSlices: List[CDTimeSlice] = {
    def getSliceRanges( section: ma2.Section, slice_index: Int ): java.util.List[ma2.Range] = { section.getRanges.zipWithIndex map { case (range: ma2.Range, index: Int) => if( index == 0 ) { new ma2.Range("time",slice_index,slice_index)} else { range } } }
    val optSection: Option[ma2.Section] = CDSection.fromString(section).map(_.toSection)
    val path = fileInput.path
    val t0 = System.nanoTime()
    val dataset = NetcdfDatasetMgr.aquireFile( path, 77.toString )
    val variable: Variable = Option( dataset.findVariable( varName ) ).getOrElse { throw new Exception(s"Can't find variable $varName in data file ${path}") }
    val global_shape = variable.getShape()
    val metadata = variable.getAttributes.map(_.toString).mkString(", ")
    val missing: Float = variable.findAttributeIgnoreCase("fmissing_value").getNumericValue.floatValue()
    val varSection = variable.getShapeAsSection
    val interSect: ma2.Section = optSection.fold( varSection )( _.intersect(varSection) )
    val timeAxis: CoordinateAxis1DTime = ( NetcdfDatasetMgr.getTimeAxis( dataset ) getOrElse { throw new Exception(s"Can't find time axis in data file ${path}") } ).section( interSect.getRange(0) )
    val dates: List[CalendarDate] = timeAxis.getCalendarDates.toList
    assert( dates.length == variable.getShape()(0), s"Data shape mismatch getting slices for var $varName in file ${path}: sub-axis len = ${dates.length}, data array outer dim = ${variable.getShape()(0)}" )
    val t1 = System.nanoTime()
    val slices: List[CDTimeSlice] =  dates.zipWithIndex map { case (date: CalendarDate, slice_index: Int) =>
      val data_section = variable.read(getSliceRanges( interSect, slice_index))
      val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
      val data_shape: Array[Int] = data_section.getShape
      val arraySpec = ArraySpec( missing, data_section.getShape, data_array )
      //      (timestamp/millisPerMin).toInt -> CDTimeSlice( timestamp, missing, data_section )  // new java.sql.Timestamp( date.getMillis )
      CDTimeSlice( new java.sql.Timestamp( date.getMillis ), Map( varId -> arraySpec ) )  //
    }
    dataset.close()
    if( fileInput.index % 500 == 0 ) {
      val sample_array =  slices.head.arrays.head._2.data
      val datasize: Int = sample_array.length
      val dataSample = sample_array(datasize/2)
      logger.info(s"Executing TimeSliceIterator.getSlices, fileInput = ${fileInput.path}, datasize = ${datasize.toString}, dataSample = ${dataSample.toString}, prep time = ${(t1 - t0) / 1.0E9} sec, preFetch time = ${(System.nanoTime() - t1) / 1.0E9} sec\n\t metadata = $metadata")
    }
    slices
  }
}




class TestDatasetProcess( id: String ) extends TestProcess( id ) with Loggable {
  def execute( sc: CDSparkContext, jobId: String, optRequest: Option[TaskRequest]=None, run_args: Map[String, String]=Map.empty ): WPSMergedEventReport= {
    import sc.session.implicits._
    val nNodes = 18
    val usedCoresPerNode = 8
    val t00 = System.nanoTime()
    //    val dataFile = "/dass/adm/edas/cache/collections/NCML/merra2_inst1_2d_asm_Nx-MERRA2.inst1.2d.asm.Nx.nc4.ncml"
    val dataFile = "/dass/adm/edas/cache/collections/NCML/merra2_m2i1nxint-MERRA2.inst1.2d.int.Nx.nc4.ag1.csv"
    val varName1 = "KE"
    val varId1 = "v1"
    val varName2 = "THV"
    val varId2 = "v2"
    //    val dataFile = "/dass/adm/edas/cache/collections/NCML/cip_merra_mth-tas.ncml"
    //    val varName = "tas"
    val inputVar: DataContainer = optRequest.map( _.variableMap.head._2 ).getOrElse( throw new Exception("Missing input"))
    logger.info( "Starting read test")
    //    val dataFile = "/Users/tpmaxwel/.edas/cache/collections/NCML/merra_daily.ncml"

    val section = ""
    //    val dataset = NetcdfDataset.openDataset(dataFile)

    val t01 = System.nanoTime()
    val agg = Aggregation.read( dataFile )
    val t02 = System.nanoTime()
    val files: List[FileInput] = agg.files
    val t03 = System.nanoTime()
    val config = optRequest.fold(Map.empty[String,String])( _.operations.head.getConfiguration )
    val domains = optRequest.fold(Map.empty[String,DomainContainer])( _.domainMap )
    val nPartitions: Int = config.get( "parts" ).fold( nNodes * usedCoresPerNode ) (_.toInt)
    val mode = config.getOrElse( "mode", "rdd" )
    val missing = Float.NaN
    val tslice = config.getOrElse( "tslice", "prefetch" )
    val t04 = System.nanoTime()
    val prepTimes = Seq( (t04-t03), (t03-t02), (t02-t01), (t01-t00) ).map( _ / 1.0E9 )
    val parallelism = Math.min( files.length, nPartitions )
    val filesDataset: RDD[FileInput] = sc.sparkContext.parallelize( files, parallelism )
    filesDataset.count()
    val t1 = System.nanoTime()
    val timesliceRDD: RDD[CDTimeSlice] = filesDataset.mapPartitions( TimeSliceMultiIterator( varId1, varName1, section, tslice ) )
    if( mode.equals("count") ) { timesliceRDD.count() }
    else if( mode.equals("ave") ) {
      val (vsum,n,tsum) = timesliceRDD.map( _.ave ).treeReduce( ( x0, x1 ) => ( (x0._1 + x1._1), (x0._2 + x1._2),  (x0._3 + x1._3)) )
      logger.info(s"\n ****** Ave = ${vsum/n}, ctime = ${tsum/n} \n\n" )
    } else if( mode.equals("double")  ) {
      val timesliceRDD1: RDD[CDTimeSlice] = filesDataset.mapPartitions( TimeSliceMultiIterator( varId2, varName2, section, tslice ) )
      timesliceRDD.cache().count()
      timesliceRDD1.cache().count()
    } else if( mode.equals("doublekey")  ) {
      val timesliceRDD1: RDD[CDTimeSlice] = filesDataset.mapPartitions( TimeSliceMultiIterator( varId2, varName2, section, tslice ) )
      timesliceRDD.keyBy( _.timestamp.getNanos ).cache().count()
      timesliceRDD1.keyBy( _.timestamp.getNanos ).cache().count()
    } else if( mode.equals("merge")  ) {
      val timesliceRDD1: RDD[CDTimeSlice] = filesDataset.mapPartitions( TimeSliceMultiIterator( varId2, varName2, section, tslice ) )
      val tm0 = System.nanoTime()
      timesliceRDD.keyBy( _.timestamp.getNanos ).cache().count()
      timesliceRDD1.keyBy( _.timestamp.getNanos ).cache().count()
      val tm1 = System.nanoTime()
      val mergedRDD = timesliceRDD.keyBy( _.timestamp.getNanos ).cogroup( timesliceRDD1.keyBy( _.timestamp.getNanos ) )
      mergedRDD.count()
      val tm2 = System.nanoTime()
      logger.info(s"\n\nCompleted MERGE test, read time = ${(tm1 - tm0) / 1.0E9} sec, merge time = ${(tm2 - tm1) / 1.0E9} sec, total time = ${(tm2 - tm0) / 1.0E9} sec\n")
    } else {
      throw new Exception( "Unrecognized mode: " + mode )
    }
    val t2 = System.nanoTime()
    val nParts = timesliceRDD.getNumPartitions
    logger.info(s"\n\nCompleted test, nFiles = ${files.length}, prep times = [${prepTimes.mkString(", ")}], parallization time = ${(t1 - t04) / 1.0E9} sec, input time = ${(t2 - t1) / 1.0E9} sec, total time = ${(t2 - t00) / 1.0E9} sec, nParts = ${nParts}, filesPerPart = ${files.length / nParts.toFloat}\n\n")

    new WPSMergedEventReport( Seq.empty )
  }
}
object TestDatasetApplication extends Loggable {
  def main(args: Array[String]) {
    EDASLogManager.isMaster
    val valtest = new TestDatasetProcess("test")
    valtest.execute( CDSparkContext(), "test" )
  }

  def getRows( file: String ) = {}
}

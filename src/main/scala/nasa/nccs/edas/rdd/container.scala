package nasa.nccs.edas.rdd

import nasa.nccs.cdapi.data.DirectRDDVariableSpec
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.kernels.{Kernel, KernelContext}
import nasa.nccs.edas.sources.{Aggregation, FileBase, FileInput}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.utilities.Loggable
import org.apache.spark.rdd.RDD
import ucar.ma2
import ucar.nc2.Variable
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.time.CalendarDate

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable

case class ArraySpec( missing: Float, shape: Array[Int], data: Array[Float] )

case class CDTimeSlice( timestamp: Long, dt: Int, arrays: Map[String,ArraySpec1] ) {
  def ++( other: CDTimeSlice ): CDTimeSlice = { new CDTimeSlice( timestamp, dt, arrays ++ other.arrays ) }
//  def validate_identity( other_index: Int ): Unit = assert ( other_index == index , s"TimeSlice index mismatch: ${index} vs ${other_index}" )
  def clear: CDTimeSlice = { new CDTimeSlice( timestamp, dt, Map.empty[String,ArraySpec1] ) }
}

class TimeSliceRDD( val rdd: RDD[CDTimeSlice], val metadata: Map[String,String] ) {
  def getParameter( key: String, default: String ="" ): String = metadata.getOrElse( key, default )
}

object RDDExtensionGenerator {
  def apply() = new RDDExtensionGenerator()
}

class RDDExtensionGenerator {
  private var _optCurrentGenerator: Option[TimeSliceGenerator] = None

  def getGenerator( varId: String, varName: String, section: String, fileInput: FileInput  ): TimeSliceGenerator = {
    if( _optCurrentGenerator.isEmpty || _optCurrentGenerator.get.fileInput.startTime != fileInput.startTime ) {
      if( _optCurrentGenerator.isDefined ) { _optCurrentGenerator.get.close }
      _optCurrentGenerator = Some( new TimeSliceGenerator(varId, varName, section, fileInput) )
    }
    _optCurrentGenerator.get
  }

  def extendPartition( existingSlices: Iterator[CDTimeSlice], fileBase: FileBase, varId: String, varName: String, section: String ): Iterator[CDTimeSlice] = {
    existingSlices map { tSlice =>
      val fileInput = fileBase.getFileInput( tSlice.timestamp )
      val generator = getGenerator( varId, varName, section, fileInput )
      val newSlice = generator.getSlice( tSlice.timestamp )
      tSlice ++ newSlice
    }
  }
}

class RDDGenerator( val sc: CDSparkContext, val nPartitions: Int) {


  def parallelize( agg: Aggregation, varId: String, varName: String, section: String ): TimeSliceRDD = {
    val parallelism = Math.min( agg.files.length, nPartitions )
    val filesDataset: RDD[FileInput] = sc.sparkContext.parallelize( agg.files, parallelism )
    val rdd = filesDataset.mapPartitions( TimeSliceMultiIterator( varId, varName, section ) )
    val variable = agg.findVariable( varName ).getOrElse { throw new Exception(s"Unrecognozed variable ${varName} in aggregation, vars = ${agg.variables.map(_.name).mkString(",")}")}
    val metadata = Map( "section" -> section, varId -> variable.toString )
    new TimeSliceRDD( rdd, metadata )
  }



  def parallelize( template: TimeSliceRDD, agg: Aggregation, varId: String, varName: String ): TimeSliceRDD = {
    val variable = agg.findVariable( varName )
    val section = template.getParameter( "section" )
    val rdd = template.rdd.mapPartitions( tSlices => RDDExtensionGenerator().extendPartition( tSlices, agg.getFilebase, varId, varName, section ) )
    val metadata = Map( "section" -> section, varId -> variable.toString )
    new TimeSliceRDD( rdd, metadata )
  }
}

object TimeSliceMultiIterator {
  def apply( varId: String, varName: String, section: String ) ( files: Iterator[FileInput] ): TimeSliceMultiIterator = {
    new TimeSliceMultiIterator( varId, varName, section, files )
  }
}

class TimeSliceMultiIterator( val varId: String, val varName: String, val section: String, val files: Iterator[FileInput]) extends Iterator[CDTimeSlice] with Loggable {
  private var _optSliceIterator: Iterator[CDTimeSlice] = if( files.hasNext ) { getSliceIterator( files.next() ) } else { Iterator.empty }
  private def getSliceIterator( fileInput: FileInput ): TimeSliceIterator = new TimeSliceIterator( varId, varName, section,  fileInput )
  val t0 = System.nanoTime()

  def hasNext: Boolean = { !( _optSliceIterator.isEmpty && files.isEmpty ) }

  def next(): CDTimeSlice = {
    if( _optSliceIterator.isEmpty ) { _optSliceIterator = getSliceIterator( files.next() ) }
    val result = _optSliceIterator.next()
    result
  }
}

class TimeSliceIterator(val varId: String, val varName: String, val section: String, val fileInput: FileInput ) extends Iterator[CDTimeSlice] with Loggable {
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
    val dt: Int = Math.round( ( dates.last.getMillis - dates.head.getMillis ) / ( dates.length - 1 ).toFloat )
    val slices: List[CDTimeSlice] =  dates.zipWithIndex map { case (date: CalendarDate, slice_index: Int) =>
      val data_section = variable.read(getSliceRanges( interSect, slice_index))
      val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
      val data_shape: Array[Int] = data_section.getShape
      val arraySpec = ArraySpec1( missing, data_section.getShape, data_array )
      //      (timestamp/millisPerMin).toInt -> CDTimeSlice( timestamp, missing, data_section )  // new java.sql.Timestamp( date.getMillis )
      CDTimeSlice( date.getMillis, dt, Map( varId -> arraySpec ) )  //
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

class DatesBase( val dates: List[CalendarDate] ) extends Loggable with Serializable {
  val nDates = dates.length
  val dt: Float = ( dates.last.getMillis - dates.head.getMillis ) / ( nDates - 1 ).toFloat
  val startTime = dates.head.getMillis
  def getIndexEstimate( timestamp: Long ): Int = Math.round( ( timestamp - startTime ) / dt )
  def getDateIndex( timestamp: Long ): Int = _getDateIndex( timestamp, getIndexEstimate(timestamp) )

  private def _getDateIndex( timestamp: Long, indexEstimate: Int ): Int = {
    if( indexEstimate < 0 ) { return 0 }
    val datesStartTime = dates( indexEstimate ).getMillis
    if( timestamp < datesStartTime ) { return _getDateIndex(timestamp,indexEstimate-1) }
    if( indexEstimate >= nDates-1) { return nDates-1 }
    val datesEndTime = dates( indexEstimate+1 ).getMillis
    if( timestamp < datesEndTime ) { return  indexEstimate  }
    return _getDateIndex( timestamp, indexEstimate + 1)
  }
}

class TimeSliceGenerator(val varId: String, val varName: String, val section: String, val fileInput: FileInput ) extends Serializable with Loggable {
  import ucar.nc2.time.CalendarPeriod.Field._
  val millisPerMin = 1000*60
  val filePath: String = fileInput.path
  val optSection: Option[ma2.Section] = CDSection.fromString(section).map(_.toSection)
  val dataset = NetcdfDatasetMgr.aquireFile( filePath, 77.toString )
  val variable: Variable = Option( dataset.findVariable( varName ) ).getOrElse { throw new Exception(s"Can't find variable $varName in data file ${filePath}") }
  val global_shape = variable.getShape()
  val metadata = variable.getAttributes.map(_.toString).mkString(", ")
  val missing: Float = variable.findAttributeIgnoreCase("fmissing_value").getNumericValue.floatValue()
  val varSection = variable.getShapeAsSection
  val interSect: ma2.Section = optSection.fold( varSection )( _.intersect(varSection) )
  val timeAxis: CoordinateAxis1DTime = ( NetcdfDatasetMgr.getTimeAxis( dataset ) getOrElse { throw new Exception(s"Can't find time axis in data file ${filePath}") } ).section( interSect.getRange(0) )
  val dates: List[CalendarDate] = timeAxis.getCalendarDates.toList
  val datesBase: DatesBase = new DatesBase( dates )
  assert( dates.length == variable.getShape()(0), s"Data shape mismatch getting slices for var $varName in file ${filePath}: sub-axis len = ${dates.length}, data array outer dim = ${variable.getShape()(0)}" )
  val dt: Int = Math.round( ( dates.last.getMillis - dates.head.getMillis ) / ( dates.length - 1 ).toFloat )
  def close = dataset.close()
  def getSliceIndex( timestamp: Long ): Int = datesBase.getDateIndex( timestamp )

  def getSlice( timestamp: Long ): CDTimeSlice = {
    def getSliceRanges( section: ma2.Section, slice_index: Int ): java.util.List[ma2.Range] = { section.getRanges.zipWithIndex map { case (range: ma2.Range, index: Int) => if( index == 0 ) { new ma2.Range("time",slice_index,slice_index)} else { range } } }
    val data_section = variable.read(getSliceRanges( interSect, getSliceIndex(timestamp)))
    val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
    val data_shape: Array[Int] = data_section.getShape
    val arraySpec = ArraySpec1( missing, data_section.getShape, data_array )
    CDTimeSlice( timestamp, dt, Map( varId -> arraySpec ) )  //
  }
}




//class RDDContainer( init_value: RDD[CDTimeSlice] ) extends Loggable {
//  private val _contents = mutable.HashSet.empty[String]
//  private var _vault = new RDDVault(init_value)
//  def releaseBatch = { _vault.clear; _contents.clear() }
//
//  class RDDVault( init_value: RDD[CDTimeSlice] ) {
//    private var _rdd: RDD[CDTimeSlice] = init_value; _rdd.cache()
//    private def _update( new_rdd: RDD[CDTimeSlice] ): Unit = { _rdd.unpersist(false); _rdd = new_rdd; _rdd.cache }
//    private def _map( f: (RDD[CDTimeSlice]) => RDD[CDTimeSlice] ): Unit = update( f(_rdd) )
//    def update( f: (CDTimeSlice) => CDTimeSlice ): Unit = update( _rdd.map( rec => rec ++ f(rec) ) )
//    def mapValues( f: (CDTimeSlice) => CDTimeSlice ): Unit = update( _rdd.mapValues( rec => f(rec) ) )
//    def +=( record: CDTimeSlice ): Unit = update( _rdd.mapValues( rec => rec ++ record ) )
//    def value = _rdd
//    def fetchContents: Set[String] = _rdd.map { case (key,rec) => rec.elements.keySet }.first
//    def release( keys: Iterable[String] ): Unit = mapValues( _.release(keys) )
//    def clear: Unit = map( _.clear )
//  }
//  def map( kernel: Kernel, context: KernelContext ): Unit = {
//    _vault.updateValues( rec => kernel.postRDDOp( kernel.map(context)(rec), context ) )
//    _contents ++= _vault.fetchContents
//  }
//  def mapReduce( node: Kernel, context: KernelContext, batchIndex: Int ): CDTimeSlice = node.mapReduce( value, context, batchIndex )
//  def execute( workflow: Workflow, node: Kernel, context: KernelContext, batchIndex: Int ): CDTimeSlice = node.execute( workflow, value, context, batchIndex )
//  def value: RDD[CDTimeSlice] = _vault.value
//  def contents: Set[String] = _contents.toSet
//  def section( section: Option[CDSection] ): Unit = _vault.mapValues( _.section(section) )
//  def fetchContents: Set[String] = _vault.fetchContents
//  def release( keys: Iterable[String] ): Unit = { _vault.release( keys ); _contents --= keys.toSet }
//
//  def addFileInputs( kernelContext: KernelContext, vSpecs: List[DirectRDDVariableSpec] ): Unit = {
//    logger.info("\n\n RDDContainer ###-> BEGIN addFileInputs: operation %s, VarSpecs: [ %s ], contents = [ %s ] --> expected: [ %s ]   -------\n".format( kernelContext.operation.name, vSpecs.map( _.uid ).mkString(", "), _vault.fetchContents.mkString(", "), contents.mkString(", ") ) )
//    val newVSpecs = vSpecs.filter( vspec => !_contents.contains(vspec.uid) )
//    if( newVSpecs.nonEmpty ) {
//      _vault.mapValues( kernelContext.addRddElements(newVSpecs) )
//      _contents ++= newVSpecs.map(_.uid).toSet
//    }
//    logger.info("\n\n RDDContainer ###-> END addFileInputs: operation %s, VarSpecs: [ %s ], contents = [ %s ] --> expected: [ %s ]   -------\n".format( kernelContext.operation.name, vSpecs.map( _.uid ).mkString(", "), _vault.fetchContents.mkString(", "), contents.mkString(", ") ) )
//  }
//  def addOperationInput( record: CDTimeSlice ): Unit = {
//    _vault += record
//    _contents ++= record.arrays.keySet
//  }
//}

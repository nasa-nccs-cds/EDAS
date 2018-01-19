package nasa.nccs.edas.rdd

import nasa.nccs.caching.BatchSpec
import nasa.nccs.cdapi.data.{DirectRDDVariableSpec, FastMaskedArray, HeapFltArray}
import org.apache.commons.lang.ArrayUtils
import nasa.nccs.cdapi.tensors.{CDArray, CDFloatArray}
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.{CDSparkContext, RecordKey}
import nasa.nccs.edas.kernels.{Kernel, KernelContext}
import nasa.nccs.edas.sources.{Aggregation, FileBase, FileInput}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.edas.workers.TransVar
import nasa.nccs.esgf.process.{CDSection, ServerContext}
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

object ArraySpec {
  def apply( tvar: TransVar ) = {
    val data_array =  tvar.getFloatArray
    new ArraySpec( data_array.last, tvar.getShape, tvar.getOrigin, data_array )
  }
}

case class ArraySpec( missing: Float, shape: Array[Int], origin: Array[Int], data: Array[Float] ) {
  def section( section: CDSection ): ArraySpec = {
    val ma2Array = ma2.Array.factory( ma2.DataType.FLOAT, shape, data )
    val sectionedArray = ma2Array.section( section.toSection.getRanges )
    new ArraySpec( missing, sectionedArray.getShape, section.getOrigin, sectionedArray.getStorage.asInstanceOf[Array[Float]] )
  }
  def size: Long = shape.product
  def ++( other: ArraySpec ): ArraySpec = concat( other )
  def toHeapFltArray = new HeapFltArray( shape, origin, data, Option( missing ) )
  def toFastMaskedArray: FastMaskedArray = FastMaskedArray( shape, data, missing )
  def toCDFloatArray: CDFloatArray = CDFloatArray(shape,data,missing)

  def combine( combineOp: CDArray.ReduceOp[Float], other: ArraySpec ): ArraySpec = {
    val result: FastMaskedArray = toFastMaskedArray.merge( other.toFastMaskedArray, combineOp )
    ArraySpec( missing, result.shape, origin, result.getData  )
  }

  def concat( other: ArraySpec ): ArraySpec = {
    val zippedShape = shape.zipWithIndex
    assert( zippedShape.drop(1).forall { case ( value:Int, index: Int ) => value == other.shape(index) }, s"Incommensurate shapes in array concatenation: ${shape.mkString(",")} vs ${other.shape.mkString(",")} " )
    val new_data: Array[Float] = ArrayUtils.addAll( data, other.data )
    val new_shape = zippedShape map { case ( value:Int, index: Int ) => if(index==0) {shape(0)+other.shape(0)} else {shape(index)} }
    ArraySpec( missing, new_shape, origin, new_data )
  }
  def toByteArray = {
    HeapFltArray.bb.putFloat( 0, missing )
    val ucarArray: ucar.ma2.Array = toCDFloatArray
    ucarArray.getDataAsByteBuffer().array() ++ HeapFltArray.bb.array()
  }
}

object CDTimeSlice {
  type ReduceOp = (CDTimeSlice,CDTimeSlice)=>CDTimeSlice
  def empty = new CDTimeSlice( -1, 0, Map.empty[String,ArraySpec] )
}

case class CDTimeSlice(timestamp: Long, dt: Int, elements: Map[String,ArraySpec] ) {
  def ++( other: CDTimeSlice ): CDTimeSlice = { new CDTimeSlice( timestamp, dt, elements ++ other.elements ) }
  def <+( other: CDTimeSlice ): CDTimeSlice = append( other )
//  def validate_identity( other_index: Int ): Unit = assert ( other_index == index , s"TimeSlice index mismatch: ${index} vs ${other_index}" )
  def clear: CDTimeSlice = { new CDTimeSlice( timestamp, dt, Map.empty[String,ArraySpec] ) }
  def section( section: CDSection ): CDTimeSlice = {  new CDTimeSlice( timestamp, dt, elements.mapValues( _.section(section) ) ) }
  def release( keys: Iterable[String] ): CDTimeSlice = {  new CDTimeSlice( timestamp, dt, elements.filterKeys(key => !keys.contains(key) ) ) }
  def size: Long = elements.values.foldLeft(0L)( (size,array) => array.size + size )
  def element( id: String ): Option[ArraySpec] = elements.get( id )
  def isEmpty = elements.isEmpty
  def ~( other: CDTimeSlice ) =  { assert( (dt == other.dt) && (timestamp == other.timestamp) , s"Mismatched Time slices: { $timestamp $dt } vs { ${other.timestamp} ${other.dt} }" ) }
  def ~>( other: CDTimeSlice ) = { assert( Math.abs( timestamp+dt - other.timestamp ) <= 1  , s"Non-adjacent Time slices: { $timestamp $dt } vs { ${other.timestamp} ${other.dt} }" ) }
  def append( other: CDTimeSlice ): CDTimeSlice = { this ~> other;  new CDTimeSlice(timestamp, dt + other.dt, elements.flatMap { case (key,array0) => other.elements.get(key).map( array1 => key -> ( array0 ++ array1 ) ) } ) }
}

class DataCollection( val metadata: Map[String,String] ) {
  def getParameter( key: String, default: String ="" ): String = metadata.getOrElse( key, default )
}

class TimeSliceRDD( val rdd: RDD[CDTimeSlice], metadata: Map[String,String] ) extends DataCollection(metadata) {
  def cache() = rdd.cache()
  def unpersist(blocking: Boolean ) = rdd.unpersist(blocking)
  def section( section: CDSection ): TimeSliceRDD = { new TimeSliceRDD( rdd.map( _.section(section) ), metadata ) }
  def release( keys: Iterable[String] ): TimeSliceRDD = new TimeSliceRDD( rdd.map( _.release(keys) ), metadata )
  def map( op: CDTimeSlice => CDTimeSlice ): TimeSliceRDD = new TimeSliceRDD( rdd.map( ts => ts ++ op(ts) ), metadata )
  def getNumPartitions = rdd.getNumPartitions
  def collect: TimeSliceCollection = new TimeSliceCollection( rdd.collect, metadata )
  def collect( op: PartialFunction[CDTimeSlice,CDTimeSlice] ): TimeSliceRDD = new TimeSliceRDD( rdd.collect(op), metadata )
  def reduce( op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice ): TimeSliceCollection = TimeSliceCollection( rdd.treeReduce(op), metadata )
  def dataSize: Long = rdd.map( _.size ).reduce ( _ + _ )
}

object TimeSliceCollection {
  def apply( slice: CDTimeSlice, metadata: Map[String,String] ): TimeSliceCollection = new TimeSliceCollection( Array(slice), metadata )
  def apply( slices: Array[CDTimeSlice], metadata: Map[String,String] ): TimeSliceCollection = new TimeSliceCollection( slices, metadata )
  def empty: TimeSliceCollection = new TimeSliceCollection( Array.empty[CDTimeSlice], Map.empty[String,String] )
}

class TimeSliceCollection( val slices: Array[CDTimeSlice], metadata: Map[String,String] ) extends DataCollection(metadata) {
  def section( section: CDSection ): TimeSliceCollection = { new TimeSliceCollection( slices.map( _.section(section) ), metadata ) }
  def sort(): TimeSliceCollection = { new TimeSliceCollection( slices.sortBy( _.timestamp ), metadata ) }
  val nslices: Int = slices.length

  def merge( other: TimeSliceCollection, op: CDTimeSlice.ReduceOp ): TimeSliceCollection = {
    val ( tsc0, tsc1 ) = ( sort(), other.sort() )
    val merged_slices = if(tsc0.slices.isEmpty) { tsc1.slices } else if(tsc1.slices.isEmpty) { tsc0.slices } else {
      tsc0.slices.zip( tsc1.slices ) map { case (s0,s1) => op(s0,s1) }
    }
    new TimeSliceCollection( merged_slices, metadata ++ other.metadata )
  }

  def concatSlices: TimeSliceCollection = {
    val concatSlices = sort().slices.reduce( _ ++ _ )
    new TimeSliceCollection( Array( concatSlices ), metadata )
  }

  def getConcatSlice: CDTimeSlice = concatSlices.slices.head
}

object PartitionExtensionGenerator {
  def apply() = new PartitionExtensionGenerator()
}

class PartitionExtensionGenerator {
  private var _optCurrentGenerator: Option[TimeSliceGenerator] = None
  private def _close = if( _optCurrentGenerator.isDefined ) { _optCurrentGenerator.get.close }
  private def _updateCache( varId: String, varName: String, section: String, fileInput: FileInput  ) = {
    if( _optCurrentGenerator.isEmpty || _optCurrentGenerator.get.fileInput.startTime != fileInput.startTime ) {
      _close
      _optCurrentGenerator = Some( new TimeSliceGenerator(varId, varName, section, fileInput) )
    }
  }
  private def _getGenerator( varId: String, varName: String, section: String, fileInput: FileInput  ): TimeSliceGenerator = {
    _updateCache( varId, varName, section, fileInput  );
    _optCurrentGenerator.get
  }

  def extendPartition( existingSlices: Iterator[CDTimeSlice], fileBase: FileBase, varId: String, varName: String, section: String ): Iterator[CDTimeSlice] = {
    val sliceIter = existingSlices map { tSlice =>
      val fileInput: FileInput = fileBase.getFileInput( tSlice.timestamp )
      val generator: TimeSliceGenerator = _getGenerator( varId, varName, section, fileInput )
      val newSlice: CDTimeSlice = generator.getSlice( tSlice.timestamp )
      tSlice ++ newSlice
    }
    _close
    sliceIter
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
    val rdd = template.rdd.mapPartitions( tSlices => PartitionExtensionGenerator().extendPartition( tSlices, agg.getFilebase, varId, varName, section ) )
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
      val section = variable.getShapeAsSection
      val arraySpec = ArraySpec( missing, data_section.getShape, section.getOrigin, data_array )
      //      (timestamp/millisPerMin).toInt -> CDTimeSlice( timestamp, missing, data_section )  // new java.sql.Timestamp( date.getMillis )
      CDTimeSlice( date.getMillis, dt, Map( varId -> arraySpec ) )  //
    }
    dataset.close()
    if( fileInput.index % 500 == 0 ) {
      val sample_array =  slices.head.elements.head._2.data
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
    def getSliceRanges( section: ma2.Section, slice_index: Int ): java.util.List[ma2.Range] = {
      section.getRanges.zipWithIndex map { case (range: ma2.Range, index: Int) => if( index == 0 ) { new ma2.Range("time",slice_index,slice_index)} else { range } }
    }
    val data_section = variable.read( getSliceRanges( interSect, getSliceIndex(timestamp)) )
    val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
    val data_shape: Array[Int] = data_section.getShape
    val section = variable.getShapeAsSection
    val arraySpec = ArraySpec( missing, data_section.getShape, section.getOrigin, data_array )
    CDTimeSlice( timestamp, dt, Map( varId -> arraySpec ) )  //
  }
}

class RDDContainer extends Loggable {
  private var _vault: Option[RDDVault] = None
  def releaseBatch = { _vault.foreach(_.clear);  _vault = None }
  private def vault: RDDVault = _vault.getOrElse { throw new Exception( "Unexpected attempt to access an uninitialized RDD Vault")}
  def value: TimeSliceRDD = vault.value
  def contents: Iterable[String] = _vault.fold( Iterable.empty[String] ) ( _.contents )
  def section( section: CDSection ): Unit = vault.map( _.section(section) )
  def release( keys: Iterable[String] ): Unit = { vault.release( keys ) }

  private def initialize( init_value: TimeSliceRDD, contents: List[String] ) = {
    _vault = Some( new RDDVault( init_value ) )
  }

  class RDDVault( init_value: TimeSliceRDD ) {
    private var _rdd = init_value; _rdd.cache()
    def update( new_rdd: TimeSliceRDD ): Unit = { _rdd.unpersist(false); _rdd = new_rdd; _rdd.cache }
    def map( f: (TimeSliceRDD) => TimeSliceRDD ): Unit = update( f(_rdd) )
    def value = _rdd
    def clear: Unit = _rdd.unpersist(false)
    def contents = _rdd.rdd.first().elements.keys
    def release( keys: Iterable[String] ) = { update( _rdd.release(keys) ) }
    def += ( record: CDTimeSlice ) = { update( _rdd.map( slice => slice ++ record ) ) }
    def += ( records: TimeSliceCollection ) = {
      assert( records.nslices <= 1, "UNIMPLEMENTED FEATURE: TimeSliceCollection -> RDDVault")
      update( _rdd.map( slice => slice ++ records.slices.headOption.getOrElse( CDTimeSlice.empty ) ) )
    }
  }
  def map( kernel: Kernel, context: KernelContext ): Unit = { vault.update( kernel.mapRDD( vault.value, context ) ) }

  def execute( workflow: Workflow, node: Kernel, context: KernelContext, batchIndex: Int ): TimeSliceCollection = node.execute( workflow, value, context, batchIndex )

  private def _extendRDD( generator: RDDGenerator, rdd: TimeSliceRDD, vSpecs: List[DirectRDDVariableSpec]  ): TimeSliceRDD = {
    if( vSpecs.isEmpty ) { rdd }
    else {
      val vspec = vSpecs.head
      val extendedRdd = generator.parallelize(rdd, vspec.getAggregation(), vspec.uid, vspec.varShortName )
      _extendRDD( generator, extendedRdd, vSpecs.tail )
    }
  }

  def extendVault( generator: RDDGenerator, vSpecs: List[DirectRDDVariableSpec] ) = { vault.update( _extendRDD( generator, _vault.get.value, vSpecs ) ) }

  def addFileInputs( sparkContext: CDSparkContext, kernelContext: KernelContext, vSpecs: List[DirectRDDVariableSpec] ): Unit = {
    logger.info("\n\n RDDContainer ###-> BEGIN addFileInputs: operation %s, VarSpecs: [ %s ], contents = [ %s ] --> expected: [ %s ]   -------\n".format( kernelContext.operation.name, vSpecs.map( _.uid ).mkString(", "), _vault.fetchContents.mkString(", "), contents.mkString(", ") ) )
    val newVSpecs = vSpecs.filter( vspec => ! contents.contains(vspec.uid) )
    if( newVSpecs.nonEmpty ) {
      val generator = new RDDGenerator( sparkContext, BatchSpec.nParts )
      logger.info( s"Generating file inputs with ${BatchSpec.nParts} partitions available, inputs = [ ${vSpecs.map( _.uid ).mkString(", ")} ]" )
      val remainingVspecs = if( _vault.isEmpty ) {
        val tvspec = vSpecs.head
        val baseRdd: TimeSliceRDD = generator.parallelize(tvspec.getAggregation(), tvspec.uid, tvspec.varShortName, tvspec.section.toString)
        initialize( baseRdd, List(tvspec.uid) )
        vSpecs.tail
      } else { vSpecs }
      extendVault( generator, remainingVspecs )
    }
    logger.info("\n\n RDDContainer ###-> END addFileInputs: operation %s, VarSpecs: [ %s ], contents = [ %s ] --> expected: [ %s ]   -------\n".format( kernelContext.operation.name, vSpecs.map( _.uid ).mkString(", "), _vault.fetchContents.mkString(", "), contents.mkString(", ") ) )
  }


  def addOperationInput( inputs: TimeSliceCollection ): Unit = { vault += inputs }
}

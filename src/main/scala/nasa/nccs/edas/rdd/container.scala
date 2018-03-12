package nasa.nccs.edas.rdd

import java.nio.FloatBuffer
import java.nio.file.Paths
import java.util.{Calendar, Date}

import nasa.nccs.caching.BatchSpec
import nasa.nccs.cdapi.cdm.{CDGrid, OperationDataInput}
import nasa.nccs.cdapi.data.{DirectRDDVariableSpec, FastMaskedArray, HeapFltArray}
import org.apache.commons.lang.ArrayUtils
import nasa.nccs.cdapi.tensors.{CDArray, CDFloatArray}
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.edas.engine.Workflow
import nasa.nccs.edas.engine.spark.{CDSparkContext, RecordKey}
import nasa.nccs.edas.kernels._
import nasa.nccs.edas.sources.{Aggregation, Collection, FileBase, FileInput}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.edas.workers.TransVar
import nasa.nccs.esgf.process.{CDSection, EDASCoordSystem, ServerContext}
import nasa.nccs.utilities.{EDTime, Loggable, cdsutils}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.spark_project.guava.io.Files
import ucar.ma2
import ucar.nc2.Variable
import ucar.nc2.dataset.CoordinateAxis1DTime
import ucar.nc2.time.CalendarDate

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable

object ArraySpec {
  def apply( tvar: TransVar ) = {
    val data_array =  HeapFltArray( tvar )
    new ArraySpec( data_array.missing.getOrElse(Float.NaN), tvar.getShape, tvar.getOrigin, data_array.data, None )
  }
  def apply( fma: FastMaskedArray, origin: Array[Int] ): ArraySpec = new ArraySpec( fma.missing, fma.shape, origin,  fma.getData, None)
}

case class ArraySpec( missing: Float, shape: Array[Int], origin: Array[Int], data: Array[Float], optGroup: Option[TSGroupIdentifier] ) {
  def size: Long = shape.product
  def ++( other: ArraySpec ): ArraySpec = concat( other )
  def toHeapFltArray( gridSpec: String, metadata: Map[String,String] = Map.empty) = new HeapFltArray( shape, origin, data, Option( missing ), gridSpec, metadata + ( "gridfile" -> gridSpec ) )
  def toFastMaskedArray: FastMaskedArray = FastMaskedArray( shape, data, missing )
  def toCDFloatArray: CDFloatArray = CDFloatArray(shape,data,missing)
  def getSection: ma2.Section = new ma2.Section( origin, shape )
  def getRelativeSection: ma2.Section = new ma2.Section( origin, shape ).shiftOrigin( new ma2.Section( origin, shape ) )

  def setGroupId( group: TSGroup, group_index: Long ): ArraySpec = {
    new ArraySpec( missing, shape, origin, data, Some( new TSGroupIdentifier(group,group_index) ) )
  }

  def section( section: CDSection ): Option[ArraySpec] = {
    val ma2Array = ma2.Array.factory( ma2.DataType.FLOAT, shape, data )
    val mySection: ma2.Section = getSection
    val newSection: ma2.Section = section.toSection
    println( s"    --> ArraySpec.section: mySection = ${mySection.toString} newSection=${newSection.toString}")
    if( mySection.intersects(newSection) ) {
      try {
        val interSection = newSection.intersect(mySection).shiftOrigin(mySection)
        val sectionedArray = ma2Array.section(interSection.getOrigin, interSection.getShape)
        Some( new ArraySpec(missing, interSection.getShape, section.getOrigin, sectionedArray.getStorage.asInstanceOf[Array[Float]], optGroup ) )
      } catch {
        case err: Exception =>
          throw err
      }
    } else {
      None
    }
  }

  def combine( combineOp: CDArray.ReduceOp[Float], other: ArraySpec, weighted: Boolean ): ArraySpec = {
    val result: FastMaskedArray = toFastMaskedArray.merge( other.toFastMaskedArray, combineOp, weighted )
    ArraySpec( missing, result.shape, origin, result.getData, optGroup  )
  }

  def concat( other: ArraySpec ): ArraySpec = {
    val zippedShape = shape.zipWithIndex
    assert( zippedShape.drop(1).forall { case ( value:Int, index: Int ) => value == other.shape(index) }, s"Incommensurate shapes in array concatenation: ${shape.mkString(",")} vs ${other.shape.mkString(",")} " )
    val new_data: Array[Float] = ArrayUtils.addAll( data, other.data )
    val new_shape = zippedShape map { case ( value:Int, index: Int ) => if(index==0) {shape(0)+other.shape(0)} else {shape(index)} }
    ArraySpec( missing, new_shape, origin, new_data, optGroup )
  }
  def toByteArray = {
    HeapFltArray.bb.putFloat( 0, missing )
    val ucarArray: ucar.ma2.Array = toCDFloatArray
    ucarArray.getDataAsByteBuffer().array() ++ HeapFltArray.bb.array()
  }
}

case class CDTimeInterval(startTime: Long, endTime: Long ) {
  def midpoint: Long = (startTime + endTime)/2
  def ~( other: CDTimeSlice ) =  { assert( (endTime == other.endTime) && (startTime == other.startTime) , s"Mismatched Time intervals: { $startTime $endTime } vs { ${other.startTime} ${other.endTime} }" ) }
  def mergeStart( other: CDTimeInterval ): Long = Math.min( startTime, other.startTime )
  def mergeEnd( other: CDTimeInterval ): Long = Math.max( endTime, other.endTime )
  def precedes( other: CDTimeInterval ) = {assert(  startTime < other.startTime, s"Disordered Time intervals: { $startTime $endTime -> ${startTime+endTime} } vs { ${other.startTime} ${other.endTime} }" ) }
  def append( other: CDTimeInterval ): CDTimeInterval = { this precedes other; new CDTimeInterval( mergeStart( other ), mergeEnd( other ) ) }
}

object CDTimeSlice {
  type ReduceOp = (CDTimeSlice,CDTimeSlice)=>CDTimeSlice
  def empty = new CDTimeSlice(-1, 0, Map.empty[String,ArraySpec], Map.empty[String, String] )
}

case class CDTimeSlice(startTime: Long, endTime: Long, elements: Map[String, ArraySpec], metadata: Map[String, String] ) {
  def ++( other: CDTimeSlice ): CDTimeSlice = { new CDTimeSlice(startTime, endTime, elements ++ other.elements, metadata) }
  def <+( other: CDTimeSlice ): CDTimeSlice = append( other )
  def clear: CDTimeSlice = { new CDTimeSlice(startTime, endTime, Map.empty[String,ArraySpec], metadata) }
  def midpoint: Long = (startTime + endTime)/2
  def mergeStart( other: CDTimeSlice ): Long = Math.min( startTime, other.startTime )
  def mergeEnd( other: CDTimeSlice ): Long = Math.max( endTime, other.endTime )
  def section( section: CDSection ): Option[CDTimeSlice] = {
    val new_elements = elements.flatMap { case (key, array) => array.section(section).map( sarray => (key,sarray) ) }
    if( new_elements.isEmpty ) { None } else { Some( new CDTimeSlice(startTime, endTime, new_elements, metadata) ) }
  }
  def release( keys: Iterable[String] ): CDTimeSlice = { new CDTimeSlice(startTime, endTime, elements.filterKeys(key => !keys.contains(key) ), metadata) }
  def selectElement( elemId: String ): CDTimeSlice = CDTimeSlice(startTime, endTime, elements.filterKeys( _.equalsIgnoreCase(elemId) ), metadata)
  def selectElements( op: String => Boolean ): CDTimeSlice = CDTimeSlice(startTime, endTime, elements.filterKeys(key => op(key) ), metadata)
  def size: Long = elements.values.foldLeft(0L)( (size,array) => array.size + size )
  def element( id: String ): Option[ArraySpec] =  elements find { case (key,value) => key.split(':').last.equals(id) } map ( _._2 )
  def isEmpty = elements.isEmpty
  def findElements( id: String ): Iterable[ArraySpec] = ( elements filter { case (key,array) => key.split(':').last.equals(id) } ) values
  def contains( other_startTime: Long ): Boolean = {
    ( other_startTime >= startTime ) && ( other_startTime <= endTime )
  }
  def setGroupId( group: TSGroup, group_index: Long ): CDTimeSlice = {
    val new_elems = elements.mapValues( _.setGroupId(group,group_index) )
    new CDTimeSlice( startTime, endTime, elements, metadata )
  }
  def contains( other: CDTimeSlice ): Boolean = contains( other.startTime )
  def ~( other: CDTimeSlice ) =  { assert( (endTime == other.endTime) && (startTime == other.startTime) , s"Mismatched Time slices: { $startTime $endTime } vs { ${other.startTime} ${other.endTime} }" ) }
  def precedes( other: CDTimeSlice ) = {assert(  startTime < other.startTime, s"Disordered Time slices: { $startTime $endTime -> ${startTime+endTime} } vs { ${other.startTime} ${other.endTime} }" ) }
  def append( other: CDTimeSlice ): CDTimeSlice = {
    this precedes other;
    new CDTimeSlice(mergeStart( other ), mergeEnd( other ), elements.flatMap { case (key,array0) => other.elements.get(key).map(array1 => key -> ( array0 ++ array1 ) ) }, metadata )
  }
  def addExtractedSlice( collection: TimeSliceCollection ): CDTimeSlice =
    collection.slices.find( _.contains( this ) ) match {
      case None =>
        throw new Exception( s"Missing matching slice in broadcast: { ${startTime}, ${endTime} }")
      case Some( extracted_slice ) =>
        CDTimeSlice( startTime, endTime, elements ++ extracted_slice.elements, metadata )
    }
}

class DataCollection( val metadata: Map[String,String] ) extends Serializable {
  def getParameter( key: String, default: String ="" ): String = metadata.getOrElse( key, default )
}

object TSGroup {
  def season( month: Int ): Int = ( (month+1) % 12 )/3
  def getGroup( groupBy: String ): TSGroup = {
    if( groupBy.equalsIgnoreCase("monthofyear") ) { new TSGroup ( cal => cal.get( Calendar.MONTH ), true ) }
    else if( groupBy.equalsIgnoreCase("decade") ) { new TSGroup ( cal => cal.get( Calendar.YEAR )/10, false ) }
    else if( groupBy.equalsIgnoreCase("year") ) { new TSGroup ( cal => cal.get( Calendar.YEAR ), false ) }
    else if( groupBy.equalsIgnoreCase("month") ) { new TSGroup ( cal =>  ( cal.get( Calendar.YEAR ) - 1970 )*12 + cal.get( Calendar.MONTH ), false ) }
    else if( groupBy.equalsIgnoreCase("hourofday") ) { new TSGroup ( cal =>  cal.get( Calendar.HOUR_OF_DAY ), true ) }
    else if( groupBy.equalsIgnoreCase("season") ) { new TSGroup ( cal =>  ( cal.get( Calendar.YEAR ) - 1970 )*4 + season( cal.get( Calendar.MONTH ) ), false ) }
    else if( groupBy.equalsIgnoreCase("seasonofyear") ) { new TSGroup ( cal =>  season( cal.get( Calendar.MONTH ) ), true ) }
    else if( groupBy.equalsIgnoreCase("day") ) { new TSGroup ( cal =>  ( cal.get( Calendar.YEAR ) - 1970 )*365 + cal.get( Calendar.DAY_OF_YEAR ), false ) }
    else if( groupBy.equalsIgnoreCase("dayofyear") ) { new TSGroup ( cal => cal.get( Calendar.DAY_OF_YEAR ), true ) }
    else { throw new Exception(s"Unrecognized groupBy argument: ${groupBy}") }
  }
}

class TSGroup( val calOp: (Calendar) => Int, val isCyclic: Boolean  ) extends Serializable {
  lazy val calendar = Calendar.getInstance()
  def group( slice: CDTimeSlice ): Int = { calendar.setTimeInMillis(slice.midpoint); calOp( calendar ) }
  def isNonCyclic = !isCyclic
}

class TSGroupIdentifier( val group: TSGroup, val group_index: Long )  extends Serializable {
  def matches( slice: CDTimeSlice ): Boolean = {
    val index = group.group( slice )
    index == group_index
  }
}


class KeyPartitioner( val nParts: Int ) extends Partitioner {
  def fromLong( key: Long ): Int = ( key % Int.MaxValue ).toInt
  def fromDouble( key: Double ): Int = fromLong( Math.round(key) )
  def numPartitions: Int = nParts

  def getPartition( key: Any ): Int = key match {
    case ikey: Int => ikey
    case lkey: Long => fromLong(lkey)
    case _ => fromDouble( key.toString.toDouble )
  }
}

object TimeSliceRDD extends Serializable {
  def apply( rdd: RDD[CDTimeSlice], metadata: Map[String,String], variableRecords: Map[String,VariableRecord] ): TimeSliceRDD = new TimeSliceRDD( rdd, metadata, variableRecords )
  def sortedReducePartition(op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice )(slices: Iterable[CDTimeSlice]): CDTimeSlice = {
    val nSlices = slices.size
    slices.toSeq.sortBy( _.startTime ).fold(CDTimeSlice.empty)(op)
  }
  def reducePartition(op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice )(slices: Iterable[CDTimeSlice]): CDTimeSlice = { slices.toSeq.reduce(op) }

  def weightedValueSumRDDPostOp(slice: CDTimeSlice): CDTimeSlice = {
    val new_elements = slice.elements.filterKeys(!_.endsWith("_WEIGHTS_")) map { case (key, arraySpec) =>
      val wts = slice.elements.getOrElse(key + "_WEIGHTS_", throw new Exception(s"Missing weights in slice, ids = ${slice.elements.keys.mkString(",")}"))
      val newData = arraySpec.toFastMaskedArray / wts.toFastMaskedArray
      key -> new ArraySpec(newData.missing, newData.shape, arraySpec.origin, newData.getData, arraySpec.optGroup )
    }
    CDTimeSlice(slice.startTime, slice.endTime, new_elements, slice.metadata )
  }

  def postOp( postOpId: String )( slice: CDTimeSlice ): CDTimeSlice = {
    if( postOpId.isEmpty ) { return slice }
    val elements: Map[String, ArraySpec] = slice.elements
    val postOpKey = PostOpOperations.get( postOpId )
    val ( weights_list, values_list ) = elements.keys.partition(_.endsWith("_WEIGHTS_"))
    val new_elems = for( values_key <- values_list; valuesSpec: ArraySpec = elements(values_key); valuesArray: FastMaskedArray = valuesSpec.toFastMaskedArray ) yield {
      val weights_key = values_key + "_WEIGHTS_"
      val weigtsArrayOpt = elements.get(weights_key)
      val resultValues: FastMaskedArray = postOpKey match {
        case PostOpOperations.normw => weigtsArrayOpt.fold( valuesArray )( wts => valuesArray / wts.toFastMaskedArray )
        case PostOpOperations.sqrt =>  valuesArray.sqrt()
        case PostOpOperations.rms =>   weigtsArrayOpt.fold( valuesArray.sqrt() )( wts => ( valuesArray / (wts.toFastMaskedArray - 1) ).sqrt() )
        case x => FastMaskedArray.empty // Never reached.
      }
      values_key -> ArraySpec(valuesArray.missing, valuesArray.shape, valuesSpec.origin, resultValues.getData, valuesSpec.optGroup )
    }
    CDTimeSlice( slice.startTime, slice.endTime, new_elems.toMap, slice.metadata )
  }

  def reduceRddByGroup(rdd: RDD[CDTimeSlice], op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice, postOpId: String, groupBy: TSGroup ): RDD[(Int,CDTimeSlice)] =
    reduceKeyedRddByGroup( rdd.keyBy( groupBy.group ), op, postOpId, groupBy )

  def reduceKeyedRddByGroup(rdd: RDD[(Int,CDTimeSlice)], op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice, postOpId: String, groupBy: TSGroup ): RDD[(Int,CDTimeSlice)] =
    rdd.reduceByKey( op ) map { case ( key, slice ) => key -> postOp( postOpId )( slice ).setGroupId( groupBy, key ) }
}

class TimeSliceRDD( val rdd: RDD[CDTimeSlice], metadata: Map[String,String], val variableRecords: Map[String,VariableRecord] ) extends DataCollection(metadata) with Loggable {
  import TimeSliceRDD._
  def cache() = rdd.cache()
  def nSlices = rdd.count
  def exe: TimeSliceRDD = { rdd.cache; rdd.count; this }
  def unpersist(blocking: Boolean ) = rdd.unpersist(blocking)
  def section( section: CDSection ): TimeSliceRDD = TimeSliceRDD( rdd.flatMap( _.section(section) ), metadata, variableRecords )
  def release( keys: Iterable[String] ): TimeSliceRDD = TimeSliceRDD( rdd.map( _.release(keys) ), metadata, variableRecords )
  def map( op: CDTimeSlice => CDTimeSlice ): TimeSliceRDD = TimeSliceRDD( rdd map op , metadata, variableRecords )
  def getNumPartitions = rdd.getNumPartitions
  def nodeList: Array[String] = rdd.mapPartitionsWithIndex { case ( index, tsIter )  => if(tsIter.isEmpty) { Iterator.empty } else { Seq( s"{P${index}-(${KernelContext.getProcessAddress}), size: ${tsIter.length}}" ).toIterator }  } collect
//  def collect( op: PartialFunction[CDTimeSlice,CDTimeSlice] ): TimeSliceRDD = TimeSliceRDD( rdd.collect(op), metadata, variableRecords )
  def dataSize: Long = rdd.map( _.size ).reduce ( _ + _ )
  def selectElement( elemId: String ): TimeSliceRDD = TimeSliceRDD ( rdd.map( _.selectElement( elemId ) ), metadata, variableRecords )
  def selectElements(  elemFilter: String => Boolean  ): TimeSliceRDD = TimeSliceRDD ( rdd.map( _.selectElements( elemFilter ) ), metadata, variableRecords )

  def reduceByGroup( op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice, elemFilter: String => Boolean, postOpId: String, groupBy: TSGroup ): TimeSliceRDD = {
    val keyedRDD: RDD[(Int,CDTimeSlice)] = rdd.keyBy( groupBy.group )
    val groupedRDD:  RDD[(Int,CDTimeSlice)] = TimeSliceRDD.reduceKeyedRddByGroup( keyedRDD.mapValues( _.selectElements( elemFilter ) ), op, postOpId, groupBy )
    val result_rdd = keyedRDD.join( groupedRDD ) map { case ( key, (slice0, slice1) ) => slice0 ++ slice1 }
    new TimeSliceRDD( result_rdd, metadata, variableRecords )
  }

  def collect( elemFilter: String => Boolean, postOpId: String ): TimeSliceCollection = {
    val processedRDD: RDD[CDTimeSlice] = rdd.map( slice => postOp( postOpId )( slice.selectElements( elemFilter ) ) )
    TimeSliceCollection( processedRDD.collect, metadata )
  }

  def reduce( op: (CDTimeSlice,CDTimeSlice) => CDTimeSlice, elemFilter: String => Boolean, postOpId: String, optGroupBy: Option[TSGroup], ordered: Boolean = false ): TimeSliceCollection = {
    val filteredRdd = rdd.map( _.selectElements( elemFilter ) )
    if (ordered) optGroupBy match {
      case None =>
        val partialProduct = filteredRdd.mapPartitions( slices => Iterator( TimeSliceRDD.sortedReducePartition(op)(slices.toIterable) ) ).collect
        val slice: CDTimeSlice = postOp( postOpId )(
          TimeSliceRDD.sortedReducePartition(op)(partialProduct)
        )
        TimeSliceCollection( slice, metadata)
      case Some( groupBy ) =>
        val partialProduct = filteredRdd.groupBy( groupBy.group ).mapValues( TimeSliceRDD.sortedReducePartition(op) ).map( item => postOp( postOpId )( item._2 ) )
        TimeSliceCollection( partialProduct.collect.sortBy( _.startTime ), metadata )
    }
    else optGroupBy match {
      case None =>
        val slice: CDTimeSlice = postOp( postOpId )( filteredRdd.treeReduce(op) )
        TimeSliceCollection( slice, metadata )
      case Some( groupBy ) =>
        val groupedRDD:  RDD[(Int,CDTimeSlice)] = TimeSliceRDD.reduceRddByGroup( filteredRdd, op, postOpId, groupBy )
        TimeSliceCollection( groupedRDD.values.collect, metadata )
    }
  }
}

object TimeSliceCollection {
  def apply( slice: CDTimeSlice, metadata: Map[String,String] ): TimeSliceCollection = TimeSliceCollection( Array(slice), metadata )
  def empty: TimeSliceCollection = TimeSliceCollection( Array.empty[CDTimeSlice], Map.empty[String,String] )

}

case class TimeSliceCollection( slices: Array[CDTimeSlice], metadata: Map[String,String] ) extends Serializable {
  def getParameter( key: String, default: String ="" ): String = metadata.getOrElse( key, default )
  def section( section: CDSection ): TimeSliceCollection = {
    TimeSliceCollection( slices.flatMap( _.section(section) ), metadata )
  }
  def sort(): TimeSliceCollection = { TimeSliceCollection( slices.sortBy( _.startTime ), metadata ) }
  val nslices: Int = slices.length

  def merge( other: TimeSliceCollection, op: CDTimeSlice.ReduceOp ): TimeSliceCollection = {
    val ( tsc0, tsc1 ) = ( sort(), other.sort() )
    val merged_slices = if(tsc0.slices.isEmpty) { tsc1.slices } else if(tsc1.slices.isEmpty) { tsc0.slices } else {
      tsc0.slices.zip( tsc1.slices ) map { case (s0,s1) => op(s0,s1) }
    }
    TimeSliceCollection( merged_slices, metadata ++ other.metadata )
  }

  def getMetadata: Map[String,String] = metadata ++ slices.headOption.fold(Map.empty[String,String])(_.metadata) // slices.foldLeft(metadata)( _ ++ _.metadata )

  def concatSlices: TimeSliceCollection = {
    val concatSlices = sort().slices.reduce( _ <+ _ )
    TimeSliceCollection( Array( concatSlices ), metadata )
  }

  def getConcatSlice: CDTimeSlice = concatSlices.slices.head
}

object PartitionExtensionGenerator {
  def apply(partIndex: Int) = new PartitionExtensionGenerator(partIndex)
}

class PartitionExtensionGenerator(val partIndex: Int) extends Serializable {
  private var _optCurrentGenerator: Option[TimeSliceGenerator] = None
  private def _close = if( _optCurrentGenerator.isDefined ) { _optCurrentGenerator.get.close; _optCurrentGenerator = None;  }
  private def _updateCache( varId: String, varName: String, section: String, fileInput: FileInput, optBasePath: Option[String]  ) = {
    if( _optCurrentGenerator.isEmpty || _optCurrentGenerator.get.fileInput.startTime != fileInput.startTime ) {
      _close
      _optCurrentGenerator = Some( new TimeSliceGenerator(varId, varName, section, fileInput, optBasePath ) )
//      println( s"\n --------------------------------------------------------------------------------------- \n -->  P[${partIndex}] Loading file ${fileInput.path}")
    }
  }
  private def _getGenerator( varId: String, varName: String, section: String, fileInput: FileInput, optBasePath: Option[String]   ): TimeSliceGenerator = {
    _updateCache( varId, varName, section, fileInput, optBasePath  );
//    println( s" P[${partIndex}] Getting generator for varId: ${varId}, varName: ${varName}, section: ${section}, fileInput: ${fileInput}" )
    _optCurrentGenerator.get
  }

  def extendPartition( existingSlices: Seq[CDTimeSlice], fileBase: FileBase, varId: String, varName: String, section: String, optBasePath: Option[String] ): Seq[CDTimeSlice] = {
    val sliceIter = existingSlices.sortBy(_.startTime) map { tSlice =>
      val fileInput: FileInput = fileBase.getFileInput( tSlice.startTime )
      val generator: TimeSliceGenerator = _getGenerator( varId, varName, section, fileInput, optBasePath )
//      println( s" ***  P[${partIndex}]-ExtendPartition for varId: ${varId}, varName: ${varName}: StartTime: ${tSlice.startTime}, date: ${new Date(tSlice.startTime).toString}, FileInput start date: ${CalendarDate.of(fileInput.startTime).toString} ${fileInput.nRows} ${fileInput.path}  ")
      val newSlice: CDTimeSlice = generator.getSlice( tSlice )
      tSlice ++ newSlice
    }
    sliceIter
  }
}

object VariableRecord {
  def apply( vspec: DirectRDDVariableSpec, collection: Collection, metadata: Map[String,String]  ): VariableRecord = {
    val grid = collection.getGrid(vspec.varShortName)
    new VariableRecord(vspec.varShortName, collection.id, grid.gridFilePath, collection.getResolution(vspec.varShortName), grid.getProjection, vspec.getParameter("dimensions", ""), vspec.metadata ++ metadata)
  }
}

class VariableRecord( val varName: String, val collection: String, val gridFilePath: String, resolution: String, projection: String, val dimensions: String, val metadata: Map[String,String] ) extends EDASCoordSystem( resolution, projection ) {
  override def toString = s"VariableRecord[ varName=${varName}, collection=${collection}, gridFilePath=${gridFilePath}, resolution=${resolution}, projection=${projection}, dimensions=${dimensions}, metadata={${metadata.mkString(",")}} )"
}

class RDDGenerator( val sc: CDSparkContext, val nPartitions: Int) extends Loggable {

  def parallelize( kernelContext: KernelContext, vspec: DirectRDDVariableSpec ): TimeSliceRDD = {
    val t0 = System.nanoTime
    val timeRange = vspec.section.getRange(0)
    val collection: Collection = vspec.getCollection
    val agg: Aggregation = collection.getAggregation( vspec.varShortName ) getOrElse { throw new Exception( s"Can't find aggregation for variable ${vspec.varShortName} in collection ${collection.collId}" ) }
    val files: Array[FileInput]  = agg.getIntersectingFiles( timeRange )
    val nTS = timeRange.length()
//    val nTSperPart = if( files.length >= nPartitions ) { -1 } else { Math.max( 1, Math.round( nTS/nPartitions.toFloat ) ) }
    val nTSperPart = if( files.length >= nPartitions ) { -1 } else { Math.ceil(  nTS/nPartitions.toFloat ).toInt }
    val nUsableParts = if (  nTSperPart == -1 ) { nPartitions } else { Math.ceil( nTS / nTSperPart.toFloat ).toInt }
    val partGens: Array[TimeSlicePartitionGenerator]  = files.map( fileInput => TimeSlicePartitionGenerator(vspec.uid, vspec.varShortName, vspec.section, fileInput, agg.parms.getOrElse("base.path", ""), nTSperPart ) )
    val partitions = partGens.flatMap( _.getTimeSlicePartitions )
    logger.info( " @DSX FIRST Partition: " + partitions.headOption.fold("")(_.toString) )
    logger.info( " @DSX LAST Partition:  " + partitions.lastOption.fold("")(_.toString) )
    val slicePartitions: RDD[TimeSlicePartition] = sc.sparkContext.parallelize( partitions )
    val t1 = System.nanoTime
    val sliceRdd: RDD[CDTimeSlice] =  slicePartitions.mapPartitions( _.flatMap( _.getSlices ) )
    val optVar = agg.findVariable( vspec.varShortName )
    if( KernelContext.workflowMode == WorkflowMode.profiling ) { val rddSize = sliceRdd.count() }
    logger.info( s" @XX Parallelize: timeRange = ${timeRange.toString}, nTS = ${nTS}, nPartGens = ${partGens.length}, Available Partitions = ${nPartitions}, Usable Partitions = ${nUsableParts}, prep time = ${(t1-t0)/1e9} , total time = ${(System.nanoTime-t0)/1e9} ")
    TimeSliceRDD( sliceRdd, agg.parms, Map( vspec.uid -> VariableRecord( vspec, collection, optVar.fold(Map.empty[String,String])(_.toMap)) ) )
  }


  def parallelize( template: TimeSliceRDD, vspec: DirectRDDVariableSpec ): TimeSliceRDD = {
    val collection: Collection = vspec.getCollection
    val agg: Aggregation = collection.getAggregation( vspec.varShortName ) getOrElse { throw new Exception( s"Can't find aggregation for variable ${vspec.varShortName} in collection ${collection.collId}" ) }
    val optVar = agg.findVariable( vspec.varShortName )
    val section = template.getParameter( "section" )
    val basePath = agg.parms.get("base.path")
    val rdd = template.rdd.mapPartitionsWithIndex( ( index, tSlices ) => PartitionExtensionGenerator(index).extendPartition( tSlices.toSeq, agg.getFilebase, vspec.uid, vspec.varShortName, section, agg.getBasePath ).toIterator )
    TimeSliceRDD( rdd, agg.parms, template.variableRecords ++ Seq( vspec.uid -> VariableRecord( vspec, collection, optVar.fold(Map.empty[String,String])(_.toMap) ) ) )
  }
}

object TimeSlicePartitionGenerator extends Loggable {
  def apply( varId: String, varName: String, section: CDSection, fileInput: FileInput, basePath: String, rowsPerPartition: Int ): TimeSlicePartitionGenerator = {
    new TimeSlicePartitionGenerator( varId, varName, section, fileInput, basePath, rowsPerPartition )
  }
}

class TimeSlicePartitionGenerator(val varId: String, val varName: String, val section: CDSection, val fileInput: FileInput, val basePath: String,  val rowsPerPartition: Int = -1 ) extends Loggable {
  val timeRange = section.getRange(0)
  val intersectingRange = fileInput.intersect( timeRange )
  val nFileIntersectingRows = intersectingRange.length
//  logger.info( s" @DSX PartIntersect, fileInput = ${fileInput.path}, nFileIntersectingRows = ${nFileIntersectingRows}, intersectingRange = ${intersectingRange.toString}, timeRange = ${timeRange.toString}" )
  val partsPerFile: Int = if(rowsPerPartition == -1) { 1 } else { Math.ceil( nFileIntersectingRows / rowsPerPartition.toFloat ).toInt }

  def getTimeSlicePartitions: IndexedSeq[TimeSlicePartition] = ( 0 until partsPerFile ) map ( iPartIndex => {
      val partStartRow = if(rowsPerPartition == -1) { intersectingRange.first } else { intersectingRange.first +  iPartIndex * rowsPerPartition }
      val partEndRow = if(rowsPerPartition == -1) { intersectingRange.last } else { Math.min( partStartRow + rowsPerPartition -1, intersectingRange.last ) }
      val partRange = new ma2.Range( partStartRow, partEndRow )
//      logger.info( s" @DSX getTimeSlicePartitions[${iPartIndex}/${partsPerFile}], rowsPerPartition = ${rowsPerPartition}, partRange = [ ${partRange.toString} ]")
      TimeSlicePartition (varId, varName, section, fileInput, basePath, partRange )
    } )
}

object TimeSlicePartition {
  def apply( varId: String, varName: String, cdsection: CDSection, fileInput: FileInput, basePath: String, partitionRange: ma2.Range ): TimeSlicePartition = {
    new TimeSlicePartition( varId, varName, cdsection, fileInput, basePath, PartitionRange(partitionRange) )
  }
  def getMissing( variable: Variable, default_value: Float = Float.NaN ): Float = {
    Seq( "missing_value", "fmissing_value", "fill_value").foreach ( attr_name => Option( variable.findAttributeIgnoreCase(attr_name) ).foreach( attr => return attr.getNumericValue.floatValue() ) )
    default_value
  }
}

object PartitionRange {
  def apply( range: ma2.Range ): PartitionRange = new PartitionRange( range.first, range.last )
}

case class PartitionRange( firstRow: Int, lastRow: Int ) extends Serializable {
  def toRange: ma2.Range = new ma2.Range( firstRow, lastRow )
  def toRange( origin: Int ): ma2.Range = toRange.shiftOrigin( origin )
  override def toString = s"PR[${firstRow} ${lastRow}]"
}

class TimeSlicePartition(val varId: String, val varName: String, cdsection: CDSection, val fileInput: FileInput, val basePath: String, val partitionRange: PartitionRange ) extends Serializable with Loggable {
  import TimeSlicePartition._
  val filePath: String = if( basePath.isEmpty ) { fileInput.path } else { Paths.get( basePath, fileInput.path ).toString }
  override def toString = s"Partition{ Var[${varName}], ${fileInput.toString}, ${cdsection.toString}, ${partitionRange.toString}, localPartRange: ${partitionRange.toRange( fileInput.firstRowIndex ).toString} }"

  def getTimeSliceRange = {
    val localPartRange = partitionRange.toRange( fileInput.firstRowIndex )
    val interSect: ma2.Section = cdsection.toSection.replaceRange(0,localPartRange)
    val dataset = NetcdfDatasetMgr.aquireFile(filePath, 77.toString)
    val fileTimeAxis = NetcdfDatasetMgr.getTimeAxis(dataset) getOrElse { throw new Exception(s"Can't find time axis in data file ${filePath}") }
    val timeAxis: CoordinateAxis1DTime = fileTimeAxis.section( localPartRange )
    val slice0 = getSliceRanges(interSect, 0).head
    val slice1 = getSliceRanges(interSect, timeAxis.getShape(0)-1).head
    dataset.close()
    s"SliceRange[${slice0.first}:${slice1.first}]"
  }

  def getGlobalOrigin( localOrigin: Array[Int], timeIndexOffest: Int ):  Array[Int] =
    localOrigin.zipWithIndex map { case ( ival, index ) => if( index == 0 ) { ival + timeIndexOffest } else {ival} }

  def getSlices: Iterator[CDTimeSlice] = {
    val t0 = System.nanoTime()
    val dataset = NetcdfDatasetMgr.aquireFile(filePath, 77.toString)
    val variable: Variable = Option(dataset.findVariable(varName)).getOrElse { throw new Exception(s"Can't find variable $varName in data file ${filePath}") }
    val localPartRange = partitionRange.toRange( fileInput.firstRowIndex )
    val interSect: ma2.Section = cdsection.toSection.replaceRange(0,localPartRange)
    val fileTimeAxis = NetcdfDatasetMgr.getTimeAxis(dataset) getOrElse { throw new Exception(s"Can't find time axis in data file ${filePath}") }
    val timeAxis: CoordinateAxis1DTime = fileTimeAxis.section( localPartRange )
    val nTimesteps = timeAxis.getShape(0)
    val slices = for (slice_index <- 0 until nTimesteps; time_bounds = timeAxis.getCoordBoundsDate(slice_index).map( _.getMillis ) ) yield {
      val sliceRanges = getSliceRanges(interSect, slice_index)
      val data_section = variable.read(sliceRanges)
      val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
      val data_shape: Array[Int] = data_section.getShape
      val arraySpec = ArraySpec( getMissing(variable), data_section.getShape, getGlobalOrigin( interSect.getOrigin, fileInput.firstRowIndex ), data_array, None )
      val time_index = sliceRanges.head.first
      if( time_index < 11 ) { logger.info( s" @DSX time index: ${time_index}, value: ${data_array(0)}") }
      CDTimeSlice(time_bounds(0), time_bounds(1), Map(varId -> arraySpec), Map( "dims" -> variable.getDimensionsString ) )
    }
    dataset.close()
    logger.info(" [%s] Completed Read of %d timeSlices in %.4f sec, partitionRange = %s".format(KernelContext.getProcessAddress, nTimesteps, (System.nanoTime() - t0) / 1.0E9, partitionRange.toString ) )
    slices.toIterator
  }

  private def getSliceRanges( section: ma2.Section, slice_index: Int ): java.util.List[ma2.Range] = {
    section.getRanges.zipWithIndex map { case (range: ma2.Range, index: Int) =>
      if (index == 0) { new ma2.Range("time", range.first + slice_index, range.first + slice_index) } else { range } }
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
    if( indexEstimate >= dates.length ) { return dates.length-1 }
    try {
      val datesStartTime = dates(indexEstimate).getMillis
      if (timestamp < datesStartTime) { return _getDateIndex(timestamp, indexEstimate - 1) }
      if (indexEstimate >= nDates - 1) { return nDates - 1 }
      val datesEndTime = dates(indexEstimate + 1).getMillis
      if (timestamp < datesEndTime) {
        return indexEstimate
      }
      return _getDateIndex(timestamp, indexEstimate + 1)
    } catch {
      case ex: Exception =>
        throw ex
    }
  }
}

class TimeSliceGenerator(val varId: String, val varName: String, val section: String, val fileInput: FileInput, val optBasePath: Option[String] ) extends Serializable with Loggable {
  import ucar.nc2.time.CalendarPeriod.Field._
  val millisPerMin = 1000*60
  val filePath: String = optBasePath.fold( fileInput.path )( basePath => Paths.get( basePath, fileInput.path ).toString )
  val optSection: Option[ma2.Section] = CDSection.fromString(section).map(_.toSection)
  val dataset = NetcdfDatasetMgr.aquireFile( filePath, 77.toString )
  val variable: Variable = Option( dataset.findVariable( varName ) ).getOrElse { throw new Exception(s"Can't find variable $varName in data file ${filePath}") }
  val global_shape = variable.getShape()
  val metadata = variable.getAttributes.map(_.toString).mkString(", ")
  val missing: Float = getMissing( variable )
  val varSection = new ma2.Section( getOrigin( fileInput.firstRowIndex, global_shape.length ), global_shape )
  val interSect: ma2.Section = optSection.fold( varSection )( _.intersect(varSection) )
  val file_timeAxis: CoordinateAxis1DTime = NetcdfDatasetMgr.getTimeAxis( dataset ) getOrElse { throw new Exception(s"Can't find time axis in data file ${filePath}") }
  val dates: List[CalendarDate] = file_timeAxis.section( interSect.shiftOrigin(varSection).getRange(0) ).getCalendarDates.toList
  val datesBase: DatesBase = new DatesBase( dates )
  def close = dataset.close()
  def getSliceIndex( timestamp: Long ): Int = datesBase.getDateIndex( timestamp )
  def getOrigin( time_offset: Int, rank : Int ): Array[Int] = ( ( 0 until rank ) map { index => if( index == 0 ) time_offset else 0 } ).toArray

  def getSlice( template_slice: CDTimeSlice  ): CDTimeSlice = {
    def getSliceRanges( section: ma2.Section, slice_index: Int ): java.util.List[ma2.Range] = {
      section.getRanges.zipWithIndex map { case (range: ma2.Range, index: Int) => if( index == 0 ) { new ma2.Range("time",slice_index,slice_index)} else { range } }
    }
    val data_section = variable.read( getSliceRanges( interSect, getSliceIndex(template_slice.startTime)) )
    val data_array: Array[Float] = data_section.getStorage.asInstanceOf[Array[Float]]
    val data_shape: Array[Int] = data_section.getShape
    val arraySpec = ArraySpec( missing, data_section.getShape, interSect.getOrigin, data_array, None )
    CDTimeSlice(template_slice.startTime, template_slice.endTime, Map( varId -> arraySpec ), template_slice.metadata )
  }

  def getMissing( variable: Variable, default_value: Float = Float.NaN ): Float = {
    Seq( "missing_value", "fmissing_value", "fill_value").foreach ( attr_name => Option( variable.findAttributeIgnoreCase(attr_name) ).foreach( attr => return attr.getNumericValue.floatValue() ) )
    default_value
  }
}

class RDDContainer extends Loggable {
  private var _vault: Option[RDDVault] = None
  val regridKernel = new CDMSRegridKernel()
  def releaseBatch = { _vault.foreach(_.clear);  _vault = None }
  private def vault: RDDVault = _vault.getOrElse { throw new Exception( "Unexpected attempt to access an uninitialized RDD Vault")}
  def value: TimeSliceRDD = vault.value
  def nSlices = _vault.fold( 0L ) ( _.value.nSlices )
  def update: TimeSliceRDD = { _vault.foreach( _.value.exe ); value }
  def contents: Iterable[String] = _vault.fold( Iterable.empty[String] ) ( _.contents )
  def section( section: CDSection ): Unit = vault.map( _.section(section) )
  def release( keys: Iterable[String] ): Unit = { vault.release( keys ) }
  def variableRecs: Map[String,VariableRecord] = value.variableRecords

  private def initialize( init_value: TimeSliceRDD, contents: List[String] ) = {
    _vault = Some( new RDDVault( init_value ) )
  }

  class RDDVault( init_value: TimeSliceRDD ) {
    private var _rdd = init_value
    def update( new_rdd: TimeSliceRDD ): Unit = { _rdd = new_rdd }
    def map( f: (TimeSliceRDD) => TimeSliceRDD ): Unit = update( f(_rdd) )
    def value = _rdd
    def clear: Unit = _rdd.unpersist(false)
    def contents = _rdd.rdd.first().elements.keys
    def release( keys: Iterable[String] ) = { update( _rdd.release(keys) ) }
    def += ( record: CDTimeSlice ) = { update( _rdd.map( slice => slice ++ record ) ) }
    def += ( records: TimeSliceCollection  ) = {
      assert( records.nslices <= 1, "UNIMPLEMENTED FEATURE: TimeSliceCollection -> RDDVault")
      update( _rdd.map( slice => slice ++ records.slices.headOption.getOrElse( CDTimeSlice.empty ) ) )
    }
    def nSlices = { _rdd.cache; _rdd.nSlices }
    def nPartitions = {  _rdd.getNumPartitions }
    def nodeList = {  _rdd.nodeList }
  }
  def map( kernel: KernelImpl, context: KernelContext ): Unit = { vault.update( kernel.mapRDD( vault.value, context ) ) }

  def regrid( context: KernelContext ): Unit = {
    val t0 = System.nanoTime()
    vault.update( regridKernel.mapRDD( vault.value, context ) )
    if( KernelContext.workflowMode == WorkflowMode.profiling ) { update }
    logger.info(" #R# Regrid time: %.2f".format( (System.nanoTime-t0)/1.0E9 ) )
  }
  def execute( workflow: Workflow, node: KernelImpl, context: KernelContext, batchIndex: Int ): TimeSliceCollection = node.execute( workflow, value, context, batchIndex )
  def reduceBroadcast( node: KernelImpl, context: KernelContext, serverContext: ServerContext, batchIndex: Int ): Unit = vault.map( node.reduceBroadcast( context, serverContext, batchIndex ) )
  def nPartitions: Int = _vault.fold(0)(_.nPartitions)
  def nodeList: Array[String] = _vault.fold( Array.empty[String] )( _.nodeList )

  private def _extendRDD( generator: RDDGenerator, rdd: TimeSliceRDD, vSpecs: List[DirectRDDVariableSpec]  ): TimeSliceRDD = {
    if( vSpecs.isEmpty ) { rdd }
    else {
      val vspec = vSpecs.head
      val extendedRdd = generator.parallelize(rdd, vspec )
      _extendRDD( generator, extendedRdd, vSpecs.tail )
    }
  }

  def extendVault( generator: RDDGenerator, vSpecs: List[DirectRDDVariableSpec] ) = { vault.update( _extendRDD( generator, _vault.get.value, vSpecs ) ) }

  def addFileInputs( sparkContext: CDSparkContext, kernelContext: KernelContext, vSpecs: List[DirectRDDVariableSpec] ): Unit = {
    val newVSpecs = vSpecs.filter( vspec => ! contents.contains(vspec.uid) )
    if( newVSpecs.nonEmpty ) {
      val generator = new RDDGenerator( sparkContext, BatchSpec.nParts )
      val t0 = System.nanoTime
      val remainingVspecs = if( _vault.isEmpty ) {
        val tvspec = vSpecs.head
        val baseRdd: TimeSliceRDD = generator.parallelize( kernelContext, tvspec )
        initialize( baseRdd, List(tvspec.uid) )
        vSpecs.tail
      } else { vSpecs }
      val t1 = System.nanoTime
      extendVault( generator, remainingVspecs )
      if( KernelContext.workflowMode == WorkflowMode.profiling ) { update }
      val t2 = System.nanoTime
      logger.info( s"Generating file inputs with ${BatchSpec.nParts} partitions available, ${nPartitions} partitions created, inputs = [ ${vSpecs.map( _.uid ).mkString(", ")} ], BatchSpec = ${BatchSpec.toString}, times = { partition: ${(t1-t0)/1.0e9}, extend: ${(t2-t1)/1.0e9} }" )
//      logger.info(  s"nodes: \n  ${nodeList.mkString("\n  ")}" )
    }
  }


  def addOperationInput( inputs: TimeSliceCollection ): Unit = { vault += inputs }
}

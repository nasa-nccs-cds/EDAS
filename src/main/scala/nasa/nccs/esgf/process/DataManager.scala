package nasa.nccs.esgf.process
import java.io.{File, PrintWriter}

import com.googlecode.concurrentlinkedhashmap.{ConcurrentLinkedHashMap, Weigher}
import nasa.nccs.caching.RDDTransientVariable
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.{DirectRDDVariableSpec, HeapDblArray, HeapLongArray}
import nasa.nccs.cdapi.tensors.{CDByteArray, CDDoubleArray, CDFloatArray}
import nasa.nccs.edas.engine.{ExecutionCallback, Workflow, WorkflowContext, WorkflowNode}
import nasa.nccs.edas.engine.spark.CDSparkContext
import nasa.nccs.edas.kernels.{AxisIndices, KernelContext, WorkflowMode}
import nasa.nccs.edas.rdd.{CDRecordRDD, VariableRecord, _}
import nasa.nccs.edas.sources.{Aggregation, Collection, Collections}
import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.utilities._
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import ucar.nc2.time.{Calendar, CalendarDate, CalendarDateRange}
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D, CoordinateAxis1DTime, NetcdfDataset}

import scala.collection.immutable.Map
import scala.collection.concurrent
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.collection.mutable
import scala.collection.immutable.Map
import scala.util.control.Breaks._

sealed abstract class DataAccessMode
object DataAccessMode {
  case object Read extends DataAccessMode
  case object Cache extends DataAccessMode
  case object MetaData extends DataAccessMode
}

object FragmentSelectionCriteria extends Enumeration { val Largest, Smallest = Value }

trait DataLoader {
  def getExistingFragment( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]  ): Option[Future[PartitionedFragment]]
//  def cacheFragmentFuture( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode]  ): Future[PartitionedFragment];
  def deleteFragments( fragIds: Iterable[String] )
  def clearCache: Set[String]
}

trait ScopeContext {
  private lazy val __configuration__ = getConfiguration
  def getConfiguration: Map[String,String]
  def config( key: String, default: String ): String = __configuration__.getOrElse(key.toLowerCase,default)
  def config( key: String ): Option[String] = __configuration__.get(key.toLowerCase)
}
case class EDASCoordSystem( resolution: String, projection: String ) extends Serializable {
  def ==( other: EDASCoordSystem ): Boolean = resolution.equals( other.resolution ) && projection.equals( other.projection )
}

object RegridSpec {
  def apply( target_input: DataFragmentSpec  ): RegridSpec = {
    val vname = target_input.varname
    new RegridSpec( target_input.collection.getGridFilePath(vname), target_input.collection.getResolution(vname), target_input.collection.getGrid(vname).getProjection, target_input.cdsection.toSection.toString  )
  }
}

class RegridSpec( val gridFile: String, resolution: String, projection: String, val subgrid: String ) extends EDASCoordSystem( resolution, projection ) { }

object GenericOperationResult {
  def apply( rddResult: CDRecordRDD ): GenericOperationResult = GenericOperationResult( Some(rddResult), None )
  def apply( collectionResult: QueryResultCollection ): GenericOperationResult = GenericOperationResult( None, Some(collectionResult) )
  def empty = GenericOperationResult( None, None )

}
case class GenericOperationResult( optRDDResult: Option[CDRecordRDD], optCollectionResult: Option[QueryResultCollection] ) {
  val EMPTY = 0
  val RDD = 1
  val COLLECTION = 2
  def getType = if( optRDDResult.isDefined ) { RDD } else if ( optCollectionResult.isDefined ) { COLLECTION } else { EMPTY }
  def getSize: Long = getType match {             // Number of 4B elements
    case RDD => optRDDResult.get.getSize
    case COLLECTION => optCollectionResult.get.getSize
    case EMPTY => 0
  }
  def getWeight: Int = {  ( getSize / 250000 ).toInt }    // Memory size in MB
}

class ResultWeigher extends Weigher[GenericOperationResult] {
  def	weightOf(result: GenericOperationResult): Int = result.getWeight
}

object ResultCacheManager {
  private val resultMemoryCache: ConcurrentLinkedHashMap[ String, GenericOperationResult ] =
    new ConcurrentLinkedHashMap.Builder[String, GenericOperationResult ].initialCapacity(1000).maximumWeightedCapacity(1000).weigher( new ResultWeigher ).build()  //  Weight = memory size in MB

  def addResult( key: String, result: CDRecordRDD ) = resultMemoryCache.put( key, GenericOperationResult( result ) )
  def addResult( key: String, result: QueryResultCollection ) = resultMemoryCache.put( key, GenericOperationResult( result ) )
  def getResult( key: String ): GenericOperationResult = resultMemoryCache.getOrDefault( key, GenericOperationResult.empty )
}

class WorkflowExecutor(val requestCx: RequestContext, val workflowCx: WorkflowContext ) extends Loggable  {
  private var _inputsRDD: RDDContainer = new RDDContainer
  val rootNode = workflowCx.rootNode
  val rootNodeId = rootNode.getNodeId
  def getGridRefInput: Option[OperationDataInput] = workflowCx.getGridRefInput
  def contents: Set[String] = _inputsRDD.contents.toSet
  def getInputs(node: WorkflowNode): List[(String,OperationInput)] = node.operation.inputs.flatMap( uid => workflowCx.inputs.get( uid ).map ( uid -> _ ) )
  def getReduceOp(context: KernelContext): CDRecord.ReduceOp = rootNode.kernel.getReduceOp(context)
  def getTargetGrid: Option[TargetGrid] = workflowCx.getTargetGrid
  def releaseBatch: Unit = _inputsRDD.releaseBatch
  def getRegridSpec: Option[RegridSpec] = getGridRefInput.map( opInput => RegridSpec( opInput.fragmentSpec ) )
  def variableRecs: Map[String,VariableRecord] = _inputsRDD.variableRecs
  def nSlices: Long = _inputsRDD.nSlices
  def update: CDRecordRDD = _inputsRDD.update

  private def releaseInputs( node: WorkflowNode, kernelCx: KernelContext ): Unit = {
    val inputs =  getInputs(node)
    for( (uid,input) <- inputs ) input.consume( kernelCx.operation )
    val groupedInputs: Map[ Boolean, List[(String,OperationInput)] ] = inputs.groupBy { case (uid,input) => node.isDisposable( input ) }
    _inputsRDD.release( groupedInputs.getOrElse(true,Map.empty).map( _._1 ) )
//    _inputsRDD.cache( groupedInputs.getOrElse(false,Map.empty).map( _._1 ) )
  }

  def execute( workflow: Workflow, kernelCx: KernelContext, batchIndex: Int ): QueryResultCollection =  {
      val result = _inputsRDD.execute( workflow, rootNode.kernel, kernelCx, batchIndex )
      releaseInputs( rootNode, kernelCx )
      result
  }

  def streamMapReduce(node: WorkflowNode, kernelCx: KernelContext, serverContext: ServerContext, batchIndex: Int ) =  {
     _inputsRDD.map( node.kernel, kernelCx )
      releaseInputs( node, kernelCx )
      if( node.kernel.requiresReduceBroadcast( kernelCx ) ) {
        _inputsRDD.reduceBroadcast( node.kernel, kernelCx, serverContext, batchIndex )
      }
  }

  def addFileInputs( serverContext: ServerContext, kernelCx: KernelContext, vSpecs: List[DirectRDDVariableSpec], section: Option[CDSection], batchIndex: Int ): Unit = {
    _inputsRDD.addFileInputs( serverContext.spark, kernelCx, vSpecs )
  }

//  def regrid( kernelCx: KernelContext ): Unit = {
//    _inputsRDD.regrid( kernelCx )
//  }

  def extendRDD(generator: RDDGenerator, rdd: CDRecordRDD, vSpecs: List[DirectRDDVariableSpec]  ): CDRecordRDD = {
    if( vSpecs.isEmpty ) { rdd }
    else {
      val vspec = vSpecs.head
      val extendedRdd = generator.parallelize(rdd, vspec )
      extendRDD( generator, extendedRdd, vSpecs.tail )
    }
  }
  private def addOperationInput(serverContext: ServerContext, inputs: QueryResultCollection, batchIndex: Int ): Unit = {
    _inputsRDD.addOperationInput(inputs, s"Add Result, Request: ${requestCx.jobId}")
  }

  def addOperationInput(serverContext: ServerContext, inputs: QueryResultCollection, section: Option[CDSection], batchIndex: Int ): Unit  = {
    addOperationInput( serverContext, inputs, batchIndex )
//    section.foreach( section => _inputsRDD.section( section ) )
  }

}

class RequestContext( val jobId: String, val inputs: Map[String, Option[DataFragmentSpec]], val task: TaskRequest, private val configuration: Map[String,String], val executionCallback: Option[ExecutionCallback]=None ) extends ScopeContext with Loggable {
  def getConfiguration = configuration.map(identity)
  val domains: Map[String,DomainContainer] = task.domainMap
  val profiler: EventAccumulator = new EventAccumulator()

  def initializeProfiler( activationStatus: String, sc: SparkContext ) = {
    logger.info( s" #EA# Initializing profiler, configuration = ${configuration.mkString(",")}, task meta = ${task.metadata.mkString(",")}")
    profiler.setActivationStatus( activationStatus )
    try { sc.register( profiler, "EDAS_EventAccumulator" ) } catch { case ex: IllegalStateException => Unit }
    profiler.reset()
  }

  def getConf( key: String, default: String ) = configuration.getOrElse(key,default)
  def missing_variable(uid: String) = throw new Exception("Can't find Variable '%s' in uids: [ %s ]".format(uid, inputs.keySet.mkString(", ")))
  def getDataSources: Map[String, Option[DataFragmentSpec]] = inputs
  def getInputSpec( uid: String ): Option[DataFragmentSpec] = inputs.get( uid ).flatten
  def getInputSpec(): Option[DataFragmentSpec] = inputs.head._2
  def getCollection( uid: String = "" ): Option[Collection] = inputs.get( uid ) match {
    case Some(optInputSpec) => optInputSpec map { inputSpec => inputSpec.getCollection }
    case None =>inputs.head._2 map { inputSpec => inputSpec.getCollection }
  }
  def getTargetGrids: Map[String,Option[TargetGrid]] = inputs.mapValues( _.flatMap( _.targetGridOpt ) )
  def getSection( serverContext: ServerContext, uid: String = "" ): Option[ma2.Section] = inputs.get( uid ) match {
    case Some(optInputSpec) => optInputSpec map { _.roi }
    case None =>inputs.head._2 map { _.roi }
  }

  def getTimingReport(label: String): String = profiler.toString
  def logTimingReport(label: String): Unit = logger.info(getTimingReport(label))

  def saveTimingReport( filePath: String, label: String): Unit = {
    val pw = new PrintWriter( new File(filePath) )
    pw.write(getTimingReport(label))
    pw.close()
    logger.info(s"Saved Profiling data to '${filePath}'")
  }

  def getDomain(domain_id: String): DomainContainer = domains.get(domain_id) match {
    case Some(domain_container) => domain_container
    case None => throw new Exception("Undefined domain in ExecutionContext: " + domain_id)
  }

  def getTargetGridOpt( uid: String  ): Option[TargetGrid] = task.getTargetGrid( uid )
  def getTargetGridIds: Iterable[String] = getTargetGrids flatMap { case ( key, valOpt ) => valOpt map ( _ => key ) }
  def getTargetGrid( uid: String  ) = getTargetGridOpt(uid).getOrElse( throw new Exception("Missing target grid for kernel input " + uid + ", grids: " + getTargetGridIds.mkString( ", " ) ) )

  //  def getAxisIndices( axisConf: String ): AxisIndices = targetGrid.getAxisIndices( axisConf  )
}

object RangeCacheMaker {
  def create: mutable.Map[String, (Int,Int)] = { new mutable.HashMap[String, (Int,Int)] with mutable.SynchronizedMap[String, (Int,Int)] {} }
}

class GridCoordSpec( val index: Int, val grid: CDGrid, val agg: Aggregation, val coordAxis: CoordinateAxis1D, val domainAxisOpt: Option[DomainAxis] )  extends Serializable with Loggable {
  val t0 = System.nanoTime()
  private val _optRange: Option[ma2.Range] = getAxisRange
  val t1 = System.nanoTime()
  lazy val _spatialCoordValues = getSpaceCoordinateValues
  val t2 = System.nanoTime()
  private val _rangeCache: concurrent.TrieMap[String, (Int,Int)] = concurrent.TrieMap.empty[String, (Int,Int)]
  val t3 = System.nanoTime()
  val enable_range_caching = true;
//  logger.info( s" Created GridCoordSpec ${coordAxis.getFullName}, times = ${(t1-t0)/1.0E9} ${(t2-t1)/1.0E9} ${(t3-t2)/1.0E9} sec" )
  def getAxisType: AxisType = coordAxis.getAxisType

  def getCFAxisName: String = Option(coordAxis.getAxisType) match  {
    case Some( axisType ) => getAxisType.getCFAxisName
    case None => logger.warn( "Using %s for CFAxisName".format(coordAxis.getShortName) ); coordAxis.getShortName
  }


  def cacheRange( startDate: CalendarDate, endDate: CalendarDate, range: (Int,Int) ): Unit = _cacheRange( _rangeKey(startDate,endDate), range )
  def getCachedRange( startDate: CalendarDate, endDate: CalendarDate ): Option[(Int,Int)] = _getCachedRange( _rangeKey(startDate,endDate) )
  private def _getCachedRange( key: String ): Option[(Int,Int)] = _rangeCache.get(key)
  private def _rangeKey( startDate: CalendarDate, endDate: CalendarDate ): String = startDate.toString + '-' + endDate.toString
  private def _cacheRange( key: String, range: (Int,Int) ): Unit = _rangeCache.put(key,range)

  def getAxisName: String = coordAxis.getFullName
  def getIndexRange: Option[ma2.Range] = _optRange
  def getLength: Int = _optRange.fold( 0 )( _.length )
  def getStartValue: Double = if( coordAxis.getAxisType == AxisType.Time ) { agg.time_start } else { _spatialCoordValues.head }
  def getEndValue: Double = if( coordAxis.getAxisType == AxisType.Time ) { agg.time_end } else { _spatialCoordValues.last }
  def toXml: xml.Elem = <axis id={getAxisName} units={getUnits} cfName={getCFAxisName} type={getAxisType.toString} start={getStartValue.toString} end={getEndValue.toString} length={getLength.toString} > </axis>
  override def toString: String = "GridCoordSpec{id=%s units=%s cfName=%s type=%s start=%f end=%f length=%d}".format(getAxisName,getUnits,getCFAxisName,getAxisType.toString,getStartValue,getEndValue,getLength)
  def getMetadata: Map[String,String] = Map( "id"->getAxisName, "units"->getUnits, "name"->getCFAxisName, "type"->getAxisType.toString, "start"->getStartValue.toString, "end"->getEndValue.toString, "length"->getLength.toString )

  private def getAxisRange: Option[ma2.Range] = {
    val axis_len = coordAxis.getShape(0)
    domainAxisOpt match {
      case Some( domainAxis ) =>  domainAxis.system match {
        case asys if asys.startsWith("ind") =>
          val dom_start = domainAxis.start.toInt
          if( dom_start < axis_len ) {
            Some( new ma2.Range(getCFAxisName, dom_start, math.min( domainAxis.end.toInt, axis_len-1 ), 1) )
          } else None
        case asys if asys.startsWith("val") || asys.startsWith("time") =>
          getIndexBounds( domainAxis.start, domainAxis.end )
        case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
      }
      case None => Some( new ma2.Range( getCFAxisName, 0, axis_len-1, 1 ) )
    }
  }

  def getBounds( range: ma2.Range ): Array[Double]  = coordAxis.getAxisType match {
    case AxisType.Time => Array(agg.toTimeValue(range.first).index, agg.toTimeValue(range.last).index)
    case x => Array( _spatialCoordValues(range.first), _spatialCoordValues(range.last) )
  }

  def getSpaceCoordinateValues: Array[Double] =  _optRange match {
    case Some(range) => CDDoubleArray.factory( coordAxis.read(List(range)) ).getArrayData()
    case None =>        CDDoubleArray.factory( coordAxis.read() ).getArrayData()
  }

  def getUnits: String =  coordAxis.getAxisType match { case AxisType.Time => EDTime.units case x => coordAxis.getUnitsString }

  def getTimeAxis: CoordinateAxis1DTime = {
    val gridDS = NetcdfDatasetMgr.aquireFile( grid.gridFilePath, 17.toString )
    CoordinateAxis1DTime.factory( gridDS, coordAxis, new java.util.Formatter() )
  }

//  def getTimeCoordIndices( tvalStart: String, tvalEnd: String, strict: Boolean = false): Option[ma2.Range] = getTimeCoordIndices( tvalStart, tvalEnd, strict )
//    if( enable_range_caching ) getTimeCoordIndicesCached( tvalStart, tvalEnd, strict )
//    else getTimeCoordIndicesNonCached( tvalStart, tvalEnd, strict )

//  def getTimeCoordIndicesCached( tvalStart: String, tvalEnd: String, strict: Boolean = false): Option[ma2.Range] = {
//    val startDate: CalendarDate = cdsutils.dateTimeParser.parse(tvalStart)
//    val endDate: CalendarDate = cdsutils.dateTimeParser.parse(tvalEnd)
//    getBoundedCalDate( startDate, BoundsRole.Start, strict ) flatMap ( boundedStartDate =>
//      getBoundedCalDate( endDate, BoundsRole.End, strict) map ( boundedEndDate => {
//        val indices = getCachedRange( boundedStartDate, boundedEndDate ) match {
//          case Some( index_range ) => index_range
//          case None =>
//            val index_range = findTimeIndicesFromCalendarDates( boundedStartDate, boundedEndDate )
//            cacheRange( boundedStartDate, boundedEndDate, index_range )
//            index_range
//        }
//        new ma2.Range( getCFAxisName, indices._1, indices._2 )
//      })
//      )
//  }
//
//  def getTimeCoordIndicesNonCached1( tvalStart: String, tvalEnd: String, strict: Boolean = false): Option[ma2.Range] = {
//    val startDate: CalendarDate = cdsutils.dateTimeParser.parse(tvalStart)
//    val endDate: CalendarDate = cdsutils.dateTimeParser.parse(tvalEnd)
//    getBoundedCalDate( startDate, BoundsRole.Start, strict ) flatMap ( boundedStartDate =>
//      getBoundedCalDate( endDate, BoundsRole.End, strict) map ( boundedEndDate => {
//        val indices = findTimeIndicesFromCalendarDates(boundedStartDate, boundedEndDate)
//        new ma2.Range(getCFAxisName, indices._1, indices._2)
//      }))
//  }

  def getTimeCoordIndices( calendar: Calendar, tvalStart: String, tvalEnd: String, strict: Boolean = false): Option[ma2.Range] = {
    val startDate: CalendarDate = cdsutils.dateTimeParser.parse(calendar,tvalStart)
    val endDate: CalendarDate = cdsutils.dateTimeParser.parse(calendar,tvalEnd)
 //   logger.info( s" @DSX: getTimeCoordIndices: ${startDate.formatted("yyyy-MM-dd:HH")} <-> ${endDate.formatted("yyyy-MM-dd:HH")} ")
    findTimeIndicesFromCalendarDates( startDate, endDate ) map { case (start,end) => new ma2.Range( getCFAxisName, start, end ) }
  }

  def findTimeIndicesFromCalendarDates( start_date: CalendarDate, end_date: CalendarDate): Option[ ( Int, Int ) ] = agg.findRowIndicesFromCalendarDates( start_date, end_date )


  //  def getTimeIndexBounds( startval: String, endval: String, strict: Boolean = false) = getTimeCoordIndex( startval, BoundsRole.Start, strict).flatMap(startIndex =>
  //    getTimeCoordIndex( endval, BoundsRole.End, strict ).map( endIndex =>
  //      { val rv = new ma2.Range( getCFAxisName, startIndex, endIndex)
  //        print( " \n\n ----> GetTimeIndexBounds: start = %s: %d: %s, end = %s: %d: %s \n\n".format( startval.toString, startIndex, _dates.get(startIndex).toString, endval.toString, endIndex, _dates.get(endIndex).toString ) )
  //        print( " \n DATES: \n   %s \n".format( _dates.subList(startIndex,endIndex+1).map(_.toString).mkString("\n   ") ) )
  //        rv; }
  //    )
  //  )

  def getNormalizedCoordinate( cval: Double ) = coordAxis.getAxisType match {
    case nc2.constants.AxisType.Lon =>
      if( (cval<0.0) && ( coordAxis.getMinValue >= 0.0 ) ) cval + 360.0
      else if( (cval>180.0) && ( coordAxis.getMaxValue <= 180.0 ) ) cval - 360.0
      else cval
    case x => cval
  }

  def findCoordElement( coordAxis1D: CoordinateAxis1D, cval: Double ) = {   // work around ucar bug.
    if(coordAxis1D.isRegular) {
      val cvals =  coordAxis1D.getCoordValues
      assert( cvals.size > 0, "Empty coordinate axis: " + coordAxis1D.getFullName )
      if( cvals.size == 1 ) if( cval == cvals(0) ) 0 else -1 else {
        val start = cvals.head
        val incr = cvals(1) - cvals(0)
        val index = math.round( ( cval - start ) / incr ).toInt
        if( (index >= cvals.size) || (index < 0) ) -1 else index
      }
    } else coordAxis1D.findCoordElement( cval )
  }

  def getGridCoordIndex(cval: Double, role: BoundsRole.Value, strict: Boolean = true): Option[Int] = {
    val coordAxis1D = CDSVariable.toCoordAxis1D( coordAxis )
    val ncval = getNormalizedCoordinate( cval )
    findCoordElement( coordAxis1D, ncval ) match {
      case -1 =>
        val end_index = coordAxis1D.getSize.toInt - 1
        val grid_end = coordAxis1D.getCoordValue(end_index)
        val grid_start = coordAxis1D.getCoordValue(0)
        if (role == BoundsRole.Start) if( ncval > grid_end ) None else {
          val grid_start = coordAxis1D.getCoordValue(0)
          logger.warn("Axis %s: ROI Start value %f outside of grid area, resetting to grid start: %f".format(coordAxis.getFullName, ncval, grid_start))
          Some(0)
        } else if( ncval <  grid_start ) None else {
          logger.warn("Axis %s: ROI Start value %s outside of grid area, resetting to grid end: %f".format(coordAxis.getFullName, ncval, grid_end))
          Some(end_index)
        }
      case ival => Some(ival)
    }
  }
  def getNumericCoordValues( range: ma2.Range ) = {
    val coord_values = coordAxis.getCoordValues
    if (coord_values.length == range.length) coord_values else (0 until range.length).map(iE => coord_values(range.element(iE))).toArray
  }

  def getOffsetBounds( v0: Double, v1: Double ): ( Double, Double ) = {
    val offset0: Boolean = ( coordAxis.getAxisType.getCFAxisName == "X" ) && ( coordAxis.getCoordValue(0) >= 0 )
    val offset1: Boolean = ( coordAxis.getAxisType.getCFAxisName == "X" ) && ( coordAxis.getCoordValue( coordAxis.getSize.toInt-1 ) <= 180 )
    val r0: Double = if( offset0 && ( v0 < 0 ) ) { v0 + 360 } else if( offset1 && ( v0 > 180 ) ) { v0 - 360 } else v0
    val r1: Double = if( offset0 && ( v1 < 0 ) ) { v1 + 360 } else if( offset1 && ( v1 > 180 ) ) { v1 - 360 } else v1
    ( r0, r1 )
  }

  def getGridIndexBounds( v0: Double, v1: Double ): Option[ma2.Range] = {
    val coordAxis1D = CDSVariable.toCoordAxis1D( coordAxis )
    val axis_size = coordAxis1D.getSize.toInt
    val ( startval, endval ) = getOffsetBounds( v0, v1 )
//    logger.info( s" @O@ ${coordAxis.getAxisType.getCFAxisName} GetOffsetBounds: ($v0, $v1) -> ($startval, $endval), Axis Start: ${coordAxis.getBound1.head}" )
    val coordStartIndex = Math.max( coordAxis1D.findCoordElementBounded(startval) - 1, 0 )
    var startIndex = -1
    for(  coordIndex <- coordStartIndex until axis_size; cval = coordAxis1D.getCoordValue( coordIndex ); if cval >= startval ) {
      if( cval <= endval ) {
        if( startIndex == -1 ) { startIndex = coordIndex }
      } else {
        return if(startIndex == -1) { Some( new ma2.Range( coordIndex, coordIndex ) ) } else { Some( new ma2.Range( startIndex, coordIndex-1 ) ) }
      }
    }
    if( startIndex == -1 ) { None } else { Some( new ma2.Range( startIndex, axis_size - 1) ) }
  }

  def getIndexBounds( startval: GenericNumber, endval: GenericNumber, strict: Boolean = false): Option[ma2.Range] = {
//    logger.info( s" @DSX: getIndexBounds: ${startval.toString} <-> ${endval.toString} ")
    val rv = if (coordAxis.getAxisType == nc2.constants.AxisType.Time) getTimeCoordIndices( agg.calendar, startval.toString, endval.toString ) else getGridIndexBounds( startval, endval )
    rv
    //    assert(indexRange.last >= indexRange.first, "CDS2-CDSVariable: Coordinate bounds appear to be inverted: start = %s, end = %s".format(startval.toString, endval.toString))
    //    indexRange
  }
}

object GridSection extends Loggable {
  def apply( variable: CDSVariable, roiOpt: Option[List[DomainAxis]] ): GridSection = {
    val t0 = System.nanoTime
    val grid = variable.collection.getGrid( variable.name )
    val t1 = System.nanoTime
    val axes = variable.getCoordinateAxesList
    val t2 = System.nanoTime
    val coordSpecs: IndexedSeq[Option[GridCoordSpec]] = for (idim <- variable.dims.indices; dim = variable.dims(idim); coord_axis_opt = variable.getCoordinateAxis(dim)) yield coord_axis_opt match {
      case Some( coord_axis ) =>
        val domainAxisOpt: Option[DomainAxis] = roiOpt.flatMap(axes => axes.find(da => da.matches( coord_axis.getAxisType )))
        Some( new GridCoordSpec(idim, grid, variable.getAggregation, coord_axis, domainAxisOpt) )
      case None =>
        logger.warn( "Unrecognized coordinate axis: %s, axes = ( %s )".format( dim, grid.getCoordinateAxes.map( axis => axis.getFullName ).mkString(", ") )); None
    }
    val t3 = System.nanoTime
    val rv = new GridSection( grid, coordSpecs.flatten )
    val t4 = System.nanoTime
//    logger.info( " @GS@ GridSection: %.4f %.4f %.4f, %.4f, T = %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9, (t4-t3)/1.0E9, (t4-t0)/1.0E9 ) )
    rv
  }

  def apply( variable: CDSVariable, section: ma2.Section ): GridSection = {
    val grid_axes = section.getRanges.map( r => new DomainAxis( DomainAxis.fromCFAxisName(r.getName), r.first, r.last, "indices" ) )
    GridSection( variable, Some( grid_axes.toList ) )
  }
}

class  GridSection( val grid: CDGrid, val axes: IndexedSeq[GridCoordSpec] ) extends Serializable with Loggable {
  def getAxisSpec( dim_index: Int ): GridCoordSpec = axes(dim_index)
  def getAxisSpec( axis_type: AxisType ): Option[GridCoordSpec] = axes.find( axis => axis.getAxisType == axis_type )
  def getAxisSpec( domainAxis: DomainAxis ): Option[GridCoordSpec] = axes.find( axis => domainAxis.matches(axis.getAxisType ) )
  def getAxisSpec( cfAxisName: String ): Option[GridCoordSpec] = {
    val anames = axes map ( _.getCFAxisName )
    axes.find( axis => axis.getCFAxisName.toLowerCase.equals(cfAxisName.toLowerCase) )
  }
  def getRank = axes.length
  def toXml: xml.Elem = <grid> { axes.map(_.toXml) } </grid>
  def getGridSpec: String  = grid.getGridSpec
  def getGridFile: String  = grid.getGridFile
//  def getTimeCoordinateAxis: Option[CoordinateAxis1DTime] = grid.getTimeCoordinateAxis( "getTime")
  //  def getCalendarDate ( idx: Int ): CalendarDate = grid.getTimeCoordinateAxis match { case Some(axis) => axis.getCalendarDate(idx); case None => throw new Exception( "Can't get the time axis for grid " + grid.name ) }

//  def getCalendarDate ( idx: Int, context: String ): CalendarDate = grid.getTimeCoordinateAxis(context) match {
//    case Some(axis) =>
//      val testdate = axis.getCalendarDate(0);
//      try { axis.getCalendarDate(idx); }
//      catch {
//        case err: IndexOutOfBoundsException =>
//          throw err;
//      }
//    case None =>
//      throw new Exception("Can't get time axis for grid " + grid.name)
//  }

//  def getTimeRange: RecordKey = grid.getTimeCoordinateAxis("getTimeRange") match {
//    case Some(axis) =>
//      val ( t0, t1 ) = ( axis.getCalendarDate(0), axis.getCalendarDate( axis.getSize.toInt-1 ) )
//      RecordKey( t0.getMillis/1000, t1.getMillis/1000, 0, axis.getSize.toInt )
//    case None =>
//      throw new Exception("Can't get time axis for grid " + grid.name)
//  }

//  def getNextCalendarDate ( calendar: Calendar, axis: CoordinateAxis1DTime, idx: Int ): CalendarDate = {
//    if( (idx+1) < axis.getSize ) { axis.getCalendarDate(idx+1) }
//    else {
//      val (d0, d1) = (axis.getCalendarDate(idx - 1).getMillis, axis.getCalendarDate(idx).getMillis)
//      CalendarDate.of(calendar, d1 + (d1 - d0))
//    }
//  }

  def getSection: Option[ma2.Section] = {
    val ranges = for( axis <- axes ) yield
      axis.getIndexRange match {
        case Some( range ) =>
          range;
        case None =>
          return None
      }
    Some( new ma2.Section( ranges: _* ) )
  }
  def addRangeNames( section: ma2.Section ): ma2.Section = new ma2.Section( getRanges( section )  )
  def getRanges( section: ma2.Section ): IndexedSeq[ma2.Range] = for( ir <- section.getRanges.indices; r0 = section.getRange(ir); axspec = getAxisSpec(ir) ) yield new ma2.Range( axspec.getCFAxisName, r0 )

  def getSubSection( roi: List[DomainAxis] ): Option[ma2.Section] = {
    //    val coordSystem = grid.coordSystems.head
    //    val roi_axes = for( dim: Dimension  <- coordSystem.getDomain ) roi.find( da => da.getCoordAxisName.matches(dim.getShortName) ) match {
    //      case Some( roi_dim ) => roi_dim
    //      case None => dim
    //    }
    val ranges: IndexedSeq[ma2.Range] = for( gridCoordSpec <- axes ) yield {
      roi.find( _.matches( gridCoordSpec.getAxisType ) ) match {
        case Some( domainAxis ) => {
          val range = domainAxis.system match {
            case asys if asys.startsWith( "ind" )=> new ma2.Range(domainAxis.start.toInt, domainAxis.end.toInt)
            case asys if asys.startsWith( "val" ) || asys.startsWith("time") =>
              //             logger.info( "  %s getIndexBounds from %s".format( gridCoordSpec.toString, domainAxis.toString ) )
              gridCoordSpec.getIndexBounds( domainAxis.start, domainAxis.end ) match {
                case Some(ibnds) => new ma2.Range (ibnds.first, ibnds.last)
                case None => return None
              }
            case _ => throw new IllegalStateException("CDSVariable: Illegal system value in axis bounds: " + domainAxis.system)
          }
          //          val irange = gridCoordSpec.getIndexRange.intersect( range )
          new ma2.Range( gridCoordSpec.getCFAxisName, range.first, range.last, 1)
        }
        case None => gridCoordSpec.getIndexRange match {
          case Some( range ) => range
          case None => return None
        }
      }
    }
//    logger.info( s" @DSX: TG.getSubSection -> ${ranges(0).toString}")
    Some( new ma2.Section( ranges: _* ) )
  }
}

object CDSection {
  def apply( section: ma2.Section ): CDSection = new CDSection( section.getOrigin, section.getShape )
  def relative( section: ma2.Section ): CDSection = new CDSection( Array.fill(section.getRank)(0), section.getShape )
  def empty( rank: Int ): CDSection = new CDSection( Array.fill(rank)(0), Array.fill(rank)(0) )
  def serialize( section: ma2.Section): String = section.getRanges map ( r => r.getName + "," + r.first.toString + "," + r.last.toString ) mkString("+")
  def deserialize( section: String ): ma2.Section = new ma2.Section( section.split('+').map( rspec => { val sspec = rspec.split(','); new ma2.Range(sspec(0).trim,sspec(1).toInt,sspec(2).toInt) } ):_* )
  def merge( sect0: String, sect1: String ): String = CDSection.serialize( CDSection.deserialize(sect0).union( CDSection.deserialize(sect1) ) )
  def getRange( section: ma2.Section, axis: String ): Option[ma2.Range] = section.getRanges.find( _.getName == axis )
  def fromString( serializedSection: String  ): Option[CDSection] = if( serializedSection.isEmpty ) { None } else {
    val specs = serializedSection.split("[{}]")(1).split(':').map( _.split(',').map(_.toInt ) )
    Some( new CDSection( specs(0), specs(1) ) )
  }
}
class CDSection( origin: Array[Int], shape: Array[Int] ) extends Serializable {
  def toSection: ma2.Section = new ma2.Section( origin, shape )
  def toSection(offset: Array[Int]): ma2.Section = new ma2.Section( origin, shape ).shiftOrigin( new ma2.Section( offset, shape ) )
  def getRange( axis_index: Int ) = toSection.getRange(axis_index)
  def getShape = toSection.getShape
  def getOrigin = toSection.getOrigin
  override def toString() = "Section{%s:%s}".format( origin.mkString(","), shape.mkString(","))
  def merge( cdsect: CDSection ): CDSection = CDSection( cdsect.toSection.union( toSection ) )
}

object GridContext extends Loggable {
  def apply( uid: String, targetGrid: TargetGrid ) : GridContext = {
    val axisMap: Map[Char,Option[( Int, HeapDblArray )]] = Map( List( 'z', 'y', 'x' ).map( axis => axis -> targetGrid.getSpatialAxisData(axis) ):_* )
    val timeAxis: Option[ HeapLongArray ] = targetGrid.getTimeAxisData
    val cfAxisNames: Array[String] = ( 0 until targetGrid.getRank ).map( dim_index => targetGrid.getCFAxisName( dim_index ) ).toArray
    val axisIndexMap: Map[String,Int] = Map( cfAxisNames.map( cfAxisName => cfAxisName.toLowerCase -> targetGrid.getAxisIndex(cfAxisName) ):_* )
    new GridContext( uid, axisMap, timeAxis, cfAxisNames, axisIndexMap, targetGrid.collection.id, targetGrid.getGridFile )
  }
}

class GridContext(val uid: String, val axisMap: Map[Char,Option[( Int, HeapDblArray )]], val timeAxis: Option[ HeapLongArray ], val cfAxisNames: Array[String], val axisIndexMap: Map[String,Int], val collectionId: String, val gridFile: String ) extends Serializable {
  def getAxisIndices( axisConf: String ): AxisIndices = new AxisIndices( axisIds=axisConf.map( ch => getAxisIndex( ch.toString.toLowerCase ) ).toSet )
  def getAxisIndex( cfAxisName: String ): Int = axisIndexMap.getOrElse( cfAxisName, throw new Exception( "Unrecognized axis name ( should be 'x', 'y', 'z', or 't' ): " + cfAxisName ) )
  def getCFAxisName( dimension_index: Int ): String = cfAxisNames(dimension_index)
  def getTimeAxisData: Option[( HeapLongArray )] = timeAxis
  def getSpatialAxisData( axis: Char ): Option[( Int, HeapDblArray )] = axisMap.getOrElse( axis, None )
  def getSpatialAxisData( axis: Char, section: CDSection ): Option[( Int, ma2.Array )] = axisMap.getOrElse( axis, None ).map {
    case ( axis_index, array ) => ( axis_index, array.toUcarDoubleArray.section( List( section.getRange(axis_index) ) ) )
  }
  def getSpatialAxisData( axis: Char, section: Option[CDSection] ): Option[( Int, ma2.Array )] = section match {
    case Some(section) => getSpatialAxisData( axis, section );
    case None => getSpatialAxisData( axis ).map { case ( index, array ) => ( index, array.toUcarDoubleArray ) }
  }
  def coordValuesToIndices( axis: Char, values: Array[Float] ): ( Int, Array[Int]) = {
    if( axis == 't' ) {
      getTimeAxisData match {
        case Some(data) => 0 -> values.flatMap(x => data.findValue(x.toLong))
        case None => -1 -> Array.emptyIntArray
      }
    } else {
      getSpatialAxisData(axis) match {
        case Some((axisIndex,data)) =>
          axisIndex -> values.flatMap(x => data.findValue(x.toDouble))
        case None => -1 ->
          Array.emptyIntArray
      }
    }
  }
}

class TargetGrid( variable: CDSVariable, roiOpt: Option[List[DomainAxis]]=None ) extends CDSVariable( variable.name, variable.collection ) {
  val grid = GridSection( variable, roiOpt )
  val dbg = 1
  def toBoundsString = roiOpt.map( _.map( _.toBoundsString ).mkString( "{ ", ", ", " }") ).getOrElse("")
  def getRank = grid.getRank
  def getGridSpec: String  = grid.getGridSpec
  def getGridFile: String  = grid.getGridFile
//  def getCalendarDate ( idx: Int, context: String ): CalendarDate = grid.getCalendarDate(idx,context)
  def getDims: IndexedSeq[String] = grid.axes.map( _.coordAxis.getDimension(0).getFullName )

  def addSectionMetadata( section: ma2.Section ): ma2.Section = grid.addRangeNames( section )

  def getSubGrid( section: ma2.Section ): TargetGrid = {
    assert( section.getRank == grid.getRank, "Section with wrong rank for subgrid: %d vs %d ".format( section.getRank, grid.getRank) )
    val subgrid_axes = section.getRanges.map( r => new DomainAxis( DomainAxis.fromCFAxisName(r.getName), r.first, r.last, "indices" ) )
    new TargetGrid( variable, Some(subgrid_axes.toList)  )
  }

  //  def getAxisIndices( axisCFNames: String ): Array[Int] = for(cfName <- axisCFNames.toArray) yield grid.getAxisSpec(cfName.toString).map( _.index ).getOrElse(-1)

  //  def getPotentialAxisIndices( axisConf: List[OperationSpecs], flatten: Boolean = false ): AxisIndices = {
  //      val axis_ids = mutable.HashSet[Int]()
  //      for( opSpec <- axisConf ) {
  //        val axes = opSpec.getSpec("axes")
  //        val axis_chars: List[Char] = if( axes.contains(',') ) axes.split(",").map(_.head).toList else axes.toList
  //        axis_ids ++= axis_chars.map( cval => getAxisIndex( cval.toString ) )
  //      }
  //      val axisIdSet = if(flatten) axis_ids.toSet else  axis_ids.toSet
  //      new AxisIndices( axisIds=axisIdSet )
  //    }

  def getAxisIndices( axisConf: String ): AxisIndices = new AxisIndices( axisIds=axisConf.map( ch => getAxisIndex(ch.toString ) ).toSet )
  def getAxisIndex( cfAxisName: String ): Int = grid.getAxisSpec( cfAxisName.toLowerCase ).map( gcs => gcs.index ).getOrElse( throw new Exception( "Unrecognized axis name ( should be 'x', 'y', 'z', or 't' ): " + cfAxisName ) )
  def getCFAxisName( dimension_index: Int ): String = grid.getAxisSpec( dimension_index ).getCFAxisName

  def getAxisData( axis: Char, section: ma2.Section ): Option[( Int, ma2.Array )] = {
    grid.getAxisSpec(axis.toString).map(axisSpec => {
      val range = section.getRange(axisSpec.index)
      axisSpec.index -> axisSpec.coordAxis.read( List( range) )
    })
  }

  def getAxisData( axis: Char ): Option[( Int, ma2.Array )] = {
    grid.getAxisSpec(axis.toString).map(axisSpec => {
      axisSpec.index -> axisSpec.coordAxis.read()
    })
  }
  def getSpatialAxisData( axis: Char ): Option[( Int, HeapDblArray )] = {
    grid.getAxisSpec(axis.toString) match {
      case Some(axisSpec) => axisSpec.coordAxis.getAxisType match {
        case AxisType.Time => None
        case x => Some(axisSpec.index -> HeapDblArray(axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing))
      }
      case None => None
    }
  }

  def coordValuesToIndices( axis: Char, values: Array[Float] ): Array[Int] = {
    grid.getAxisSpec(axis.toString) match {
      case Some(axisSpec) => axisSpec.coordAxis.getAxisType match {
        case AxisType.Time =>
          val data =  HeapLongArray( axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing )
          values.flatMap( x => data.findValue(x.toLong) )
        case x =>
          val data = HeapDblArray(axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing)
          values.flatMap( x => data.findValue(x.toDouble) )
      }
      case None => Array.emptyIntArray
    }
  }

  def getTimeAxisData: Option[ HeapLongArray ] = {
    grid.getAxisSpec("t") match {
      case Some(axisSpec) => axisSpec.coordAxis.getAxisType match {
        case AxisType.Time => Some( HeapLongArray( axisSpec.coordAxis.read(), Array(0), axisSpec.getMetadata, variable.missing ) )
        case x => None
      }
      case None => None
    }
  }
  def getTimeCoordAxis: Option[ CoordinateAxis1D ] = grid.getAxisSpec("t").map( _.coordAxis )

  def getBounds( section: ma2.Section ): Option[Array[Double]] = {
    val xrangeOpt: Option[Array[Double]] = Option( section.find("X") ) flatMap ( (r: ma2.Range) => grid.getAxisSpec("X").map( (gs: GridCoordSpec) => gs.getBounds(r) ) )
    val yrangeOpt: Option[Array[Double]] = Option( section.find("Y") ) flatMap ( (r: ma2.Range) => grid.getAxisSpec("y").map(( gs: GridCoordSpec) => gs.getBounds(r) ) )
    xrangeOpt.flatMap( xrange => yrangeOpt.map( yrange => Array( xrange(0), xrange(1), yrange(0), yrange(1) )) )
  }

}

class ServerContext( val dataLoader: DataLoader, val spark: CDSparkContext )  extends ScopeContext with Serializable with Loggable  {
  def getVariable( collection: Collection, varname: String ): CDSVariable = collection.getVariable(varname)
  def getConfiguration: Map[String,String] = appParameters.getParameterMap

  def getOperationInput( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNode: WorkflowNode ): OperationInput = {
    dataLoader.getExistingFragment( fragSpec, partsConfig, Some(workflowNode) ) match {
      case Some( fragFut ) => Await.result( fragFut, Duration.Inf )
      case None => new EDASDirectDataInput( fragSpec, partsConfig, workflowNode )
      //        if(fragSpec.autoCache) {
//          val fragFut = cacheInputData( fragSpec, partsConfig, Some(workflowNode) )
//          Await.result( fragFut, Duration.Inf )
//        }
//        else { new EDASDirectDataInput( fragSpec, partsConfig, workflowNode ) }
    }
  }

  def getAxisData( fragSpec: DataFragmentSpec, axis: Char ): Option[( Int, ma2.Array )] = {
    val variable: CDSVariable = fragSpec.getVariable
    fragSpec.targetGridOpt.flatMap(targetGrid =>
      targetGrid.grid.getAxisSpec(axis.toString).map(axisSpec => {
        val range = fragSpec.roi.getRange(axisSpec.index)
        axisSpec.index -> axisSpec.coordAxis.read( List( range) )
      })
    )
  }

  //  def createTargetGrid( dataContainer: DataContainer, domainContainerOpt: Option[DomainContainer] ): TargetGrid = {
  //    val roiOpt: Option[List[DomainAxis]] = domainContainerOpt.map( domainContainer => domainContainer.axes )
  //    val t0 = System.nanoTime
  //    lazy val variable: CDSVariable = dataContainer.getVariable
  //    val t1 = System.nanoTime
  //    val rv = new TargetGrid( variable, roiOpt )
  //    val t2 = System.nanoTime
  //    logger.info( " CreateTargetGridT: %.4f %.4f ".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
  //    rv
  //  }

  //  def getAxes( fragSpec: DataFragmentSpec ) = {
  //    val variable: CDSVariable = dataLoader.getVariable( fragSpec.collection, fragSpec.varname )
  //    for( range <- fragSpec.roi.getRanges ) {
  //      variable.getCoordinateAxis()
  //    }
  //
  //  }
  //    val dataset: CDSDataset = getDataset( fragSpec.collection,  fragSpec.varname  )
  //    val coordAxes = dataset.getCoordinateAxes
  // //   val newCoordVars: List[GridCoordSpec] = (for (coordAxis <- coordAxes) yield inputSpec.getRange(coordAxis.getShortName) match {
  // //     case Some(range) => Some( new GridCoordSpec( coordAxis, range, ) ) )
  ////      case None => None
  ////    }).flatten
  //    dataset.getCoordinateAxes
  //  }

  //  def getSubset( fragSpec: DataFragmentSpec, new_domain_container: DomainContainer, dataAccessMode: DataAccessMode = DataAccessMode.Read ): PartitionedFragment = {
  //    val t0 = System.nanoTime
  //    val baseFragment = dataLoader.getFragment( fragSpec, dataAccessMode )
  //    val t1 = System.nanoTime
  //    val variable = getVariable( fragSpec.collection, fragSpec.varname )
  //    val targetGrid = fragSpec.targetGridOpt match { case Some(tg) => tg; case None => new TargetGrid( variable, Some(fragSpec.getAxes) ) }
  //    val newFragmentSpec = targetGrid.createFragmentSpec( variable, targetGrid.grid.getSubSection(new_domain_container.axes),  new_domain_container.mask )
  //    val rv = baseFragment.cutIntersection( newFragmentSpec.roi )
  //    val t2 = System.nanoTime
  //    logger.info( " GetSubsetT: %.4f %.4f".format( (t1-t0)/1.0E9, (t2-t1)/1.0E9 ) )
  //    rv
  //  }

  def createInputSpec( dataContainer: DataContainer, domain_container_opt: Option[DomainContainer],  request: TaskRequest ): (String, Option[DataFragmentSpec]) = {
    val t0 = System.nanoTime
    val data_source: DataSource = dataContainer.getSource
    val t1 = System.nanoTime
    val variable: CDSVariable = dataContainer.getVariable
    val t2 = System.nanoTime
    val targetGrid = request.getTargetGrid( dataContainer )
    val t3 = System.nanoTime
    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
    val fragRoiOpt = data_source.fragIdOpt.map( fragId => DataFragmentKey(fragId).getRoi )
    val domain_mdata = domain_container_opt.map( _.metadata ).getOrElse(Map.empty)
    val optSection: Option[ma2.Section] = fragRoiOpt match { case Some(roi) => Some(roi); case None => targetGrid.grid.getSection }
    val optDomainSect: Option[ma2.Section] = domain_container_opt.flatMap( domain_container => targetGrid.grid.getSubSection( domain_container.axes ) )
    val fragSpec: Option[DataFragmentSpec] = optSection map { section =>
      new DataFragmentSpec( dataContainer.uid, variable.name, variable.collection, data_source.fragIdOpt, Some(targetGrid), variable.dims.mkString(","),
        variable.units, variable.getAttributeValue("long_name", variable.fullname), section, optDomainSect, domain_mdata, variable.missing, variable.getAttributeValue("numDataFiles", "1").toInt, maskOpt, data_source.autoCache )
    }
    val rv = dataContainer.uid -> fragSpec
    logger.info( " LoadVariableDataT: section=%s, domainSect=%s, fragId=%s, fragRoi=%s, %.4f %.4f %.4f, T = %.4f ".format(
      optSection.getOrElse("null").toString, optDomainSect.getOrElse("null").toString, data_source.fragIdOpt.getOrElse("null").toString, fragRoiOpt.getOrElse("null").toString, (t1-t0)/1.0E9, (t2-t1)/1.0E9, (t3-t2)/1.0E9, (t3-t0)/1.0E9 ) )
    rv
  }

//  def cacheInputData( fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNodeOpt: Option[WorkflowNode] ): Future[PartitionedFragment] = {
//    logger.info( " ****>>>>>>>>>>>>> Cache Input Data: " + fragSpec.getKeyString )
//    dataLoader.getExistingFragment( fragSpec, partsConfig, workflowNodeOpt ) match {
//      case Some(partFut) => partFut
//      case None => dataLoader.cacheFragmentFuture( fragSpec, partsConfig, workflowNodeOpt )
//    }
//  }

  def deleteFragments( fragIds: Iterable[String] ) = {
    dataLoader.deleteFragments( fragIds )
  }

  def clearCache: Set[String] = dataLoader.clearCache

//  def cacheInputData( dataContainer: DataContainer, partsConfig: Map[String,String], domain_container_opt: Option[DomainContainer], targetGrid: TargetGrid, workflowNodeOpt: Option[WorkflowNode]  ): Option[( DataFragmentKey, Future[PartitionedFragment] )] = {
//    val data_source: DataSource = dataContainer.getSource
//    logger.info( "cacheInputData"  )
//    val variable: CDSVariable = dataContainer.getVariable
//    val maskOpt: Option[String] = domain_container_opt.flatMap( domain_container => domain_container.mask )
//    val optSection: Option[ma2.Section] = data_source.fragIdOpt match {
//      case Some(fragId) => Some(DataFragmentKey(fragId).getRoi);
//      case None => targetGrid.grid.getSection
//    }
//    val optDomainSect: Option[ma2.Section] = domain_container_opt.flatMap( domain_container => targetGrid.grid.getSubSection(domain_container.axes) )
//    val domain_mdata = domain_container_opt.map( _.metadata ).getOrElse(Map.empty)
//    if( optSection == None ) logger.warn( "Attempt to cache empty segment-> No caching will occur: " + dataContainer.toString )
//    optSection map { section =>
//      val fragSpec = new DataFragmentSpec( dataContainer.uid, variable.name, variable.collection, data_source.fragIdOpt, Some(targetGrid), variable.dims.mkString(","),
//        variable.units, variable.getAttributeValue("long_name", variable.fullname), section, optDomainSect, domain_mdata, variable.missing, variable.getAttributeValue("numDataFiles", "1").toInt, maskOpt, data_source.autoCache )
//      logger.info( "cache fragSpec: " + fragSpec.getKey.toString )
//      dataLoader.getExistingFragment(fragSpec, partsConfig, workflowNodeOpt) match {
//        case Some(partFut) =>
//          logger.info( "Found existing cached fragment to match: " + fragSpec.getKey.toString )
//          (fragSpec.getKey -> partFut)
//        case None =>
//          logger.info( "Creating new cached fragment: " + fragSpec.getKey.toString )
//          (fragSpec.getKey -> dataLoader.cacheFragmentFuture(fragSpec, partsConfig, workflowNodeOpt))
//      }
//    }
//  }
}



package nasa.nccs.edas.kernels

import java.io._
import java.net.{InetAddress, UnknownHostException}
import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}
import scala.xml
import nasa.nccs.caching.EDASPartitioner
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.{HeapFltArray, _}
import nasa.nccs.cdapi.tensors.CDFloatArray.{ReduceNOpFlt, ReduceOpFlt, ReduceWNOpFlt}
import nasa.nccs.cdapi.tensors.{CDArray, CDFloatArray}
import nasa.nccs.edas.engine.{EDASExecutionManager, Workflow, WorkflowNode}
import nasa.nccs.edas.rdd._
import nasa.nccs.edas.workers.TransVar
import nasa.nccs.edas.workers.python.{PythonWorker, PythonWorkerPortal}
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{EventAccumulator, Loggable}
import nasa.nccs.wps.{WPSProcess, WPSProcessOutput}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import ucar.ma2.IndexIterator
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable

object Port {
  def apply( name: String, cardinality: String, description: String="", datatype: String="", identifier: String="" ) = {
    new Port(  name,  cardinality,  description, datatype,  identifier )
  }
}

class Port( val name: String, val cardinality: String, val description: String, val datatype: String, val identifier: String ) extends Serializable {

  def toXml = {
    <port name={name} cardinality={cardinality}>
      { if ( description.nonEmpty ) <description> {description} </description> }
      { if ( datatype.nonEmpty ) <datatype> {datatype} </datatype> }
      { if ( identifier.nonEmpty ) <identifier> {identifier} </identifier> }
    </port>
  }
}

object EmptyFloatIterator {
  def apply(): Iterator[Float] = new EmptyFloatIterator
}

class EmptyFloatIterator extends Iterator[Float] {
  def hasNext: Boolean = false
  def next(): Float = { throw new Exception( "Next on empty float iterator") }
}

class AxisIndices( private val axisIds: Set[Int] = Set.empty ) extends Serializable {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
  override def toString = axisIds.mkString(",")
}

object WorkflowMode {
  val streaming = 0
  val profiling = 1
}

object KernelContext extends Loggable {
  private var _workflowMode = WorkflowMode.streaming

  def enableProfiling = {
    logger.info( "%PP% ENABLE PROFILING " )
    _workflowMode = WorkflowMode.profiling
  }
  def workflowMode = _workflowMode

  def apply( operation: OperationContext, executor: WorkflowExecutor ): KernelContext = {
    val sectionMap: Map[String, Option[CDSection]] = executor.requestCx.inputs.mapValues(_.map(_.cdsection)).map(identity)
    val gridMapVars: Map[String,Option[GridContext]] = executor.requestCx.getTargetGrids.map { case (uid,tgridOpt) =>
      uid -> tgridOpt.map( tg => GridContext(uid,tg))
    }
    val gridMapCols: Map[String,Option[GridContext]] = gridMapVars.flatMap { case ( uid, gcOpt ) => gcOpt.map( gc => ( gc.collectionId, Some(gc) ) ) }
    new KernelContext( operation, gridMapVars ++ gridMapCols, sectionMap, executor.requestCx.domains, executor.requestCx.getConfiguration, executor.workflowCx.crs, executor.getRegridSpec, executor.requestCx.profiler )
  }

  def relClockTime: Float = { (System.currentTimeMillis()/1000f)%10000f }

  def getHostAddress: String = try {
    val ip = InetAddress.getLocalHost
    s"${ip.getHostName}(${ip.getHostAddress})"
  } catch { case e: UnknownHostException => "UNKNOWN" }

  def getProcessAddress: String = try {
    val ip = InetAddress.getLocalHost
    val currentThread = Thread.currentThread
    s"${ip.getHostName}:T${currentThread.getId.toString}"
  } catch { case e: UnknownHostException => "UNKNOWN" }
}

class KernelContext( val operation: OperationContext, val grids: Map[String,Option[GridContext]], val sectionMap: Map[String,Option[CDSection]], val domains: Map[String,DomainContainer],
                     _configuration: Map[String,String], val crsOpt: Option[String], val regridSpecOpt: Option[RegridSpec], val profiler: EventAccumulator,
                     val _designatedRecordStartTime: Long = Long.MaxValue, private var _variableRecs: Map[String,VariableRecord] = Map.empty[String,VariableRecord] ) extends Loggable with Serializable with ScopeContext {
  import KernelContext._
  val trsOpt = getTRS
  val timings: mutable.SortedSet[(Float, String)] = mutable.SortedSet.empty
  val configuration: Map[String,String] = crsOpt.map(crs => _configuration + ("crs" -> crs)) getOrElse _configuration
  val _weightsOpt: Option[String] = operation.getConfiguration.get("weights")
  lazy val axes: AxisIndices = grid.getAxisIndices(config("axes", ""))
  def getGroup: Option[TSGroup]  = operation.config("groupBy") map TSGroup.getGroup
  def nonCyclicGroupOp: Boolean = getGroup.fold( false )( _.isNonCyclic )
  def getResultId( inputId: String ): String = operation.output(inputId)
  def setDesignatedRecord( record: CDRecord ): KernelContext = {
    new  KernelContext( operation, grids, sectionMap, domains, _configuration, crsOpt, regridSpecOpt, profiler, record.startTime, _variableRecs )
  }
  def isDesignatedRecord( record: CDRecord ): Boolean = { record.startTime == _designatedRecordStartTime }

  lazy val grid: GridContext = getTargetGridContext
  def addVariableRecords( varRecs: Map[String,VariableRecord] ): KernelContext = { _variableRecs = _variableRecs ++ varRecs; this }
  def getInputVariableRecord(vid: String): Option[VariableRecord] = _variableRecs.get(vid)

  def findGrid(gridRef: String): Option[GridContext] = grids.find(item => (item._1.equalsIgnoreCase(gridRef) || item._1.split('-')(0).equalsIgnoreCase(gridRef))).flatMap(_._2)

  def getConfiguration: Map[String, String] = configuration ++ operation.getConfiguration
  def addConfig( new_config_elems: (String,String)* ) = new KernelContext( operation, grids, sectionMap, domains, _configuration ++ new_config_elems, crsOpt, regridSpecOpt, profiler )

  def getReductionSize: Int = {
    val section: CDSection = sectionMap.head._2.getOrElse( throw new Exception(s"Can't find section for inputs of operation ${operation.identifier}") )
    getAxes.getAxes.map( axisIndex => section.getShape( axisIndex ) ).product
  }

  def getAxes: AxisIndices = axes

  def doesTimeOperations = axes.includes( 0 )

  def getContextStr: String = getConfiguration map { case (key, value) => key + ":" + value } mkString ";"

  def getDomainMetadata(domId: String): Map[String, String] = domains.get(domId) match {
    case Some(dc) => dc.metadata;
    case None => Map.empty
  }



  def findAnyGrid: GridContext = (grids.find { case (k, v) => v.isDefined }).getOrElse(("", None))._2.getOrElse(throw new Exception("Undefined grid in KernelContext for op " + operation.identifier))

  def getGridConfiguration(key: String): Option[String] = _configuration.get("crs").orElse(getDomains.flatMap(_.metadata.get("crs")).headOption)

  def getWeightMode: Option[String] = _weightsOpt

  def getDomains: List[DomainContainer] = operation.getDomains flatMap domains.get

  def getDomainSections: List[CDSection] = operation.getDomains.flatMap(sectionMap.get).flatten

  private def getCRS: Option[String] = getGridConfiguration("crs")

  private def getTRS: Option[String] = getGridConfiguration("trs")

//  def conf(params: Map[String, String]): KernelContext = new KernelContext(operation, grids, sectionMap, domains, configuration ++ params, crsOpt, regridSpecOpt, profiler)

  def commutativeReduction: Boolean = if (getAxes.includes(0)) { true } else { false }

  def doesTimeReduction: Boolean = getAxes.includes(0)

  private def getTargetGridContext: GridContext = crsOpt match {
    case Some(crs) =>
      if (crs.startsWith("~")) {
        findGrid(crs.substring(1)).getOrElse(throw new Exception(s"Unsupported grid specification '$crs' in KernelContext for op '$operation'"))
      }
      else if (crs.contains('~')) {
        findAnyGrid
      }
      else {
        throw new Exception("Currently unsupported crs specification")
      }
    case None => findAnyGrid
  }
}

case class ResultManifest( val name: String, val dataset: String, val description: String, val units: String ) {}

object Kernel extends Loggable {
  var profileTime: Float = 0f
  val customKernels = List[Kernel]( new CDMSRegridKernel() )
  def isEmpty( kvp: CDRecord ) = kvp.elements.isEmpty

  def getResultFile( resultId: String, deleteExisting: Boolean = false ): File = {
    val resultsDir = getResultDir
    val resultFile = new File( resultsDir.toString + s"/$resultId.nc" )
    if( deleteExisting && resultFile.exists ) resultFile.delete
    resultFile
  }

  def getResultDir: File = {
    val rawResultsDirPath = appParameters( "wps.shared.data.dir", appParameters("edas.results.dir", "~/.wps/results") )
    val resultsDirPath = rawResultsDirPath.replace( "~",  System.getProperty("user.home") ).replaceAll("[()]","-").replace("=","~")
    val resultsDir = new File(resultsDirPath); resultsDir.mkdirs()
    resultsDir
  }

  def apply(module: String, kernelSpec: String, api: String): Kernel = {
    val specToks = kernelSpec.split("[;]")
    customKernels.find(_.matchesSpecs( module, specToks.head )) match {
      case Some(kernel) =>
        kernel
      case None => api match {
        case "python" =>
          val options = str2Map(specToks(3))
          new zmqPythonKernel(module, specToks(0), specToks(1), specToks(2), options, false )
      }
      case wtf => throw new Exception("Unrecognized kernel api: " + api)
    }
  }

  private def str2Map( metadata: String ): Map[String,String] =
    Map( metadata.stripPrefix("{").stripSuffix("}").split("[,]").toSeq map { pair => pair.split("[:]") } map { a => ( a(0).replaceAll("[\"' ]",""), a(1).replaceAll("[\"' ]","") ) }: _* )

}

object KernelUtilities extends Loggable {
  def getWeights( inputId: String, context: KernelContext, weighting_type_opt: Option[String]=None, broadcast: Boolean = true ): CDFloatArray =  {
    val weighting_type = weighting_type_opt.getOrElse( context.config("weights", if (context.config("axes", "").contains('y')) "cosine" else "") )
    val t0 = System.nanoTime
    val weights = context.sectionMap.get( inputId ).flatten match {
      case Some(section) =>
        weighting_type match {
          case "cosine" =>
            context.grid.getSpatialAxisData('y', section) match {
              case Some(axis_data) => computeWeights( weighting_type, Map('y' -> axis_data), section.getShape, Float.MaxValue, broadcast )
              case None => logger.warn("Can't access AxisData for variable %s => Using constant weighting.".format(inputId)); CDFloatArray.const(section.getShape, 1f)
            }
          case x =>
            if (!x.isEmpty) { logger.warn("Can't recognize weighting method: %s => Using constant weighting.".format(x)) }
            CDFloatArray.const(section.getShape, 1f)
        }
      case None => CDFloatArray.empty
    }
    logger.info( "Computed weights in time %.4f s".format(  (System.nanoTime - t0) / 1.0E9 ) )
    weights
  }

  def computeWeights( weighting_type: String, axisDataMap: Map[ Char, ( Int, ma2.Array ) ], shape: Array[Int], invalid: Float, broadcast: Boolean ) : CDFloatArray  = {
    weighting_type match {
      case "cosine" =>
        axisDataMap.get('y') match {
          case Some( ( axisIndex, yAxisData ) ) =>
            val axis_length = yAxisData.getSize
            val axis_data =  CDFloatArray.factory( yAxisData.copy(), Float.MaxValue )
            assert( axis_length == shape(axisIndex), "Y Axis data mismatch, %d vs %d".format(axis_length,shape(axisIndex) ) )
            val cosineWeights: CDFloatArray = axis_data.map( x => Math.cos( Math.toRadians(x) ).toFloat )
            val base_shape: Array[Int] = Array( shape.indices.map(i => if(i==axisIndex) shape(axisIndex) else 1 ): _* )
            val weightsArray: CDArray[Float] =  CDArray( base_shape, cosineWeights.getStorage, invalid )
            if(broadcast) { weightsArray.broadcast( shape ) }
            weightsArray
          case None => throw new NoSuchElementException( "Missing axis data in weights computation, type: %s".format( weighting_type ))
        }
      case x => throw new NoSuchElementException( "Can't recognize weighting method: %s".format( x ))
    }
  }
}

class KIType { val Op = 0; val MData = 1 }

object PartSortUtils {
  implicit object PartSortOrdering extends Ordering[String] {
    def compare( k1: String, k2: String ) = k1.split('%')(1).toInt - k2.split('%')(1).toInt
  }
}

object KernelStatus {
  val public = 3;
  val restricted = 2;
  val developmental = 1;
  val experimental = 0;
  def parse( status: String ) = status.toLowerCase match {
    case x if x.startsWith("pub") => public
    case x if x.startsWith("res") => restricted
    case x if x.startsWith("dev") => developmental
    case x if x.startsWith("exp") => experimental
    case x => throw new Exception( "Unknown Kernel Status: " + status )
  }
}

object PostOpOperations {
  val normw = 0
  val sqrt = 2
  val rms = 3
  val keyMap = Map( "normw" -> normw, "sqrt" -> sqrt, "rms" -> rms )
  def get( key: String ): Int = keyMap.getOrElse( key.toLowerCase, throw new Exception(s"Unrecognized PostOp operation key: ${key}"))
}

abstract class Kernel( val options: Map[String,String] = Map.empty ) extends Loggable with Serializable with WPSProcess {
  val doesAxisReduction: Boolean
  val sampleInputs = false
  val identifiers = this.getClass.getName.split('$').flatMap(_.split('.'))
  val status = options.get("visibility").fold(KernelStatus.developmental)( opVal => KernelStatus.parse(opVal) )
  def operation: String = identifiers.last.toLowerCase
  def module: String = identifiers.dropRight(1).mkString(".")
  def id = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")
  val extInputs: Boolean = options.getOrElse("handlesInput","false").toBoolean
  val parallelizable: Boolean = options.getOrElse( "parallelize", (!extInputs).toString ).toBoolean
  logger.info( s" #PK# Create Kernel ${id}, status=${status}, parallelizable=${parallelizable}, options={ ${options.mkString("; ")} }")
  val identifier = name
  def matchesSpecs( _module: String, _operation: String ): Boolean = { _module.equals(module) && _operation.equals(operation) }
  val nOutputsPerInput: Int = options.getOrElse("nOutputsPerInput","1").toInt
  def cleanUp() = {}
  override def toString = s"Kernel[ id=${id} status=${status}]"
  def getOpName(context: KernelContext): String = "%s(%s)".format(name, context.operation.inputs.mkString(","))
  def toXmlHeader = <kernel module={module} name={name}>
    {if (title.nonEmpty) <title> {title} </title>}
    {if (description.nonEmpty) <description> {description} </description>}
  </kernel>

  def getWorkflowNodes( workflow: Workflow, operation: OperationContext ): List[WorkflowNode]

  def extractRegridOperation( workflow: Workflow, operation: OperationContext ): (OperationContext, Option[OperationContext]) = {
    val config = operation.getConfiguration
    val crs = config.getOrElse("crs","")

    if( config.keys.contains("grid") || crs.startsWith("~") ) {
      val opId = UID( operation.identifier.split('-').last )
      val rid = CDMSRegrid.resultId(workflow.request)
      val groupedConfig = config.groupBy { case (key, value) => CDMSRegrid.configKeys.contains(key.toLowerCase) }
      val regridOp = new OperationContext( opId + CDMSRegrid.kernelId, CDMSRegrid.kernelId, rid, operation.inputs, groupedConfig.getOrElse( true, Map.empty ) )
      val filteredOp =  new OperationContext( operation.identifier, operation.name, operation.rid, regridOp.outputs, groupedConfig.getOrElse( false, Map.empty ) )
      ( filteredOp,  Some(regridOp) )
    } else {
      ( operation, None )
    }
  }
}

abstract class MultiKernel( options: Map[String,String] = Map.empty ) extends Kernel(options) {
  def insertNewName( oldIdentifier: String, newName: String ): String = ( Array(newName) ++ oldIdentifier.split('-').tail ).mkString("-")

  def getWorkflowNodes( workflow: Workflow, operation: OperationContext ): List[WorkflowNode] = {
    val ( filtered_op, optRegridOperation ) = extractRegridOperation( workflow, operation )
    val expandedOps: List[OperationContext] = getExpandedOperations( workflow, filtered_op )
    val workflowNodes = expandedOps.flatMap ( op_context =>
      workflow.createKernel( op_context.name.toLowerCase ) match {
        case kernelImpl: KernelImpl => List( new WorkflowNode( op_context, kernelImpl ) )
        case multiKernel: MultiKernel => multiKernel.getWorkflowNodes( workflow, op_context )
      }
    )
    workflowNodes ++ optRegridOperation.map( regrid_op => WorkflowNode( regrid_op, workflow ) )
  }
  def getExpandedOperations( workflow: Workflow, operation: OperationContext ): List[OperationContext]
}

abstract class KernelImpl( options: Map[String,String] = Map.empty ) extends Kernel(options) {
  import Kernel._
  val weighted: Boolean
  def getInputArrays(inputs: CDRecord, context: KernelContext ): List[(String,ArraySpec)] = context.operation.inputs.flatMap(id => inputs.filterElements( id ))
  val mapCombineOp: Option[ReduceOpFlt] = options.get("mapOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  val mapCombineNOp: Option[ReduceNOpFlt] = None
  val mapCombineWNOp: Option[ReduceWNOpFlt] = None
  val reduceCombineOp: Option[ReduceOpFlt] = options.get("reduceOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  def requiresReduceBroadcast( context: KernelContext ): Boolean = { context.operation.operatesOnAxis('t' ) && reduceCombineOp.isDefined }
  val initValue: Float = 0f

  def getWorkflowNodes( workflow: Workflow, operation: OperationContext ): List[WorkflowNode] = {
    val ( filtered_op, optRegridOperation ) = extractRegridOperation( workflow, operation )
    List( new WorkflowNode( filtered_op, this ) ) ++ optRegridOperation.map( regrid_op => WorkflowNode( regrid_op, workflow ))
  }
  def execute(workflow: Workflow, input: CDRecordRDD, context: KernelContext, batchIndex: Int ): QueryResultCollection = { mapReduce(input, context, batchIndex ) }
  def isDisposable( input: OperationInput ): Boolean = input.disposable
  def elemFilter(rid: String) = (elemId: String) => elemId.toLowerCase.contains( rid )

  def map(context: KernelContext )( rec: CDRecord ): CDRecord

  def mapRDD(input: CDRecordRDD, context: KernelContext ): CDRecordRDD = {
    EDASExecutionManager.checkIfAlive
//    logger.info( s" @WW@ mapRDD: op: ${context.operation.identifier}, input elems: ${input.rdd.first.elements.keys.mkString(",")}, op inputs: ${context.operation.inputs.mkString(",")}")
    if( sampleInputs ) {
      val slices = input.rdd.collect()
      logger.info( s" @S@: Kernel ${id}.map Data Input Sample: \n  @S@:   ${slices.map ( _.elements.map{ case (key,array) =>
        s" $key: [ ${ array.data.mkString(", ") } ]" }.mkString("; ")).mkString("\n  @S@:   ") }")
    }
    val rv = input.map( map( context.setDesignatedRecord( input.first ) ) )
//    val msg = s" @WW@ mapRDD: op: ${context.operation.identifier}, output count: ${rv.rdd.count}, output slice shape: [ ${rv.rdd.first.elements.head._2.shape.mkString(", ")} ], elems = ${rv.rdd.first.elements.keys.mkString(",")}"
//    logger.info( msg )
    rv
  }

  def mapReduce(input: CDRecordRDD, context: KernelContext, batchIndex: Int, merge: Boolean = false ): QueryResultCollection = {
    val t0 = System.nanoTime()
//    logger.info(s" @WW@ mapReduce: ${context.operation.identifier}, inputs: ${input.rdd.first.elements.keys.mkString(",")} " )
    val mapresult: CDRecordRDD = context.profiler.profile("mapReduce.mapRDD") (() => { mapRDD(input, context) } )
    if( KernelContext.workflowMode == WorkflowMode.profiling ) { mapresult.exe }
    val rv = context.profiler.profile("mapReduce.reduce") ( () => {
      logger.info( s" #M# Beginning mapReduce, kernel =  ${context.operation.identifier} " )
      reduce( mapresult, context, batchIndex, merge || orderedReduce(context) )
    } )
    logger.info(" #M# Executed mapReduce, time: %.2f, metadata = { %s }".format( (System.nanoTime-t0)/1.0E9, rv.getMetadata.mkString("; ") ))
    rv
  }

  def reduce(input: CDRecordRDD, context: KernelContext, batchIndex: Int, ordered: Boolean = false ): QueryResultCollection = {
    EDASExecutionManager.checkIfAlive
    val rid = context.operation.rid.toLowerCase
    val postOpId: String = options.getOrElse( "postOp", "" )
    if( sampleInputs ) {
      val slices = input.rdd.collect()
      logger.info( s" @S@: Kernel ${id}.reduce Input Data Sample: \n  @S@:   ${slices.map ( _.elements.map{ case (key,array) =>
        s" $key: [ ${ array.data.mkString(", ") } ]" }.mkString("; ")).mkString("\n  @S@:   ") }")
    }
    if( !parallelizable ) { input.collect( elemFilter(rid), postOpId ) }
    else {
      val axes = context.getAxes
      val result: QueryResultCollection =  context.profiler.profile[QueryResultCollection]( "Kernel.reduce" ) (() => {
          input.reduce( getReduceOp(context), elemFilter(rid), postOpId, context.getGroup, ordered )
        })
      result
    }
  }

  def getReduceOp(context: KernelContext): CDRecord.ReduceOp = {
    if ( reduceCombineOp contains CDFloatArray.customOp ) {
      customReduceRDD(context)
    } else { reduceRDDOp(context) }
  }

  def reduceBroadcast(context: KernelContext, serverContext: ServerContext, batchIndex: Int )(input: CDRecordRDD): CDRecordRDD = {
    assert( batchIndex == 0, "reduceBroadcast is not supported over multiple batches")
    context.getGroup match {
      case Some( group ) =>
        val rid = context.operation.rid.toLowerCase
        val elemFilter = (elemId: String) => { elemId.startsWith( context.getResultId( elemId.split('-').head ) ) }
        val rv = input.reduceByGroup( getReduceOp(context), elemFilter, options.getOrElse("postOp",""), group )
        rv
      case None =>
        val groupOpt = context.getGroup
        val reducedCollection: QueryResultCollection = reduce( input, context, batchIndex )
        val result_slice = reducedCollection.records.head   // If there is no grouping then there will be a single result slice.
        val new_rdd =  input.rdd.map( _ ++ result_slice )
        new CDRecordRDD( new_rdd, input.metadata, input.variableRecords )
    }
  }


  /// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ End of public interface ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  private def filterInputs (input: CDRecordRDD, context: KernelContext ): CDRecordRDD = { input }

  protected def addWeights( context: KernelContext ): Boolean = {
    context.getWeightMode match {
      case Some( weights ) =>
        val axes = context.operation.getConfiguration("axes")
        if( weights == "cosine" ) { axes.indexOf( "y" ) > -1 }
        else throw new Exception( "Unrecognized weights type: " + weights )
      case None => false
    }
  }

  private def orderedReduce(context: KernelContext) = { reduceCombineOp.isEmpty || ! context.getAxes.includes(0) }

  private def combine(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices): DataFragment = reduceCombineOp match {
    case Some(combineOp) =>
      if (axes.includes(0)) DataFragment(a0.spec, CDFloatArray.combine(combineOp, a0.data, a1.data))
      else { a0 ++ a1 }
    case None => { a0 ++ a1 }
  }

  private def getParamValue( binnedArrayData: List[Map[Int,HeapFltArray]], paramId: String ): String = {
    val pvals = scala.collection.mutable.SortedSet[String]()
    for( binnedArrayMap <- binnedArrayData; if binnedArrayMap.size > 1 ) {
      val pval = binnedArrayMap.head._2.attr(paramId)
      pvals += pval
    }
    if( pvals.size > 1 ) { throw new Exception( " Multiple " + paramId + " definitions in getParamValue: " + pvals.mkString(" ") ) }
    pvals.head
  }

  private def getCombinedGridfile( inputs: Map[String,ArrayBase[Float]] ): String = {
    for ( ( id, array ) <- inputs ) array.metadata.get("gridfile") match { case Some(gridfile) => return gridfile; case None => Unit }
    throw new Exception( " Missing gridfile in kernel inputs: " + name )
  }

  private def weightedValueSumRDDCombiner( context: KernelContext)(a0: CDRecord, a1: CDRecord ): CDRecord = {
    val axes = context.getAxes
    val t0 = System.nanoTime
    val elems = a0.elements flatMap { case (key, data0) =>
      a1.elements.get( key ) match {
        case Some( data1 ) =>
          val wKey = key + "_WEIGHTS_"
          val vTot: FastMaskedArray = data0.toFastMaskedArray + data1.toFastMaskedArray
          val t1 = System.nanoTime
          val w0Opt: Option[ArraySpec] = a0.elements.get( wKey )
          val w1Opt: Option[ArraySpec] = a1.elements.get( wKey )
          val wTotOpt: Option[FastMaskedArray] = if(w0Opt.isDefined && w1Opt.isDefined) Option( w0Opt.get.toFastMaskedArray + w1Opt.get.toFastMaskedArray ) else None
          val t2 = System.nanoTime
          val resultList0 = List( key -> ArraySpec( vTot.missing, vTot.shape, data0.origin, vTot.getData, data0.optGroup ) )
          wTotOpt.fold( resultList0 )( wTot => resultList0 ++ List( wKey -> ArraySpec(wTot.missing, wTot.shape, data0.origin, wTot.getData, data0.optGroup ) ) )
        case None => logger.warn("Missing elemint in Record combine: " + key); List.empty[(String,ArraySpec)]
      }
    }
    val t3 = System.nanoTime
    new CDRecord(a0.mergeStart(a1), a0.mergeEnd(a1), elems, a0.metadata )
  }

  protected def combineRDD(context: KernelContext)(rec0: CDRecord, rec1: CDRecord ): CDRecord = {
    if( rec0.isEmpty ) { rec1 } else if (rec1.isEmpty) { rec0 } else context.profiler.profile[CDRecord]( "Kernel.combineRDD" ) (() => {
      val axes = context.getAxes
      val keys = rec0.elements.keys
      val new_elements: Iterator[(String, ArraySpec)] = rec0.elements.iterator flatMap { case (key0, array0) => rec1.elements.get(key0) match {
        case Some(array1) => reduceCombineOp match {
          case Some(combineOp) =>
            if (axes.includes(0)) Some( key0 -> array0.combine( combineOp, array1, weighted ) )
            else Some( key0 -> (array0 ++ array1) )
          case None =>
            Some( key0 -> (array0 ++ array1) )
        }
        case None =>
          None
      }}
      CDRecord(rec0.mergeStart(rec1), rec0.mergeEnd(rec1), TreeMap(new_elements.toSeq: _*), rec0.metadata )
    })
  }

  private def combineElements( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
    if( elements0.isEmpty ) { elements1.toIndexedSeq } else if (elements1.isEmpty) { elements0.toIndexedSeq } else {
      options.get("reduceOp") match {
        case Some( reduceOp ) =>
          if( reduceOp.toLowerCase == "sumw" ) {
            weightedSumReduction( key, elements0, elements1 )
          } else if( reduceOp.toLowerCase == "avew" ) {
            weightedAveReduction( key, elements0, elements1 )
          } else {
            throw new Exception( s"Unimplemented multi-input reduce op for kernel ${identifier}: " + reduceOp )
          }
        case None =>
          logger.warn( s"No reduce op defined for kernel ${identifier}, appending elements" )
          appendElements( key, elements0, elements1 )
      }
  }

  private def missing_element( key: String ) = throw new Exception( s"Missing element in weightedSumReduction for Kernel ${identifier}, key: " + key )

  private def getFloatBuffer( size: Int ): FloatBuffer = {
    val vbb: ByteBuffer = ByteBuffer.allocateDirect( size * 4 )
    vbb.order( ByteOrder.nativeOrder() );    // use the device hardware's native byte order
    vbb.asFloatBuffer();
  }

  private def weightedSumReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
    if( elements0.isEmpty ) { elements1.toIndexedSeq } else if (elements1.isEmpty) { elements0.toIndexedSeq } else {
      val key_lists = elements0.keys.partition( _.endsWith("_WEIGHTS_") )
      val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
      val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
      val weights0 = elements0.getOrElse( weights_key, missing_element(key) )
      val weights1 = elements1.getOrElse( weights_key, missing_element(key) )
      val values0 = elements0.getOrElse( values_key, missing_element(key) )
      val values1 = elements1.getOrElse( values_key, missing_element(key) )
      val t0 = System.nanoTime()
      val resultWeights = FloatBuffer.allocate( values0.data.length )
      val resultValues = FloatBuffer.allocate(  weights0.data.length )
      values0.missing match {
        case Some( undef ) =>
          for( index <- values0.data.indices; v0 = values0.data(index); v1 = values1.data(index) ) {
            if( v0 == undef || v0.isNaN ) {
              if( v1 == undef || v1.isNaN ) {
                resultValues.put( index, undef )
              } else {
                resultValues.put( index, v1 )
                resultWeights.put( index, weights1.data(index) )
              }
            } else if( v1 == undef || v1.isNaN ) {
              resultValues.put( index, v0 )
              resultWeights.put( index, weights0.data(index) )
            } else {
              val w0 = weights0.data(index)
              val w1 = weights1.data(index)
              resultValues.put( index, v0 + v1 )
              resultWeights.put( index,  w0 + w1 )
            }
          }
        case None =>
          for( index <- values0.data.indices ) {
            resultValues.put( values0.data(index) + values1.data(index) )
            resultWeights.put( weights0.data(index) + weights1.data(index) )
          }
      }
      val valuesArray =  HeapFltArray( CDFloatArray( values0.shape,  resultValues.array,  values0.missing.getOrElse(Float.MaxValue) ),  values0.origin,  values0.metadata,  values0.weights  )
      val weightsArray = HeapFltArray( CDFloatArray( weights0.shape, resultWeights.array, weights0.missing.getOrElse(Float.MaxValue) ), weights0.origin, weights0.metadata, weights0.weights )
      logger.info("Completed weightedSumReduction '%s' in %.4f sec, shape = %s".format(identifier, ( System.nanoTime() - t0 ) / 1.0E9, values0.shape.mkString(",") ) )
      IndexedSeq( values_key -> valuesArray, weights_key -> weightsArray )
    }

  private def weightedAveReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
    if( elements0.isEmpty ) { elements1.toIndexedSeq } else if (elements1.isEmpty) { elements0.toIndexedSeq } else {
      val key_lists = elements0.keys.partition( _.endsWith("_WEIGHTS_") )
      val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
      val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in weightedSumReduction for Kernel ${identifier}, keys: " + elements0.keys.mkString(",") ) )
      val weights0 = elements0.getOrElse( weights_key, missing_element(key) )
      val weights1 = elements1.getOrElse( weights_key, missing_element(key) )
      val values0 = elements0.getOrElse( values_key, missing_element(key) )
      val values1 = elements1.getOrElse( values_key, missing_element(key) )
      val t0 = System.nanoTime()
      val weightsSum = FloatBuffer.allocate( values0.data.length )
      val weightedValues0 = FloatBuffer.allocate(  values0.data.length )
      val weightedValues1 = FloatBuffer.allocate(  values0.data.length )
      values0.missing match {
        case Some( undef ) =>
          for( index <- values0.data.indices; v0 = values0.data(index); v1 = values1.data(index) ) {
            if( v0 == undef || v0.isNaN ) {
              if( v1 == undef || v1.isNaN ) {
                weightedValues0.put( index, undef )
                weightedValues1.put( index, undef )
              } else {
                weightedValues0.put( index, undef )
                weightedValues1.put( index, v1*weights1.data(index) )
                weightsSum.put( index, weights1.data(index) )
              }
            } else if( v1 == undef || v1.isNaN ) {
              weightedValues0.put( index, v0*weights0.data(index) )
              weightedValues1.put( index, undef )
              weightsSum.put( index, weights0.data(index) )
            } else {
              weightedValues0.put( index, values0.data(index) * weights0.data(index) )
              weightedValues1.put( index, values1.data(index) * weights1.data(index) )
              weightsSum.put( index, weights0.data(index) + weights1.data(index) )
            }
          }
          for( index <- values0.data.indices; wv0 = weightedValues0.get(index); wv1 = weightedValues1.get(index); ws = weightsSum.get(index) ) {
            if( wv0 == undef ) {
              if (wv1 == undef) { weightedValues0.put(index, undef) } else { weightedValues0.put(index, wv1/ws) }
            } else if (wv1 == undef) { weightedValues0.put(index, wv0 / ws) }
            else {
              weightedValues0.put( index,  (wv0 + wv1) / ws )
            }
          }
        case None =>
          for( index <- values0.data.indices ) {
            weightedValues0.put( index, values0.data(index) * weights0.data(index) )
            weightedValues1.put( index, values1.data(index) * weights1.data(index) )
            weightsSum.put( index, weights0.data(index) + weights1.data(index) )
          }
          for( index <- values0.data.indices ) {
            weightedValues0.put( index, (weightedValues0.get(index) + weightedValues1.get(index)) / weightsSum.get(index) )
          }
      }
      val valuesArray =  HeapFltArray( CDFloatArray( values0.shape,  weightedValues0.array,  values0.missing.getOrElse(Float.MaxValue) ),  values0.origin,  values0.metadata,  values0.weights  )
      val weightsArray = HeapFltArray( CDFloatArray( weights0.shape, weightsSum.array, weights0.missing.getOrElse(Float.MaxValue) ), weights0.origin, weights0.metadata, weights0.weights )
      logger.info("Completed weightedAveReduction '%s' in %.4f sec, shape = %s".format(identifier, ( System.nanoTime() - t0 ) / 1.0E9, values0.shape.mkString(",") ) )
      IndexedSeq( values_key -> valuesArray, weights_key -> weightsArray )
    }

  private def appendElements( key: String,  elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
    if( elements0.isEmpty ) { elements1.toIndexedSeq } else if (elements1.isEmpty) { elements0.toIndexedSeq } else {
      elements0 flatMap { case (key,fltArray) => elements1.get(key) map ( fltArray1 => key -> fltArray.append(fltArray1) ) } toIndexedSeq
    }

  private def customReduceRDD(context: KernelContext)(a0: CDRecord, a1: CDRecord ): CDRecord = if(Kernel.isEmpty(a0)) {a1} else if(Kernel.isEmpty(a1)) {a0} else {
    collectRDDOp(context)( a0, a1 )
  }

  private def reduceOp(context: KernelContext)(a0op: Option[DataFragment], a1op: Option[DataFragment]): Option[DataFragment] = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices(context.config("axes", ""))
    val rv = a0op match {
      case Some(a0) =>
        a1op match {
          case Some(a1) => Some(combine(context)(a0, a1, axes))
          case None => Some(a0)
        }
      case None =>
        a1op match {
          case Some(a1) => Some(a1)
          case None => None
        }
    }
    //    logger.info("Executed %s reduce op, time = %.4f s".format( context.operation.name, (System.nanoTime - t0) / 1.0E9 ) )
    rv
  }

  private def collectRDDOp(context: KernelContext)(a0: CDRecord, a1: CDRecord ): CDRecord = { a0 ++ a1 }


  private def reduceRDDOp(context: KernelContext)(a0: CDRecord, a1: CDRecord ): CDRecord =
    if( a0.isEmpty ) {
      a1
    } else  if(
      a1.isEmpty ) {
      a0
    } else {
      combineRDD(context)( a0, a1 )
    }

  private def getStringArg(args: Map[String, String], argname: String, defaultVal: Option[String] = None): String = {
    args.get(argname) match {
      case Some(sval) => sval
      case None => defaultVal match {
        case Some(sval) => sval;
        case None => throw new Exception(s"Parameter $argname (int) is reqired for operation " + this.id);
      }
    }
  }

  private def getIntArg(args: Map[String, String], argname: String, defaultVal: Option[Int] = None): Int = {
    args.get(argname) match {
      case Some(sval) => try {
        sval.toInt
      } catch {
        case err: NumberFormatException => throw new Exception(s"Parameter $argname must ba an integer: $sval")
      }
      case None => defaultVal match {
        case Some(ival) => ival
        case None => throw new Exception(s"Parameter $argname (int) is reqired for operation " + this.id);
      }
    }
  }

  private def getFloatArg(args: Map[String, String], argname: String, defaultVal: Option[Float] = None): Float = {
    args.get(argname) match {
      case Some(sval) => try {
        sval.toFloat
      } catch {
        case err: NumberFormatException => throw new Exception(s"Parameter $argname must ba a float: $sval")
      }
      case None => defaultVal match {
        case Some(fval) => fval
        case None => throw new Exception(s"Parameter $argname (float) is reqired for operation " + this.id);
      }
    }
  }

//  def getMontlyBinMap(id: String, context: KernelContext): CDCoordMap = {
//    context.sectionMap.get(id).flatten.map( _.toSection ) match  {
//      case Some( section ) =>
//        val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap( context.grid, section )
//        cdTimeCoordMap.getMontlyBinMap( section )
//      case None => throw new Exception( "Error, can't get section for input " + id )
//    }
//  }

}



abstract class SingularRDDKernel( options: Map[String,String] = Map.empty ) extends KernelImpl(options)  {
  override def map ( context: KernelContext ) ( inputs: CDRecord  ): CDRecord =  context.profiler.profile(s"SingularRDDKernel.map(${KernelContext.getProcessAddress}):${inputs.toString}")(() => {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val inputId: String = context.operation.inputs.headOption.getOrElse("NULL")
    val shape = inputs.elements.head._2.shape
    val inputsArrays = inputs.filterElements(inputId)
    assert( ! inputsArrays.isEmpty, "Missing input to '" + this.getClass.getName + "' map op: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    val elems = inputsArrays flatMap { case ( key, input_array ) => mapCombineOp match {
      case Some(combineOp) =>
        if( this.weighted ) {
          val suffixes = Seq( "", "_WEIGHTS_" )
          input_array.toFastMaskedArray.weightedReduce( combineOp, axes.args, initValue, None ).zip(suffixes).map { case (result, suffix) => {
            val result_data = result.getData
            context.operation.output(key) + suffix -> ArraySpec(input_array.missing, result.shape, input_array.origin, result_data, input_array.optGroup)
          }}
        } else {
          val result = input_array.toFastMaskedArray.reduce(combineOp, axes.args, initValue)
          val result_data = result.getData
          Seq( context.operation.output( key ) -> ArraySpec(input_array.missing, result.shape, input_array.origin, result_data, input_array.optGroup) )
        }
      case None =>
        //            logger.info(" ##### KERNEL [%s]: Map Op: NONE".format( name ) )
        Seq(context.operation.rid -> input_array )
    } }
    val dt = (System.nanoTime - t0) / 1.0E9
    CDRecord(inputs.startTime, inputs.endTime, inputs.elements ++ elems, inputs.metadata )
  })
}

abstract class CombineRDDsKernel(options: Map[String,String] ) extends KernelImpl(options)  {
  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = {
    if( mapCombineOp.isDefined ) {
      assert(inputs.elements.size > 1, "Missing input(s) to dual input operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))
      val input_arrays = getInputArrays( inputs, context )
      val grouped_input_arrays: Map[String, List[(String,ArraySpec)]] = input_arrays groupBy { case (uid,array) => uid.split('-').head }
      val results: Map[String,ArraySpec] = grouped_input_arrays.map {
        case ( vid, input_arrays ) =>
          context.operation.output( vid ) -> input_arrays.map(_._2).reduce( (a0,a1) => a0.combine( mapCombineOp.get, a1, weighted ) )
      }
      CDRecord(inputs.startTime, inputs.endTime, inputs.elements ++ results, inputs.metadata )
    } else { inputs }
  }
}

object CDMSRegrid {
  val configKeys = List( "grid", "shape", "res", "origin" )
  val kernelId = "python.cdmsmodule.regrid"
  def resultId( request: TaskRequest ) = request.id + "regrid"
}

class CDMSRegridKernel extends zmqPythonKernel( "python.cdmsmodule", "regrid", "Regridder", "Regrids the inputs using UVCDAT", Map( "parallelize" -> "True", "visibility" -> "public" ), false ) {
  override def extractRegridOperation( workflow: Workflow, operation: OperationContext ): (OperationContext, Option[OperationContext]) = ( operation, None )

  override def map ( context: KernelContext ) (inputs: CDRecord  ): CDRecord = context.profiler.profile(s"CDMSRegridKernel.map(${KernelContext.getProcessAddress})")(() => {
    val t0 = System.nanoTime
    val regridSpec: RegridSpec = context.regridSpecOpt.getOrElse(throw new Exception("Undefined target Grid in regrid operation"))

    val (acceptable_array_map, regrid_array_map) = context.profiler.profile(s"CDMSRegridKernel.PartitionInputs(${KernelContext.getProcessAddress})")(() => {
      if (context.operation.getConfParm("grid").isEmpty) {
        inputs.elements.partition { case (key, array) => context.getInputVariableRecord(key).fold(true)(_ == regridSpec) }
      } else {
        (Map.empty, inputs.elements)
      }
    })

    if (regrid_array_map.isEmpty) { inputs } else {

      val worker: PythonWorker = context.profiler.profile(s"CDMSRegridKernel.StartingPythonWorker(${KernelContext.getProcessAddress})")(() => {
        val optGridParm: Option[String] = context.operation.getConfParm("grid")
        val workerManager: PythonWorkerPortal = PythonWorkerPortal.getInstance
        logger.info(" #S#: Starting CDMSRegridKernel, inputs[%d] = [ %s ]".format(inputs.startTime, inputs.elements.keys.mkString(", ")))
        workerManager.getPythonWorker
      })

      context.profiler.profile(s"CDMSRegridKernel.SendingAcceptableArrays(${KernelContext.getProcessAddress})")(() => {
        val targetGrid: GridContext = context.grid
        for ((uid, input_array) <- acceptable_array_map) context.getInputVariableRecord(uid) foreach { varRec =>
          val data_array = input_array.toHeapFltArray(varRec.gridFilePath, Map("collection" -> targetGrid.collectionId, "name" -> varRec.varName, "dimensions" -> varRec.dimensions))
          logger.info(s" #S# Sending acceptable Array ${uid} data to python worker, shape = [ ${input_array.shape.mkString(", ")} ]\n ** varRec=${varRec.toString}\n ** metadata = { ${data_array.metadata.toString} }")
          worker.sendArrayMetadata(uid, data_array)
        }
      })

      context.profiler.profile(s"CDMSRegridKernel.SendingRegridArrays(${KernelContext.getProcessAddress})")(() => {
        for ((uid, input_array) <- regrid_array_map) context.getInputVariableRecord(uid) foreach { varRec => {
          val t10 = System.nanoTime
          val data_array = input_array.toHeapFltArray(varRec.gridFilePath, Map("collection" -> varRec.collection, "name" -> varRec.varName, "dimensions" -> varRec.dimensions))
          val t11 = System.nanoTime
          worker.sendRequestInput(uid, data_array)
          val t12 = System.nanoTime
          logger.info(s" #TS# Sending regrid Array ${uid} data to python worker, prep time = %.2f, send time = %.2f, shape = [ ${input_array.shape.mkString(", ")} ]\n ** varRec=${varRec.toString}\n ** metadata = { ${data_array.metadata.toString} }".format((t11 - t10) / 1.0E9, (t12 - t11) / 1.0E9))
        }}
      })

      val (gridFile, resultArrays) = context.profiler.profile(s"CDMSRegridKernel.WorkerExecution(${KernelContext.getProcessAddress})")(() => {
        val rID = UID()
        val context_metadata = indexAxisConf(context.getConfiguration, context.grid.axisIndexMap) + ("gridSpec" -> regridSpec.gridFile, "gridSection" -> regridSpec.subgrid, "createGridFile" -> context.isDesignatedRecord(inputs).toString )
        logger.info(s" RRR Sending regrid request to python worker, op=${context.operation.identifier}, rid = ${rID}, keys = [ ${regrid_array_map.keys.mkString(", ")} ], operation metadata: { ${context_metadata.mkString(", ")} }")
        worker.sendRequest("python.cdmsModule.regrid-" + rID, regrid_array_map.keys.toArray, context_metadata)
        var gFile = ""
        val resultItems: Iterable[(String, ArraySpec)] = for (uid <- regrid_array_map.keys) yield {
          val tvar = worker.getResult
          val result = ArraySpec(tvar)
          if (gFile.isEmpty) { gFile = Option( tvar.getMetaDataValue("gridfile") ).getOrElse("") }
          context.operation.output(uid) -> result
        }
        (gFile, resultItems)
      })

      val reprocessed_input_map = resultArrays.toMap
      val result_arrays = reprocessed_input_map ++ acceptable_array_map.map { case (key, value) => ( context.operation.output(key), value) }
      val rv = CDRecord( inputs.startTime, inputs.endTime, result_arrays, inputs.metadata + ("gridspec" -> gridFile) )
      logger.info(" #M# Gateway[T:%s]: Executed operation %s, time: %.2f".format(Thread.currentThread.getId, context.operation.identifier, (System.nanoTime - t0) / 1.0E9))
      rv
    }
  })
}

class zmqPythonKernel( _module: String, _operation: String, _title: String, _description: String, options: Map[String,String], axisElimination: Boolean  ) extends KernelImpl(options) {
  override def operation: String = _operation

  override def module = _module

  override def name = _module.split('.').last + "." + _operation

  override def id = _module + "." + _operation

  override val identifier = name
  val doesAxisReduction: Boolean = axisElimination;
  val weighted = false
  val outputs = List(WPSProcessOutput("operation result"))
  val title = _title
  val description = _description

  override def cleanUp(): Unit = PythonWorkerPortal.getInstance.shutdown()

  override def map(context: KernelContext)(inputs: CDRecord): CDRecord = {
    logger.info("&MAP: EXECUTING zmqPythonKernel, inputs = [ %s ]".format(name, inputs.elements.keys.mkString(", ") ) )
    val targetGrid: GridContext = context.grid
    val workerManager: PythonWorkerPortal = PythonWorkerPortal.getInstance()
    val worker: PythonWorker = workerManager.getPythonWorker
    try {
      val input_arrays: List[(String,ArraySpec)] = getInputArrays(inputs, context)
      val t1 = System.nanoTime
      for (input_id <- context.operation.inputs) inputs.element(input_id) match {
        case Some(input_array) =>
          val optVarRec: Option[VariableRecord] = context.getInputVariableRecord(input_id)
          val data_array = input_array.toHeapFltArray(targetGrid.gridFile, Map( "collection"->targetGrid.collectionId, "name"->optVarRec.fold("")(_.varName), "dimensions"->optVarRec.fold("")(_.dimensions)))
          worker.sendRequestInput( input_id, data_array )
        case None =>
          worker.sendUtility(List("input", input_id).mkString(";"))
      }
      val metadata = indexAxisConf(context.getConfiguration, context.grid.axisIndexMap) ++ Map("resultDir" -> Kernel.getResultDir.toString)
      worker.sendRequest(context.operation.identifier, context.operation.inputs.toArray, metadata)
      val resultItems: Seq[(String, ArraySpec)] = for (iInput <- 0 until (input_arrays.length * nOutputsPerInput)) yield {
        val tvar: TransVar = worker.getResult
        val uid = tvar.getMetaData.get("uid")
        val result = ArraySpec(tvar)
        context.operation.rid + ":" + uid + "~" + tvar.id() -> result
      }
      CDRecord(inputs.startTime, inputs.endTime, inputs.elements ++ resultItems, inputs.metadata )
    } finally {
      workerManager.releaseWorker(worker)
    }
  }
  def indexAxisConf( metadata: Map[String,String], axisIndexMap: Map[String,Int] ): Map[String,String] = {
    try {
      metadata.get("axes") match {
        case Some(axis_spec) =>
          val axisIndices = axis_spec.map( _.toString).map( axis => axisIndexMap(axis) )
          metadata + ( "axes" -> axisIndices.mkString(""))
        case None => metadata
      }
    } catch { case e: Exception => throw new Exception( "Error converting axis spec %s to indices using axisIndexMap {%s}: %s".format( metadata.get("axes"), axisIndexMap.mkString(","), e.toString ) )  }
  }
}

//  override def customReduceRDD(context: KernelContext)(a0: ( RecordKey, CDTimeSlice ), a1: ( RecordKey, CDTimeSlice ) ): ( RecordKey, CDTimeSlice ) = {
//    val ( rec0, rec1 ) = ( a0._2, a1._2 )
//    val ( k0, k1 ) = ( a0._1, a1._1 )
//    val t0 = System.nanoTime
//    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
//    val worker: PythonWorker = workerManager.getPythonWorker
//    val ascending = k0 < k1
//    val new_key = if(ascending) { k0 + k1 } else { k1 + k0 }
//    val op_metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap )
//    rec0.elements.map {
//      case (key, element0) =>  rec1.elements.get(key).map( element1 => key -> {
//        val (array0, array1) = if (ascending) (element0, element1) else (element1, element0)
//        val uids = Array( s"${array0.uid}", s"${array1.uid}" )
//        worker.sendRequestInput( uids(0), array0 )
//        worker.sendRequestInput( uids(1), array1 )
//        worker.sendRequest( context.operation.identifier, uids, Map( "action" -> "reduce", "axes" -> context.getAxes.getAxes.mkString(",") ) )
//      })
//    }
//    val resultItems = rec0.elements.map {
//      case (key, element0) =>
//        val tvar = worker.getResult
//        val result = HeapFltArray( tvar )
//        context.operation.rid + ":" + element0.uid -> result
//    }
//    logger.debug("&MERGE %s: finish, time = %.4f s".format( context.operation.identifier, (System.nanoTime - t0) / 1.0E9 ) )
//    context.addTimestamp( "Custom Reduce Op complete" )
//    new_key -> CDTimeSlice( resultItems, rec0.mergeMetadata("merge", rec1), rec0.partition )
//  }
//
//}

class TransientFragment( val dataFrag: DataFragment, val request: RequestContext, val varMetadata: Map[String,nc2.Attribute] ) extends OperationDataInput( dataFrag.spec, varMetadata.toMap ) {
  def toXml(id: String): xml.Elem = {
    val units = varMetadata.get("units") match { case Some(attr) => attr.getStringValue; case None => "" }
    val long_name = varMetadata.getOrElse("long_name",varMetadata.getOrElse("fullname",varMetadata.getOrElse("varname", new Attribute("varname","UNDEF")))).getStringValue
    val description = varMetadata.get("description") match { case Some(attr) => attr.getStringValue; case None => "" }
    val axes = varMetadata.get("axes") match { case Some(attr) => attr.getStringValue; case None => "" }
    <result id={id} missing_value={dataFrag.data.getInvalid.toString} shape={dataFrag.data.getShape.mkString("(",",",")")} units={units} long_name={long_name} description={description} axes={axes}> { dataFrag.data.mkBoundedDataString( ", ", 1100 ) } </result>
  }
  def domainDataFragment( partIndex: Int,  optSection: Option[ma2.Section]  ): Option[DataFragment] = Some(dataFrag)
  def data(partIndex: Int ): CDFloatArray = dataFrag.data
  def delete() = {;}
  override def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = { ; }
}

class SerializeTest {
  val input_array: CDFloatArray = CDFloatArray.const( Array(4), 2.5f )
  val ucar_array = CDFloatArray.toUcarArray( input_array )
  val byte_data = ucar_array.getDataAsByteBuffer().array()
  println( "Byte data: %x %x %x %x".format( byte_data(0),byte_data(1), byte_data(2), byte_data(3) ))
  val tvar = new TransVar( " | |0|4| ", byte_data, 0 )
  val result = HeapFltArray( tvar, None )
  println( "Float data: %f %f %f %f".format( result.data(0), result.data(1), result.data(2), result.data(3) ))
}

class zmqSerializeTest {
  import nasa.nccs.edas.workers.test.floatClient
  val input_array: CDFloatArray = CDFloatArray.const( Array(4), 2.5f )
  val ucar_array = CDFloatArray.toUcarArray( input_array )
  val byte_data = ucar_array.getDataAsByteBuffer().array()
  println( "Byte data: %d %d %d %d".format( byte_data(0),byte_data(1), byte_data(2), byte_data(3) ))
  floatClient.run( byte_data )
}

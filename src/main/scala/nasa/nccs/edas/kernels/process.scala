package nasa.nccs.edas.kernels

import java.io._
import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}

import nasa.nccs.caching.EDASPartitioner
import nasa.nccs.cdapi.cdm._
import nasa.nccs.cdapi.data.{HeapFltArray, _}
import nasa.nccs.cdapi.tensors.CDFloatArray.{ReduceNOpFlt, ReduceOpFlt, ReduceWNOpFlt}
import nasa.nccs.cdapi.tensors.{CDArray, CDCoordMap, CDFloatArray, CDTimeCoordMap}
import nasa.nccs.edas.engine.{EDASExecutionManager, Workflow, WorkflowNode}
import nasa.nccs.edas.engine.WorkflowNode.regridKernel
import nasa.nccs.edas.engine.spark.{CDSparkContext, RecordKey}
import nasa.nccs.edas.rdd.{ArraySpec, CDTimeSlice, TimeSliceCollection, TimeSliceRDD}
import nasa.nccs.edas.sources.Aggregation
import nasa.nccs.edas.workers.TransVar
import nasa.nccs.edas.workers.python.{PythonWorker, PythonWorkerPortal}
import nasa.nccs.edas.utilities.{appParameters, runtime}
import nasa.nccs.esgf.process._
import nasa.nccs.utilities.{Loggable, ProfilingTool}
import nasa.nccs.wps.{WPSProcess, WPSProcessOutput}
import org.apache.spark.rdd.RDD
import ucar.nc2.Attribute
import ucar.{ma2, nc2}

import scala.collection.JavaConversions._
import scala.collection.immutable.{SortedMap, TreeMap}
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

class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
  override def toString = axisIds.mkString(",")
}

object KernelContext {
  def apply( operation: OperationContext, executor: WorkflowExecutor ): KernelContext = {
    val sectionMap: Map[String, Option[CDSection]] = executor.requestCx.inputs.mapValues(_.map(_.cdsection)).map(identity)
    val gridMapVars: Map[String,Option[GridContext]] = executor.requestCx.getTargetGrids.map { case (uid,tgridOpt) =>
      uid -> tgridOpt.map( tg => GridContext(uid,tg))
    }
    val gridMapCols: Map[String,Option[GridContext]] = gridMapVars.flatMap { case ( uid, gcOpt ) => gcOpt.map( gc => ( gc.collectionId, Some(gc) ) ) }
    new KernelContext( operation, gridMapVars ++ gridMapCols, sectionMap, executor.requestCx.domains, executor.requestCx.getConfiguration, executor.workflowCx.crs, executor.getRegridSpec, executor.requestCx.profiler )
  }
}

class KernelContext( val operation: OperationContext, val grids: Map[String,Option[GridContext]], val sectionMap: Map[String,Option[CDSection]], val domains: Map[String,DomainContainer],  _configuration: Map[String,String], val crsOpt: Option[String], val regridSpecOpt: Option[RegridSpec], val profiler: ProfilingTool ) extends Loggable with Serializable with ScopeContext {
  val trsOpt = getTRS
  val timings: mutable.SortedSet[(Float, String)] = mutable.SortedSet.empty
  val configuration = crsOpt.map(crs => _configuration + ("crs" -> crs)) getOrElse _configuration
  val _weightsOpt: Option[String] = operation.getConfiguration.get("weights")

  lazy val grid: GridContext = getTargetGridContext

  def findGrid(gridRef: String): Option[GridContext] = grids.find(item => (item._1.equalsIgnoreCase(gridRef) || item._1.split('-')(0).equalsIgnoreCase(gridRef))).flatMap(_._2)

  def getConfiguration: Map[String, String] = configuration ++ operation.getConfiguration

  def getAxes: AxisIndices = grid.getAxisIndices(config("axes", ""))

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

  def conf(params: Map[String, String]): KernelContext = new KernelContext(operation, grids, sectionMap, domains, configuration ++ params, crsOpt, regridSpecOpt, profiler)

  def commutativeReduction: Boolean = if (getAxes.includes(0)) { true } else { false }

  def doesTimeReduction: Boolean = getAxes.includes(0)

  def addTimestamp(label: String, log: Boolean = false): Unit = {
    profiler.timestamp(label)
    if (log) {
      logger.info(label)
    }
  }

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

class AxisIndices( private val axisIds: Set[Int] = Set.empty ) {
  def getAxes: Seq[Int] = axisIds.toSeq
  def args = axisIds.toArray
  def includes( axisIndex: Int ): Boolean = axisIds.contains( axisIndex )
  override def toString = axisIds.mkString(",")
}

object Kernel extends Loggable {
  val customKernels = List[Kernel]( new CDMSRegridKernel() )
  def isEmpty( kvp: CDTimeSlice ) = kvp.elements.isEmpty

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
    customKernels.find(_.matchesSpecs(specToks)) match {
      case Some(kernel) => kernel
      case None => api match {
        case "python" => new zmqPythonKernel(module, specToks(0), specToks(1), specToks(2), str2Map(specToks(3)), false, "developmental" )
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
            val axis_data = CDFloatArray.factory( yAxisData, Float.MaxValue )
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

abstract class Kernel( val options: Map[String,String] = Map.empty ) extends Loggable with Serializable with WPSProcess {
  import Kernel._
  val identifiers = this.getClass.getName.split('$').flatMap(_.split('.'))
  val status = KernelStatus.developmental
  val doesAxisElimination: Boolean
  def operation: String = identifiers.last.toLowerCase
  def module: String = identifiers.dropRight(1).mkString(".")
  def id = identifiers.mkString(".")
  def name = identifiers.takeRight(2).mkString(".")
  val extInputs: Boolean = options.getOrElse("handlesInput","false").toBoolean
  val parallelizable: Boolean = options.getOrElse( "parallelize", (!extInputs).toString ).toBoolean
  val identifier = name
  def matchesSpecs( specs: Array[String] ): Boolean = { (specs.size >= 2) && specs(0).equals(module) && specs(1).equals(operation) }
  val nOutputsPerInput: Int = options.getOrElse("nOutputsPerInput","1").toInt
  def getInputArrays( inputs: CDTimeSlice, context: KernelContext ): List[ArraySpec] = context.operation.inputs.map( id => inputs.element( id ).getOrElse { throw new Exception(s"Can't find input ${id} for kernel ${identifier}") } )


  val mapCombineOp: Option[ReduceOpFlt] = options.get("mapOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  val mapCombineNOp: Option[ReduceNOpFlt] = None
  val mapCombineWNOp: Option[ReduceWNOpFlt] = None
  val reduceCombineOp: Option[ReduceOpFlt] = options.get("reduceOp").fold (options.get("mapreduceOp")) (Some(_)) map CDFloatArray.getOp
  val initValue: Float = 0f
  def cleanUp() = {}

  def mapRDD(input: TimeSliceRDD, context: KernelContext ): TimeSliceRDD = {
    EDASExecutionManager.checkIfAlive
    logger.info( "Executing map OP for Kernel " + id + "---> OP = " + context.operation.identifier  )
    input.map( map(context) )
  }

  def getReduceOp(context: KernelContext): (CDTimeSlice,CDTimeSlice)=>CDTimeSlice = {
    if (context.doesTimeReduction) {
      reduceCombineOp match {
        case Some(redOp) => redOp match {
          case CDFloatArray.customOp => customReduceRDD(context)
          case op => reduceRDDOp(context)
        }
        case None => reduceRDDOp(context)
      }
    } else {
      collectRDDOp(context)
    }
  }

  def execute( workflow: Workflow, input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    mapReduce(input, context, batchIndex )
  }

  def mapReduce(input: TimeSliceRDD, context: KernelContext, batchIndex: Int ): TimeSliceCollection = {
    val mapresult: TimeSliceRDD = mapRDD( input, context )
    logger.debug( "\n\n ----------------------- BEGIN reduce[%d] Operation: %s (%s): thread(%s) ----------------------- \n".format( batchIndex, context.operation.identifier, context.operation.rid, Thread.currentThread().getId ) )
    runtime.printMemoryUsage
    val t0 = System.nanoTime()
    val nparts = mapresult.getNumPartitions
    EDASExecutionManager.checkIfAlive
    evaluateProductSize( mapresult, context )
    if( !parallelizable || (nparts==1) ) { mapresult.collect }
    else {
      val result = mapresult reduce getReduceOp(context)
      logger.debug("\n\n ----------------------- FINISHED reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
      context.addTimestamp( "FINISHED reduce Operation" )
      result
    }
  }

  def evaluateProductSize(input: TimeSliceRDD, context: KernelContext ): Unit =  if ( !context.doesTimeReduction ) {
    val result_size: Long = input.dataSize
    logger.info( s" %E% Evaluating: Product size: ${result_size/1.0e9f} G, max product size: ${EDASPartitioner.maxProductSize/1.0e9f} G" )
    if( result_size > EDASPartitioner.maxProductSize ) { throw new Exception(s"The product of this request is too large: ${result_size/1e9} G, max product size:  ${EDASPartitioner.maxProductSize/1e9} G") }
  }

  def finalize( mapReduceResult: TimeSliceCollection, context: KernelContext ): TimeSliceCollection = {
    postRDDOp( mapReduceResult.sort, context  )
  }

  def addWeights( context: KernelContext ): Boolean = {
    context.getWeightMode match {
      case Some( weights ) =>
        val axes = context.operation.getConfiguration("axes")
        if( weights == "cosine" ) { axes.indexOf( "y" ) > -1 }
        else throw new Exception( "Unrecognized weights type: " + weights )
      case None => false

    }
  }


  def getOpName(context: KernelContext): String = "%s(%s)".format(name, context.operation.inputs.mkString(","))
  def map(context: KernelContext )( rec: CDTimeSlice ): CDTimeSlice
  def aggregate(context: KernelContext )( rec0: CDTimeSlice, rec1: CDTimeSlice ): CDTimeSlice = { rec0 }

  def combine(context: KernelContext)(a0: DataFragment, a1: DataFragment, axes: AxisIndices): DataFragment = reduceCombineOp match {
    case Some(combineOp) =>
      if (axes.includes(0)) DataFragment(a0.spec, CDFloatArray.combine(combineOp, a0.data, a1.data))
      else { a0 ++ a1 }
    case None => { a0 ++ a1 }
  }

  def getParamValue( binnedArrayData: List[Map[Int,HeapFltArray]], paramId: String ): String = {
    val pvals = scala.collection.mutable.SortedSet[String]()
    for( binnedArrayMap <- binnedArrayData; if binnedArrayMap.size > 1 ) {
      val pval = binnedArrayMap.head._2.attr(paramId)
      pvals += pval
    }
    if( pvals.size > 1 ) { throw new Exception( " Multiple " + paramId + " definitions in getParamValue: " + pvals.mkString(" ") ) }
    pvals.head
  }

  def getCombinedGridfile( inputs: Map[String,ArrayBase[Float]] ): String = {
    for ( ( id, array ) <- inputs ) array.metadata.get("gridfile") match { case Some(gridfile) => return gridfile; case None => Unit }
    throw new Exception( " Missing gridfile in kernel inputs: " + name )
  }
  
  def combineRDD(context: KernelContext)(rec0: CDTimeSlice, rec1: CDTimeSlice ): CDTimeSlice = {
    val t0 = System.nanoTime
    val axes = context.getAxes
    if( rec0.isEmpty ) { rec1 } else if (rec1.isEmpty) { rec0 } else {
      val keys = rec0.elements.keys
      val new_elements: Iterator[(String, ArraySpec)] = rec0.elements.iterator flatMap { case (key0, array0) => rec1.elements.get(key0) match {
        case Some(array1) => reduceCombineOp match {
          case Some(combineOp) =>
            if (axes.includes(0)) Some( key0 -> array0.combine( combineOp, array1 ) )
            else Some( key0 -> (array0 ++ array1) )
          case None => Some( key0 -> (array0 ++ array1) )
        }
        case None => None
      }}
      //    logger.debug("&COMBINE: %s, time = %.4f s".format( context.operation.name, (System.nanoTime - t0) / 1.0E9 ) )
      context.addTimestamp("combineRDD complete")
      CDTimeSlice( rec0.timestamp, rec0.dt + rec1.dt, TreeMap(new_elements.toSeq: _*) )
    }
  }

  def combineElements( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
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

  def missing_element( key: String ) = throw new Exception( s"Missing element in weightedSumReduction for Kernel ${identifier}, key: " + key )

  def getFloatBuffer( size: Int ): FloatBuffer = {
    val vbb: ByteBuffer = ByteBuffer.allocateDirect( size * 4 )
    vbb.order( ByteOrder.nativeOrder() );    // use the device hardware's native byte order
    vbb.asFloatBuffer();
  }

  def weightedSumReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
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

  def weightedAveReduction( key: String, elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
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

  def appendElements( key: String,  elements0: Map[String,HeapFltArray], elements1: Map[String,HeapFltArray] ): IndexedSeq[(String,HeapFltArray)] =
    if( elements0.isEmpty ) { elements1.toIndexedSeq } else if (elements1.isEmpty) { elements0.toIndexedSeq } else {
      elements0 flatMap { case (key,fltArray) => elements1.get(key) map ( fltArray1 => key -> fltArray.append(fltArray1) ) } toIndexedSeq
    }

  def customReduceRDD(context: KernelContext)(a0: CDTimeSlice, a1: CDTimeSlice ): CDTimeSlice = if(Kernel.isEmpty(a0)) {a1} else if(Kernel.isEmpty(a1)) {a0} else {
    logger.warn( s"No reducer defined for parallel op '$name', executing simple merge." )
    collectRDDOp(context)( a0, a1 )
  }

  def postOp(result: DataFragment, context: KernelContext): DataFragment = result

  def postRDDOp( pre_result: TimeSliceCollection, context: KernelContext ): CDTimeSlice = {
    options.get("postOp") match {
      case Some( postOp ) =>
        if( postOp == "normw") {
          val key_lists = pre_result.elements.keys.partition( _.endsWith("_WEIGHTS_") )
          val weights_key = key_lists._1.headOption.getOrElse( throw new Exception( s"Can't find weignts key in postRDDOp for Kernel ${identifier}, keys: " + pre_result.elements.keys.mkString(",") ) )
          val values_key  = key_lists._2.headOption.getOrElse( throw new Exception( s"Can't find values key in postRDDOp for Kernel ${identifier}, keys: " + pre_result.elements.keys.mkString(",") ) )
          val weights = pre_result.elements.getOrElse( weights_key, missing_element(weights_key) )
          val values = pre_result.elements.getOrElse( values_key, missing_element(values_key) )
          val averageValues = FloatBuffer.allocate(  values.data.length )
          values.missing match {
            case Some( undef ) =>
              for( index <- values.data.indices; value = values.data(index) ) {
                if( value == undef || value.isNaN  ) { undef }
                else {
                  val wval =  weights.data(index)
                  averageValues.put( value / wval )
                }
              }
            case None =>
              for( index <- values.data.indices ) { averageValues.put( values.data(index) / weights.data(index) ) }
          }
          val valuesArray =  HeapFltArray( CDFloatArray( values.shape,  averageValues.array,  values.missing.getOrElse(Float.MaxValue) ),  values.origin,  values.metadata,  values.weights  )
          context.addTimestamp( "postRDDOp complete" )
          new CDTimeSlice( TreeMap( values_key -> valuesArray ), pre_result.metadata, pre_result.partition )
        } else if( (postOp == "sqrt") || (postOp == "rms") ) {
          val new_elements = pre_result.elements map { case (values_key, values) =>
            val averageValues = FloatBuffer.allocate(values.data.length)
            values.missing match {
              case Some(undef) =>
                if( postOp == "sqrt" ) {
                  for (index <- values.data.indices; value = values.data(index)) {
                    if (value == undef || value.isNaN  ) { undef }
                    else { averageValues.put(Math.sqrt(value).toFloat) }
                  }
                } else if( postOp == "rms" ) {
                  val axes = context.config("axes", "").toUpperCase // values.metadata.getOrElse("axes","")
                  val roi: ma2.Section = CDSection.deserialize( values.metadata.getOrElse("roi","") )
                  val reduce_ranges = axes.flatMap( axis => CDSection.getRange( roi, axis.toString ) )
                  val norm_factor = reduce_ranges.map( _.length() ).fold(1)(_ * _) - 1
                  if( norm_factor == 0 ) { throw new Exception( "Missing or unrecognized 'axes' parameter in rms reduce op")}
                  for (index <- values.data.indices; value = values.data(index)) {
                    if (value == undef || value.isNaN  ) { undef }
                    else { averageValues.put(Math.sqrt(value/norm_factor).toFloat  ) }
                  }
                }
              case None =>
                if( postOp == "sqrt" ) {
                  for (index <- values.data.indices) {
                    averageValues.put(Math.sqrt(values.data(index)).toFloat)
                  }
                } else if( postOp == "rms" ) {
                  val norm_factor = values.metadata.getOrElse("N", "1").toInt - 1
                  if( norm_factor == 1 ) { logger.error( "Missing norm factor in rms") }
                  for (index <- values.data.indices) {
                    averageValues.put( Math.sqrt(values.data(index)/norm_factor).toFloat  )
                  }
                }
            }
            val newValuesArray = HeapFltArray(CDFloatArray(values.shape, averageValues.array, values.missing.getOrElse(Float.MaxValue)), values.origin, values.metadata, values.weights)
            ( values_key -> newValuesArray )
          }
          context.addTimestamp( "postRDDOp complete" )
          new CDTimeSlice( new_elements, pre_result.metadata, pre_result.partition )
        }
        else { throw new Exception( "Unrecognized postOp configuration: " + postOp ) }
      case None => pre_result
    }
  }

  def reduceOp(context: KernelContext)(a0op: Option[DataFragment], a1op: Option[DataFragment]): Option[DataFragment] = {
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

  def collectRDDOp(context: KernelContext)(a0: CDTimeSlice, a1: CDTimeSlice ): CDTimeSlice = { a0 ++ a1 }


  def reduceRDDOp(context: KernelContext)(a0: CDTimeSlice, a1: CDTimeSlice ): CDTimeSlice =
    if(Kernel.isEmpty(a0)) {a1} else if(Kernel.isEmpty(a1)) {a0} else { combineRDD(context)( a0, a1 ) }

  def getDataSample(result: CDFloatArray, sample_size: Int = 20): Array[Float] = {
    val result_array = result.floatStorage.array
    val start_value = result_array.size / 3
    result_array.slice(start_value, Math.min(start_value + sample_size, result_array.size))
  }

  def toXmlHeader = <kernel module={module} name={name}>
    {if (title.nonEmpty) <title> {title} </title>}
    {if (description.nonEmpty) <description> {description} </description>}
  </kernel>

  def getStringArg(args: Map[String, String], argname: String, defaultVal: Option[String] = None): String = {
    args.get(argname) match {
      case Some(sval) => sval
      case None => defaultVal match {
        case Some(sval) => sval;
        case None => throw new Exception(s"Parameter $argname (int) is reqired for operation " + this.id);
      }
    }
  }

  def getIntArg(args: Map[String, String], argname: String, defaultVal: Option[Int] = None): Int = {
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

  def getFloatArg(args: Map[String, String], argname: String, defaultVal: Option[Float] = None): Float = {
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

  def weightedValueSumRDDCombiner( context: KernelContext)(a0: CDTimeSlice, a1: CDTimeSlice ): CDTimeSlice = {
    val axes = context.getAxes
    if (axes.includes(0)) {
      val t0 = System.nanoTime
      val elems = a0.elements flatMap { case (key, data0) =>
        a1.elements.get( key ) match {
          case Some( data1 ) =>
//            logger.info(s"weightedValueSumRDDCombiner: element ${key}, shape0: ${data0.shape.mkString(",")}, shape1: ${data1.shape.mkString(",")}" )
            val vTot: FastMaskedArray = data0.toFastMaskedArray + data1.toFastMaskedArray
            val t1 = System.nanoTime
            val wTotOpt: Option[Array[Float]] = data0.toMa2WeightsArray flatMap { wtsArray0 => data1.toMa2WeightsArray map { wtsArray1 => (wtsArray0 + wtsArray1).toFloatArray } }
            val t2 = System.nanoTime
            val array_mdata = MetadataOps.mergeMetadata (context.operation.name) (data0.metadata, data1.metadata )
            Some( key -> HeapFltArray (vTot.toCDFloatArray, data0.origin, array_mdata, wTotOpt) )
          case None => logger.warn("Missing elemint in Record combine: " + key); None
        }
      }
      val part_mdata = MetadataOps.mergeMetadata( context.operation.name )( a0.metadata, a1.metadata )
      val t3 = System.nanoTime
      context.addTimestamp( "weightedValueSumCombiner complete" )
      new CDTimeSlice( elems, part_mdata, a0.partition )
    }
    else {
      a0 ++ a1
    }
  }


  def weightedValueSumRDDPostOp(result: CDTimeSlice, context: KernelContext): CDTimeSlice = {
    val new_elements = result.elements map { case (key, fltArray ) =>
      fltArray.toMa2WeightsArray match {
        case Some( wtsArray ) => (key, HeapFltArray( ( fltArray.toFastMaskedArray / wtsArray ).toCDFloatArray, fltArray.origin, fltArray.metadata, None ) )
        case None => (key, fltArray )
      }
    }
//    logger.info( "weightedValueSumPostOp:, Elems:" )
//    new_elements.foreach { case (key, heapFltArray) => logger.info(" ** key: %s, values sample = [ %s ]".format( key, heapFltArray.toCDFloatArray.mkBoundedDataString(", ",16)) ) }
    new CDTimeSlice( new_elements, result.metadata, result.partition )
  }

  def getMontlyBinMap(id: String, context: KernelContext): CDCoordMap = {
    context.sectionMap.get(id).flatten.map( _.toSection ) match  {
      case Some( section ) =>
        val cdTimeCoordMap: CDTimeCoordMap = new CDTimeCoordMap( context.grid, section )
        cdTimeCoordMap.getMontlyBinMap( section )
      case None => throw new Exception( "Error, can't get section for input " + id )
    }
  }

}



abstract class SingularRDDKernel( options: Map[String,String] = Map.empty ) extends Kernel(options)  {
  override def map ( context: KernelContext ) ( inputs: CDTimeSlice  ): CDTimeSlice = {
    val t0 = System.nanoTime
    val axes: AxisIndices = context.grid.getAxisIndices( context.config("axes","") )
    val inputId: String = context.operation.inputs.headOption.getOrElse("NULL")
    val shape = inputs.elements.head._2.shape
    logger.debug(" ##### KERNEL [%s]: Map Op: combine, input shape = %s".format( name, shape.mkString(",") ) )
    val elem = inputs.element(inputId) match {
      case Some( input_array ) =>
        mapCombineOp match {
          case Some(combineOp) =>
            val result = input_array.toFastMaskedArray.reduce(combineOp, axes.args, initValue)
//            logger.info( "Input data sample = [ %s ]".format(cdinput.toCDFloatArray.getArrayData(30).map( _.toString ).mkString(", ") ) )
            val result_data = result.getData
            logger.info(" ##### KERNEL [%s]: Map Op: combine, axes = %s, result shape = %s, result value[0] = %.4f".format( name, axes, result.shape.mkString(","), result_data(0) ) )
            ArraySpec( input_array.missing, result.shape, input_array.origin, result_data )
          case None =>
            logger.info(" ##### KERNEL [%s]: Map Op: NONE".format( name ) )
            input_array
        }
      case None => throw new Exception( "Missing input to '" + this.getClass.getName + "' map op: " + inputId + ", available inputs = " + inputs.elements.keySet.mkString(",") )
    }
    val dt = (System.nanoTime - t0) / 1.0E9
    logger.info("Executed Kernel %s map op, time = %.4f s".format(name, dt ))
    context.addTimestamp( "Map Op complete, time = %.4f s, shape = (%s)".format( dt, shape.mkString(",") ) )
    CDTimeSlice(  inputs.timestamp, inputs.dt, inputs.elements ++ Seq(context.operation.rid -> elem) )
  }
}

abstract class CombineRDDsKernel(options: Map[String,String] ) extends Kernel(options)  {
  override def map ( context: KernelContext ) (inputs: CDTimeSlice  ): CDTimeSlice = {
    if( mapCombineOp.isDefined ) {
      val t0 = System.nanoTime
      assert(inputs.elements.size > 1, "Missing input(s) to dual input operation " + id + ": required inputs=(%s), available inputs=(%s)".format(context.operation.inputs.mkString(","), inputs.elements.keySet.mkString(",")))
      val input_arrays: List[ArraySpec] = getInputArrays( inputs, context )
      val result_array: ArraySpec = input_arrays.reduce( (a0,a1) => a0.combine( mapCombineOp.get, a1 ) )
      logger.info("Executed Kernel %s map op, time = %.4f s".format(name, (System.nanoTime - t0) / 1.0E9))
      context.addTimestamp("Map Op complete")
      CDTimeSlice(  inputs.timestamp, inputs.dt, inputs.elements ++ Seq(context.operation.rid -> result_array) )
    } else { inputs }
  }
}



//class CDMSRegridKernel extends zmqPythonKernel( "python.cdmsmodule", "regrid", "Regridder", "Regrids the inputs using UVCDAT", Map( "parallelize" -> "True" ), false, "restricted" ) {
//
//  override def map ( context: KernelContext ) (inputs: CDTimeSlice  ): CDTimeSlice = {
//    val t0 = System.nanoTime
//    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance
//    val worker: PythonWorker = workerManager.getPythonWorker
//    try {
//      logger.info("&MAP: Starting Kernel %s, inputs = [ %s ]".format(name, inputs.elems.mkString(", ") ) )
//      val targetGrid: GridContext = context.grid
//      val regridSpec: RegridSpec = context.regridSpecOpt.getOrElse( throw new Exception( "Undefined target Grid in regrid operation"))
//      val ( input_array_map, passthrough_array_map ) = inputs.elements.partition { case ( key, array ) => context.operation.inputs.contains(key) }
//      val input_arrays: List[ArraySpec] = context.operation.inputs.map( id => inputs.element( id ).getOrElse { throw new Exception(s"Can't find input ${id} for kernel ${identifier}") } )
//
//      val (acceptable_arrays, regrid_arrays) = op_input_arrays.partition(_.gridFilePath.equals(regridSpec.gridFile))
//      if (regrid_arrays.isEmpty) { inputs } else {
//        for (input_array <- acceptable_arrays) { worker.sendArrayMetadata( input_array.uid, input_array) }
//        for (input_array <- regrid_arrays) {
//          logger.info( s"Sending Array ${input_array.uid} data to python worker, shape = [ ${input_array.shape.mkString(", ")} ]")
//          worker.sendRequestInput( input_array.uid, input_array )
//        }
//        val acceptable_array_map = Map(acceptable_arrays.map(array => array.uid -> array): _*)
//
//        logger.info("Gateway: Executing operation %s".format( context.operation.identifier ) )
//        val context_metadata = indexAxisConf(context.getConfiguration, context.grid.axisIndexMap) + ("gridSpec" -> regridSpec.gridFile, "gridSection" -> regridSpec.subgrid )
//        val rID = UID()
//        worker.sendRequest("python.cdmsModule.regrid-" + rID, regrid_arrays.map(_.uid).toArray, context_metadata )
//
//        val resultItems = for (input_array <- regrid_arrays) yield {
//          val tvar = worker.getResult
//          val result = HeapFltArray( tvar, Some(regridSpec.gridFile), Some(inputs.partition.origin) )
//          context.operation.rid + ":" + input_array.uid -> result
//        }
//        val reprocessed_input_map = TreeMap(resultItems: _*)
//        val array_metadata = inputs.metadata ++ op_input_arrays.head.metadata ++ List("uid" -> context.operation.rid, "gridSpec" -> regridSpec.gridFile, "gridSection" -> regridSpec.subgrid  )
//        val array_metadata_crs = context.crsOpt.map( crs => array_metadata + ( "crs" -> crs ) ).getOrElse( array_metadata )
//        logger.info("&MAP: Finished Kernel %s, acceptable inputs = [ %s ], reprocessed inputs = [ %s ], passthrough inputs = [ %s ], time = %.4f s, metadata = %s".format(name, acceptable_array_map.keys.mkString(","), reprocessed_input_map.keys.mkString(","), passthrough_array_map.keys.mkString(","), (System.nanoTime - t0) / 1.0E9, array_metadata_crs.mkString(";") ) )
//        context.addTimestamp( "Map Op complete" )
//        CDTimeSlice( reprocessed_input_map ++ acceptable_array_map ++ passthrough_array_map, array_metadata_crs, inputs.partition )
//      }
//    } finally {
//      workerManager.releaseWorker( worker )
//    }
//  }
//}

class zmqPythonKernel( _module: String, _operation: String, _title: String, _description: String, options: Map[String,String], axisElimination: Boolean, visibility_status: String  ) extends Kernel(options) {
  override def operation: String = _operation
  override def module = _module
  override def name = _module.split('.').last + "." + _operation
  override def id = _module + "." + _operation
  override val identifier = name
  override val doesAxisElimination: Boolean = axisElimination;
  override val status = KernelStatus.parse( visibility_status )
  val outputs = List( WPSProcessOutput( "operation result" ) )
  val title = _title
  val description = _description

  override def cleanUp(): Unit = PythonWorkerPortal.getInstance.shutdown()

  override def map ( context: KernelContext ) ( inputs: CDTimeSlice  ): CDTimeSlice = {
    val t0 = System.nanoTime
    val workerManager: PythonWorkerPortal  = PythonWorkerPortal.getInstance()
    val worker: PythonWorker = workerManager.getPythonWorker
    try {
      val input_arrays: List[ArraySpec] = getInputArrays( inputs, context )
      val t1 = System.nanoTime
      for( input_id <- context.operation.inputs ) inputs.element(input_id) match {
        case Some( input_array ) =>
          worker.sendRequestInput(input_id, input_array.toHeapFltArray )
        case None =>
          worker.sendUtility( List( "input", input_id ).mkString(";") )
      }
      val metadata = indexAxisConf( context.getConfiguration, context.grid.axisIndexMap ) ++ Map( "resultDir" -> Kernel.getResultDir.toString )
      worker.sendRequest(context.operation.identifier, context.operation.inputs.toArray, metadata )
      val resultItems: Seq[(String,ArraySpec)] = for( iInput <-  0 until (input_arrays.length * nOutputsPerInput)  ) yield {
        val tvar: TransVar = worker.getResult
        val uid = tvar.getMetaData.get( "uid" )
        val result = ArraySpec( tvar )
        context.operation.rid + ":" + uid + "~" + tvar.id() -> result
      }
      logger.info( "Gateway: Executing operation %s in time %.4f s".format( context.operation.identifier, (System.nanoTime - t1) / 1.0E9 ) )
      context.addTimestamp( "Map Op complete" )
      CDTimeSlice(  inputs.timestamp, inputs.dt, inputs.elements ++ resultItems )
    } finally {
      workerManager.releaseWorker( worker )
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
//  def indexAxisConf( metadata: Map[String,String], axisIndexMap: Map[String,Int] ): Map[String,String] = {
//    try {
//      metadata.get("axes") match {
//        case Some(axis_spec) =>
//          val axisIndices = axis_spec.map( _.toString).map( axis => axisIndexMap(axis) )
//          metadata + ( "axes" -> axisIndices.mkString(""))
//        case None => metadata
//      }
//    } catch { case e: Exception => throw new Exception( "Error converting axis spec %s to indices using axisIndexMap {%s}: %s".format( metadata.get("axes"), axisIndexMap.mkString(","), e.toString ) )  }
//  }
//}

class TransientFragment( val dataFrag: DataFragment, val request: RequestContext, val varMetadata: Map[String,nc2.Attribute] ) extends OperationDataInput( dataFrag.spec, varMetadata ) {
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
  override def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, batchIndex: Int ) = { ; }
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

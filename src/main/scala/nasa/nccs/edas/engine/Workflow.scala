package nasa.nccs.edas.engine

import nasa.nccs.caching.{BatchSpec, EDASPartitioner, RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm.{EDASDirectDataInput, OperationInput, _}
import nasa.nccs.caching.Partitions
import nasa.nccs.cdapi.data.RDDRecord
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.edas.engine.spark.{RecordKey, _}
import nasa.nccs.edas.kernels.Kernel.RDDKeyValPair
import nasa.nccs.edas.kernels._
import nasa.nccs.edas.utilities.runtime
import nasa.nccs.esgf.process.{WorkflowExecutor, _}
import nasa.nccs.utilities.{DAGNode, Loggable, ProfilingTool}
import nasa.nccs.wps._
import org.apache.spark.rdd.RDD
import ucar.{ma2, nc2}
import ucar.nc2.dataset.CoordinateAxis1DTime

import scala.collection.mutable
import scala.util.Try

object WorkflowNode {
  val regridKernel = new CDMSRegridKernel()
  private val _productNodes = new mutable.HashMap[String,(RecordKey,RDDRecord)]
  def apply( operation: OperationContext, kernel: Kernel  ): WorkflowNode = { new WorkflowNode( operation, kernel ) }
  def apply( node: DAGNode ) : WorkflowNode = promote( node )
  def promote( node: DAGNode ) : WorkflowNode = node match {
    case workflowNode: WorkflowNode => workflowNode
    case _ => throw new Exception( "Unknown element in workflow: " + node.getClass.getName )
  }
  def addProduct( uid: String, product: (RecordKey,RDDRecord) ): Unit = { _productNodes += ( uid -> product ) }
  def getProduct( uid: String ): Option[(RecordKey,RDDRecord)] = _productNodes.get(uid)
}

class WorkflowNode( val operation: OperationContext, val kernel: Kernel  ) extends DAGNode with Loggable {
  import WorkflowNode._
  private val contexts = mutable.HashMap.empty[String,KernelContext]
  private var _isMergedSubworkflowRoot: Boolean = false;

  def markAsMergedSubworkflowRoot: WorkflowNode = { _isMergedSubworkflowRoot = true; this }
  def isMergedSubworkflowRoot: Boolean = _isMergedSubworkflowRoot

  def getResultId: String = operation.rid
  def getNodeId: String = operation.identifier

  def isSubworkflowBoundayNode: Boolean = isRoot || doesTimeElimination

  def cacheProduct( executionResult: KernelExecutionResult  ): Unit = if(executionResult.holdsData) {
    logger.info( s"WorkflowNode CACHE PRODUCT: ${operation.rid}" )
    WorkflowNode.addProduct( operation.rid, executionResult.kvp )
  }
  def getProduct: Option[(RecordKey,RDDRecord)] = {
    val rv = WorkflowNode.getProduct( operation.rid )
    logger.info( s"WorkflowNode GET PRODUCT: ${operation.rid}, success: ${rv.isDefined.toString}" )
    rv
  }

  def fatal( msg: String ) = throw new Exception( s"Workflow Node '${operation.identifier}' Error: " + msg )
  def getKernelOption( key: String , default: String = ""): String = kernel.options.getOrElse(key,default)
  def doesTimeElimination: Boolean = operation.operatesOnAxis('t' ) && kernel.doesAxisElimination

  def getKernelContext( executor: WorkflowExecutor ): KernelContext = contexts.getOrElseUpdate( executor.requestCx.jobId, KernelContext( operation, executor ) )

  def map(input: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = kernel.mapRDD( input, context )

  def mapReduce(input: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int  ): (RecordKey,RDDRecord) = kernel.mapReduce( input, context, batchIndex )

//  def reduce(mapresult: RDD[(RecordKey,RDDRecord)], context: KernelContext, batchIndex: Int ): (RecordKey,RDDRecord) = {
//    logger.debug( "\n\n ----------------------- BEGIN reduce[%d] Operation: %s (%s): thread(%s) ----------------------- \n".format( batchIndex, context.operation.identifier, context.operation.rid, Thread.currentThread().getId ) )
//    runtime.printMemoryUsage
//    val t0 = System.nanoTime()
//    val nparts = mapresult.getNumPartitions
//    if( !kernel.parallelizable || (nparts==1) ) { mapresult.collect()(0) }
//    else {
//      val result = mapresult treeReduce kernel.getReduceOp(context)
//      logger.debug("\n\n ----------------------- FINISHED reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
//      context.addTimestamp( "FINISHED reduce Operation" )
//      result
//    }
//  }


  //  def collect(mapresult: RDD[(PartitionKey,RDDPartition)], context: KernelContext ): RDDPartition = {
//    logger.info( "\n\n ----------------------- BEGIN collect Operation: %s (%s) ----------------------- \n".format( context.operation.identifier, context.operation.rid ) )
//    val t0 = System.nanoTime()
//    var repart_mapresult = mapresult repartitionAndSortWithinPartitions PartitionManager.getPartitioner(mapresult)
//    val result = repart_mapresult.reduce(kernel.mergeRDD(context) _)._2
//    logger.info("\n\n ----------------------- FINISHED collect Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
//    result
//  }


  def regridRDDElems(input: RDD[(RecordKey,RDDRecord)], context: KernelContext): RDD[(RecordKey,RDDRecord)] =
    input.mapValues( rec => regridKernel.map( context )(rec) ) map identity

  def timeConversion(input: RDD[(RecordKey,RDDRecord)], partitioner: RangePartitioner, context: KernelContext, requestCx: RequestContext ): RDD[(RecordKey,RDDRecord)] = {
    val trsOpt: Option[String] = context.trsOpt
    val gridMap: Map[String,TargetGrid] = Map( (for( uid: String <- context.operation.inputs; targetGrid: TargetGrid = requestCx.getTargetGrid(uid) ) yield  uid -> targetGrid ) : _* )
    val targetTrsGrid: TargetGrid = trsOpt match {
      case Some( trs ) =>
        val trs_input = context.operation.inputs.find( _.split('-')(0).equals( trs.substring(1) ) ).getOrElse( fatal( "Invalid trs configuration: " + trs ) )
        gridMap.getOrElse( trs_input, fatal( "Invalid trs configuration: " + trs ) )
      case None => gridMap.values.head
    }
    val toAxis: CoordinateAxis1DTime = targetTrsGrid.getTimeCoordinateAxis.getOrElse( fatal( "Missing time axis for configuration: " + trsOpt.getOrElse("None") ) )
    val toAxisRange: ma2.Range = targetTrsGrid.getFullSection.getRange(0)
    val new_partitioner: RangePartitioner = partitioner.colaesce
    val conversionGridMap: Map[String,TargetGrid] = gridMap.filter { case (uid, grid) => grid.shape(0) != toAxis.getSize }
    val fromAxisMap: Map[ Int, CoordinateAxis1DTime ] =  conversionGridMap map { case (uid, grid) => grid.shape(0) ->
      requestCx.getTargetGridOpt(uid).getOrElse(throw new Exception("Missing Target Grid: " + uid))
        .getTimeCoordinateAxis.getOrElse(throw new Exception("Missing Time Axis: " + uid) )    }
    val conversionMap: Map[Int,TimeConversionSpec] = fromAxisMap mapValues ( fromAxis => { val converter = TimeAxisConverter( toAxis, fromAxis, toAxisRange ); converter.computeWeights(); } ) map (identity)
    CDSparkContext.coalesce( input, context ).map { case ( pkey, rdd_part ) => ( new_partitioner.range, rdd_part.reinterp( conversionMap ) ) } repartitionAndSortWithinPartitions new_partitioner
  }


//  def disaggPartitions(input: RDD[(RecordKey,RDDRecord)], context: KernelContext ): RDD[(RecordKey,RDDRecord)] = {
//    logger.info( "Executing map OP for Kernel " + kernel.id + ", OP = " + context.operation.identifier )
//    val keyedInput: RDD[(RecordKey,RDDRecord)] = input.mapPartitionsWithIndex( kernel.keyMapper )
//    keyedInput.mapValues( kernel.map(context) )
//  }
}



object Workflow {
  def apply( request: TaskRequest, executionMgr: CDS2ExecutionManager ): Workflow = {
    new Workflow( request, executionMgr )
  }
}

class WorkflowContext(val inputs: Map[String, OperationInput], val rootNode: WorkflowNode ) {
  val crs: Option[String] = getSubworkflowCRS

  def getGridObjectRef: Option[String] = crs match {
    case Some( crs ) =>
      if( crs.startsWith("~") ) Some( crs.substring(1).toLowerCase )
      else throw new Exception( "References to collections or variables in 'crs' declarations must start with '~', e.g.:  'crs':'~giss_r1i1p1'  or   'crs':'~v1' "  )
    case None => None
  }

  def getDataFragmentSpec( id: String ): Option[DataFragmentSpec] = inputs.get( id ) flatMap  {
    case opDataInput: OperationDataInput => Some(opDataInput.fragmentSpec);
    case _ => None
  }

  def getCollectionIds: List[String] = Set( inputs.keys.flatMap( id => getDataFragmentSpec(id)).map ( _.collection.collId ).toSeq: _* ).toList

  def getGridRefInput: Option[OperationDataInput] = inputs.values.find( _.matchesReference( getGridObjectRef ) ).asInstanceOf[Option[OperationDataInput]]
  def getTargetGrid: Option[TargetGrid] = getGridRefInput.map(_.getGrid)

  def getSubworkflowCRS: Option[String] = {
    val antecedents = rootNode.antecedents( (node: DAGNode) => !WorkflowNode(node).isMergedSubworkflowRoot  ) += rootNode
    val optCrsNode = antecedents.find( node => WorkflowNode(node).operation.getConfiguration.exists { case (key, value) => key.toLowerCase == "crs" } )     // TODO: Check search ordering
    val OptCrs = optCrsNode.flatMap( WorkflowNode(_).operation.getConfParm("crs") )
    val result = if( OptCrs.isEmpty ) {
      val dataInputs = inputs.values.flatMap { case data_input: OperationDataInput => Some(data_input); case _ => None }
      dataInputs.headOption.map( "~" + _.fragmentSpec.getCollection.id )
    } else { OptCrs }
    result
  }
}

case class KernelExecutionResult( key: RecordKey, record: RDDRecord, files: List[String] ) {
  val holdsData: Boolean = record.elements.nonEmpty
  def kvp: (RecordKey,RDDRecord) = ( key, record )
}

class Workflow( val request: TaskRequest, val executionMgr: CDS2ExecutionManager ) extends Loggable {
  val nodes: Seq[WorkflowNode] = request.operations.map(opCx => WorkflowNode( opCx, createKernel( opCx.name.toLowerCase ) ) )
  val roots = findRootNodes()
  private val _nodeInputs: mutable.HashMap[String, OperationInput] = mutable.HashMap.empty[String, OperationInput]

  def createKernel(id: String): Kernel = executionMgr.getKernel(id)

  def generateProduct( executor: WorkflowExecutor ): Option[WPSProcessExecuteResponse] = {
    val kernelExecutionResult = executeKernel( executor )
    if( executor.workflowCx.rootNode.isRoot ) { createResponse( kernelExecutionResult, executor ) }
    else { executor.workflowCx.rootNode.cacheProduct( kernelExecutionResult ); None }
  }

  def executeKernel(executor: WorkflowExecutor ):  KernelExecutionResult = {
    val t0 = System.nanoTime()
    val root_node = executor.rootNode
    val kernelCx: KernelContext  = root_node.getKernelContext( executor )
    kernelCx.addTimestamp( s"Executing Kernel for node ${root_node.getNodeId}" )
    val isIterative = executor.hasBatch(1)
    var batchIndex = 0
    var aggResult = ( RecordKey.empty, RDDRecord.empty )
    var resultFiles = mutable.ListBuffer.empty[String]
    do {
      val batchResult = executeBatch( executor, kernelCx, batchIndex )
      if( kernelCx.doesTimeReduction || !isIterative ) {
        val reduceOp = executor.getReduceOp(kernelCx)
        aggResult = reduceOp( aggResult, batchResult )
      } else {
        val resultMap = batchResult._2.elements.mapValues( _.toCDFloatArray )
        resultFiles += CDS2ExecutionManager.saveResultToFile(executor, resultMap, batchResult._2.metadata, List.empty[nc2.Attribute] )
      }
    } while ( { batchIndex+=1; executor.hasBatch(batchIndex) } )

    val t1 = System.nanoTime()
    if( Try( executor.requestCx.config("unitTest","false").toBoolean ).getOrElse(false)  ) { root_node.kernel.cleanUp(); }
    val t2 = System.nanoTime()
    logger.info(s"********** Completed Execution of Kernel[%s(%s)]: %s , total time = %.3f sec, cleanUp time = %.3f sec   ********** \n".format(root_node.kernel.name,root_node.kernel.id, root_node.operation.identifier, (t2 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
    KernelExecutionResult( aggResult._1, aggResult._2, resultFiles.toList )
  }

  private def common_inputs( node0: WorkflowNode, node_input_map: Map[ String, Set[String] ] )( node1: WorkflowNode ): Boolean = {
    val node0_inputs: Set[String] = node_input_map.getOrElse( node0.getNodeId, Set.empty )
    val node1_inputs: Set[String] = node_input_map.getOrElse( node1.getNodeId, Set.empty )
    node0_inputs.intersect(node1_inputs).nonEmpty
  }

  def pruneProductNodeList( product_nodes: Seq[WorkflowNode], requestCx: RequestContext ): Seq[WorkflowNode] = {
    val pruned_node_list = mutable.ListBuffer.empty[WorkflowNode]
    val node_input_map: Map[ String, Set[String] ] = Map( product_nodes.map( node => node.getNodeId -> getSubworkflowInputs(requestCx, node, false ).keys.toSet ): _* )
    val node_stack = mutable.Queue[WorkflowNode]() ++= product_nodes
    while( node_stack.nonEmpty ) {
      val test_node = node_stack.dequeue()
      if( ! node_stack.exists( common_inputs( test_node, node_input_map ) ) ) { pruned_node_list += test_node }
    }
    pruned_node_list
  }

  def executeRequest(requestCx: RequestContext): Seq[ WPSProcessExecuteResponse ] = {
    linkNodes( requestCx )
    val product_nodes = DAGNode.sort( nodes.filter( node => node.isRoot || node.doesTimeElimination ) ).toList
    val subworkflow_root_nodes: Seq[WorkflowNode] = pruneProductNodeList( product_nodes, requestCx ).map( _.markAsMergedSubworkflowRoot )
    val productNodeOpts = for( subworkflow_root_node <- subworkflow_root_nodes ) yield {
      val workflowCx = new WorkflowContext( getSubworkflowInputs( requestCx, subworkflow_root_node, true ), subworkflow_root_node )
      logger.info( "\n\n ----------------------- Execute PRODUCT Node: %s -------\n".format( subworkflow_root_node.getNodeId ))
      val executor = new WorkflowExecutor( requestCx, safety_check( workflowCx ) )
      generateProduct( executor )
    }
    productNodeOpts.flatten
  }

  def safety_check ( workflowCx: WorkflowContext ): WorkflowContext = {
    import DomainAxis._
    if( workflowCx.getCollectionIds.length > 1 ) {
      for( domain <- request.domainMap.values; axis <- domain.axes ) {
        if( axis.system=="indices" ) { throw new Exception( "Use of 'system'='indices' is not currently permitted in workflows with inputs from different collections") }
      }
    }
    workflowCx
  }

  def executeBatch(executor: WorkflowExecutor, kernelCx: KernelContext, batchIndex: Int ):  ( RecordKey, RDDRecord )  = {
    processInputs( executor.rootNode, executor, kernelCx, batchIndex)
    kernelCx.addTimestamp (s"Executing Map Op, Batch ${batchIndex.toString} for node ${ executor.rootNode.getNodeId}", true)
    val  (key: RecordKey, rec: RDDRecord) =  executor.execute( kernelCx, batchIndex )
    logger.info("\n\n ----------------------- END mapReduce: NODE %s, operation: %s, batch id: %d, contents = [ %s ]  -------\n".format( executor.rootNode.getNodeId, kernelCx.operation.identifier, batchIndex, executor.contents.mkString(", ") ) )
    key -> executor.rootNode.kernel.postRDDOp( executor.rootNode.kernel.orderElements( rec.configure("gid", kernelCx.grid.uid), kernelCx ), kernelCx  )
  }

//  def streamMapReduceBatchRecursive( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[RDD[(RecordKey,RDDRecord)]] =
//    prepareInputs(node, opInputs, kernelContext, requestCx, batchIndex) map ( inputs => {
//      logger.info( s"Executing mapReduce Batch ${batchIndex.toString}" )
//      val mapresult = node.map( inputs, kernelContext )
//      val result: RDD[(RecordKey,RDDRecord)] = streamReduceNode( mapresult, node, kernelContext, batchIndex )
//      streamMapReduceBatchRecursive( node, opInputs, kernelContext, requestCx, batchIndex + 1 ) match {
//        case Some( next_result ) =>
//          val reduceOp = node.kernel.getReduceOp(kernelContext)
//          result.join(next_result).mapValues(rdds => node.kernel.combineRDD(kernelContext)(rdds._1, rdds._2))
//        case None => result
//      }})

//  def streamReduceNode(mapresult: RDD[(RecordKey,RDDRecord)], node: WorkflowNode, context: KernelContext, batchIndex: Int ): RDD[(RecordKey,RDDRecord)] = {
//    logger.debug( "\n\n ----------------------- BEGIN stream reduce[%d] Operation: %s (%s): thread(%s) ----------------------- \n".format( batchIndex, context.operation.identifier, context.operation.rid, Thread.currentThread().getId ) )
//    runtime.printMemoryUsage
//    val t0 = System.nanoTime()
//    if( context.doesTimeReduction ) {}
//        val inputNParts = mapresult.partitions.length
//        val pre_result_pair = mapresult treeReduce node.kernel.getReduceOp(context)
//        val result = pre_result_pair._1 -> node.kernel.postRDDOp( pre_result_pair._2, context  )
//        logger.debug("\n\n ----------------------- FINISHED stream reduce Operation: %s (%s), time = %.3f sec ----------------------- ".format(context.operation.identifier, context.operation.rid, (System.nanoTime() - t0) / 1.0E9))
//        val results = List.fill(inputNParts)( result )
//        executionMgr.serverContext.spark.sparkContext.parallelize( results )
//    } else { mapresult }
//  }



  def stream(node: WorkflowNode, executor: WorkflowExecutor, batchIndex: Int ): Unit = {
    val kernelContext = node.getKernelContext( executor )
    processInputs(node, executor, kernelContext, batchIndex)
    executor.execInput( node, kernelContext, batchIndex )
    logger.info( s"Executed STREAM mapReduce Batch ${batchIndex.toString}" )
  }

  def linkNodes(requestCx: RequestContext): Unit = {
    logger.info( s"linkNodes; inputs = ${requestCx.inputs.keys.mkString(",")}")
    for (workflowNode <- nodes; uid <- workflowNode.operation.inputs)  {
      requestCx.getInputSpec(uid) match {
        case Some(inputSpec) => Unit
        case None =>
          nodes.find(_.getResultId.equals(uid)) match {
            case Some(inode) => workflowNode.addInput(inode)
            case None =>
              val errorMsg = " * Unidentified input in workflow node %s: '%s': This is typically due to an empty domain intersection with the dataset! \n ----> inputs ids = %s, input source keys = %s, input source values = %s, result ids = %s".format(
                workflowNode.getNodeId, uid, requestCx.inputs.keySet.map(k=>s"'$k'").mkString(", "), requestCx.inputs.keys.mkString(", "), requestCx.inputs.values.mkString(", "),
                nodes.map(_.getNodeId).map(k=>s"'$k'").mkString(", "))
              logger.error(errorMsg)
              throw new Exception(errorMsg)
          }
      }
    }
  }

  def findRootNodes(): List[WorkflowNode] = {
    import scala.collection.mutable.LinkedHashSet
    val results = LinkedHashSet( nodes:_* )
    for (potentialRootNode <- nodes ) {
       for ( workflowNode <- nodes; uid <- workflowNode.operation.inputs )  {
          if( potentialRootNode.getResultId.equals(uid) ) {
            results.remove(potentialRootNode)
          }
       }
    }
    return results.toList
  }

  def getSubWorkflow(rootNode: WorkflowNode, merged: Boolean ): List[WorkflowNode] = {
    val filter = (node: DAGNode) => if( merged ) { !WorkflowNode(node).isMergedSubworkflowRoot } else { !WorkflowNode(node).isSubworkflowBoundayNode }
    ( rootNode.predecesors(filter).map( WorkflowNode.promote ) += rootNode ).toList
  }

  def getSubworkflowInputs(requestCx: RequestContext, rootNode: WorkflowNode, merged: Boolean): Map[String, OperationInput] = {
    val inputMaps = getSubWorkflow(rootNode,merged).map( getNodeInputs( requestCx, _ ) )
    inputMaps.foldLeft( mutable.HashMap.empty[String, OperationInput] )( _ ++= _ ).toMap
  }

  def getSubworkflowCRS( rootNode: WorkflowNode, inputs: Iterable[OperationInput] ): Option[String] = {
    val antecedents = rootNode.antecedents( (node: DAGNode) => !WorkflowNode(node).isMergedSubworkflowRoot  ) += rootNode
    val optCrsNode = antecedents.find( node => WorkflowNode(node).operation.getConfiguration.exists { case (key, value) => key.toLowerCase == "crs" } )     // TODO: Check search ordering
    val OptCrs = optCrsNode.flatMap( WorkflowNode(_).operation.getConfParm("crs") )
    if( OptCrs.isEmpty ) {
      val dataInputs = inputs.flatMap { case data_input: OperationDataInput => Some(data_input); case _ => None }
      dataInputs.headOption.map( "~" + _.fragmentSpec.getCollection.collId )
    } else { OptCrs }
  }

  def getNodeInput( uid: String, requestCx: RequestContext, workflowNode: WorkflowNode): OperationInput = requestCx.getInputSpec(uid) match {
    case Some(inputSpec) =>
      logger.info("getInputSpec: %s -> %s ".format(uid, inputSpec.longname))
      if( workflowNode.kernel.extInputs ) { new ExternalDataInput( inputSpec, workflowNode ) }
      else                                { executionMgr.serverContext.getOperationInput(inputSpec, requestCx.getConfiguration, workflowNode ) }
    case None =>
      nodes.find( _.getResultId.equals(uid) ) match {
        case Some(inode) => new DependencyOperationInput( inode, workflowNode )
        case None =>
          val errorMsg = " ** Unidentified input in workflow node %s: %s, input ids = %s".format(workflowNode.getNodeId, uid, requestCx.inputs.keySet.mkString(", "))
          logger.error(errorMsg)
          throw new Exception(errorMsg)
      }
  }


  def getNodeInputs(requestCx: RequestContext, workflowNode: WorkflowNode): Map[String, OperationInput] = {
    val items = for (uid <- workflowNode.operation.inputs) yield {
      uid -> _nodeInputs.getOrElseUpdate( uid, getNodeInput( uid, requestCx, workflowNode) ).registerConsumer( workflowNode.operation )
    }
    Map(items: _*)
  }

  def createResponse( executionResult: KernelExecutionResult, executor: WorkflowExecutor  ): Option[WPSProcessExecuteResponse] = {
    val resultId = cacheResult( executionResult, executor )
    logger.info( s"Create result ${resultId}: req-context metadata: ${executor.requestCx.task.metadata.mkString("; ")}" )
    val node = executor.rootNode
    executor.requestCx.getConf("response", "xml") match {
        case "object" =>
          Some( new RefExecutionResult("WPS", node.kernel, node.operation.identifier, resultId, List.empty[String] ) )
        case "xml" =>
          Some( new RDDExecutionResult("WPS", List(node.kernel), node.operation.identifier, executionResult.record, resultId) )// TODO: serviceInstance
        case "file" =>
          val resultFiles: List[String] = executionMgr.getResultFilePath( executionResult, executor )
          Some( new RefExecutionResult("WPS", node.kernel, node.operation.identifier, resultId, resultFiles) )
      }
  }

  def cacheResult( executionResult: KernelExecutionResult, executor: WorkflowExecutor  ): String = {
    if( executionResult.holdsData ) {
      collectionDataCache.putResult(executor.requestCx.jobId, new RDDTransientVariable( executionResult.record, executor.rootNode.operation, executor.requestCx))
      logger.info(" ^^^^## Cached result, rid = " + executor.requestCx.jobId + ", head elem metadata = " + executionResult.record.elements.head._2.metadata)
    }
    executor.requestCx.jobId
  }

  def needsRegrid(rdd: RDD[(RecordKey,RDDRecord)], requestCx: RequestContext, kernelContext: KernelContext ): Boolean = {
    try {
      val sampleRDDPart: RDDRecord = rdd.first._2
      val targetGrid = requestCx.getTargetGridOpt(kernelContext.grid.uid).getOrElse(throw new Exception("Undefined Target Grid for kernel " + kernelContext.operation.identifier))
      if (targetGrid.getGridSpec.startsWith("gspec")) return true
      sampleRDDPart.elements.foreach { case (uid, data) => if (data.gridSpec != targetGrid.getGridSpec) kernelContext.crsOpt match {
        case Some(crs) =>
          return true
        case None =>
          requestCx.getTargetGridOpt(uid) match {
            case Some(tgrid) => if (!tgrid.shape.sameElements(targetGrid.shape)) return true
            case None => throw new Exception(s"Undefined Grid in input ${uid} for kernel " + kernelContext.operation.identifier)
          }
      }}
    } catch { case err: Exception => logger.error( s"Empty input in needsRegrid: ${rdd.id} ")}
    return false
  }

  def unifyGrids(rdd: RDD[(RecordKey,RDDRecord)], requestCx: RequestContext, kernelContext: KernelContext, node: WorkflowNode  ): RDD[(RecordKey,RDDRecord)] = {
    logger.info( "unifyGrids: OP = " + node.operation.name )
    if( needsRegrid(rdd,requestCx,kernelContext) )
      node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
    else rdd
  }

  def processInputs(node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, batchIndex: Int ) = {
    kernelContext.addTimestamp( "Generating RDD for inputs: " + executor.workflowCx.inputs.keys.mkString(", "), true )
    executor.getInputs(node).foreach { case (uid, opinput) =>
      opinput.processInput( uid, this, node, executor, kernelContext, batchIndex)
    }
    logger.info("\n\n ----------------------- Completed RDD input map[%d], thread: %s -------\n".format(batchIndex, Thread.currentThread().getId ))
  }

  def getTimeReferenceRdd(rddMap: Map[String,RDD[(RecordKey,RDDRecord)]], kernelContext: KernelContext ): Option[RDD[(RecordKey,RDDRecord)]] = kernelContext.trsOpt map { trs =>
    rddMap.keys.find( _.split('-').dropRight(1).mkString("-").equals(trs.substring(1)) ) match {
      case Some(trsKey) => rddMap.getOrElse(trsKey, throw new Exception( s"Error retreiving key $trsKey from rddMap with keys {${rddMap.keys.mkString(",")}}" ) )
      case None => throw new Exception( s"Unmatched trs $trs in kernel ${kernelContext.operation.name}, keys = {${rddMap.keys.mkString(",")}}" )
    }
  }

  def applyTimeConversion( new_partitioner: RangePartitioner, kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) ( rdd: RDD[(RecordKey,RDDRecord)] ): RDD[(RecordKey,RDDRecord)] = {
    CDSparkContext.getPartitioner(rdd) match {
      case Some( partitioner ) =>
        val repart_result = if (partitioner.equals (new_partitioner) ) { rdd }
        else {
          val convertedResult = if ( partitioner.numElems != new_partitioner.numElems ) {
            node.timeConversion (rdd, new_partitioner, kernelContext, requestCx)
          } else { rdd }
          CDSparkContext.repartition (convertedResult, new_partitioner)
        }
        if ( node.kernel.parallelizable ) { repart_result }
        else { CDSparkContext.coalesce(repart_result, kernelContext) }
      case None => rdd
    }
  }

  def unifyRDDs(rddMap: Map[String,RDD[(RecordKey,RDDRecord)]], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) : RDD[(RecordKey,RDDRecord)] = {
    logger.info( "unifyRDDs: " + rddMap.keys.mkString(", ") )
    val t0 = System.nanoTime
    val convertedRdds: Iterable[RDD[(RecordKey,RDDRecord)]] = if( node.kernel.extInputs ) { rddMap.values }
    else {
      val trsRdd: RDD[(RecordKey, RDDRecord)] = getTimeReferenceRdd(rddMap, kernelContext).getOrElse(rddMap.values.head)
      CDSparkContext.getPartitioner(trsRdd) match {
        case Some(timeRefPartitioner) => rddMap.values map applyTimeConversion(timeRefPartitioner, kernelContext, requestCx, node)
        case None => rddMap.values
      }
    }
    val matchedRdds = convertedRdds.map( rdd =>
      if( needsRegrid(rdd,requestCx,kernelContext) ) node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
      else rdd
    )
    logger.info( "Merge RDDs, unify time = %.4f sec".format( (System.nanoTime - t0) / 1.0E9 ) )
    if( matchedRdds.size == 1 ) matchedRdds.head else matchedRdds.tail.foldLeft( matchedRdds.head )( CDSparkContext.merge )
  }

  //  def domainRDDPartition( opInputs: Map[String,OperationInput], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ): RDD[(Int,RDDPartition)] = {
  //    val targetGrid: TargetGrid = getKernelGrid( kernelContext, requestCx )
  //    val opSection: Option[ma2.Section] = getOpSectionIntersection( targetGrid, node )
  //    val rawRdds: Iterable[RDDRegen] = opInputs.map { case ( uid, opinput ) => opinput match {
  //      case ( dataInput: PartitionedFragment) =>
  //        new RDDRegen( executionMgr.serverContext.spark.getRDD( uid, dataInput, requestCx, opSection, node ), dataInput.getGrid, targetGrid, node, kernelContext )
  //      case ( kernelInput: DependencyOperationInput  ) =>
  //        logger.info( "\n\n ----------------------- Stream DEPENDENCY Node: %s -------\n".format( kernelInput.workflowNode.getNodeId ))
  //        val ( result, context ) = kernelInput.workflowNode.stream( requestCx )
  //        new RDDRegen( result, getKernelGrid(context,requestCx), targetGrid, node, kernelContext )
  //      case (  x ) =>
  //        throw new Exception( "Unsupported OperationInput class: " + x.getClass.getName )
  //    }
  //    }
  //    val rawResult: RDD[(Int,RDDPartition)] = if( opInputs.size == 1 ) rawRdds.head._1 else rawRdds.tail.foldLeft( rawRdds.head._1 )( CDSparkContext.merge(_._1,_._1) )
  //    if(needsRegrid) { node.map( rawResult, kernelContext, regridKernel ) } else rawResult
  //  }

  def getOpSections( targetGrid: TargetGrid, node: WorkflowNode ): Option[ IndexedSeq[ma2.Section] ] = {
    val optargs: Map[String, String] = node.operation.getConfiguration
    val domains: IndexedSeq[DomainContainer] = optargs.get("domain") match {
      case Some(domainIds) => domainIds.split(",").flatMap( request.getDomain(_) ).toIndexedSeq
      case None => return Some( IndexedSeq.empty[ma2.Section] )
    }
    //    logger.info( "OPT DOMAIN Arg: " + optargs.getOrElse( "domain", "None" ) )
    //    logger.info( "OPT Domains: " + domains.map(_.toString).mkString( ", " ) )
    Some( domains.map(dc => targetGrid.grid.getSubSection(dc.axes) match {
      case Some(section) => section
      case None => return None
    }))
  }

  def getOpSectionIntersection( targetGrid: TargetGrid, node: WorkflowNode): Option[ ma2.Section ] = getOpSections(targetGrid,node) match {
    case None => None
    case Some( sections ) =>
      if( sections.isEmpty ) None
      else {
        val result = sections.foldLeft(sections.head)( _.intersect(_) )
        if (result.computeSize() > 0) { Some(result) }
        else  None
      }
  }
  def getOpCDSectionIntersection(targetGrid: TargetGrid, node: WorkflowNode): Option[ CDSection ] = getOpSectionIntersection(targetGrid, node).map( CDSection( _ ) )
}


//object SparkTestApp extends App {
//  val nparts = 4
//  def _reduce( rdd: RDD[(Int,Float)], combiner: (Float,Float)=>Float ): RDD[(Int,Float)] = {
//    val mod_rdd = rdd map { case (i,x) => (i/2,x) }
//    val reduced_rdd = mod_rdd.reduceByKey( combiner )
//    if( reduced_rdd.count() > 1 ) _reduce( reduced_rdd, combiner ) else reduced_rdd
//  }
//  val conf = new SparkConf(false).setMaster( s"local[$nparts]" ).setAppName( "SparkTestApp" )
//  val sc = new SparkContext(conf)
//  val rdd: RDD[(Int,Float)] = sc.parallelize( (20 to 0 by -1) map ( i => (i,i.toFloat) ) )
//  val partitioner = new RangePartitioner(nparts,rdd)
//  val ordereddRdd = rdd.partitionBy(partitioner).sortByKey(true)
//  val result = ordereddRdd.collect()
//  println( "\n\n" + result.mkString(", ") + "\n\n" )
//}


package nasa.nccs.edas.engine

import nasa.nccs.caching.{BatchSpec, RDDTransientVariable, collectionDataCache}
import nasa.nccs.cdapi.cdm.{OperationInput, _}
import nasa.nccs.edas.engine.EDASExecutionManager.saveResultToFile
import nasa.nccs.edas.kernels._
import nasa.nccs.edas.rdd.{QueryResultCollection, VariableRecord}
import nasa.nccs.esgf.process.{WorkflowExecutor, _}
import nasa.nccs.utilities.{DAGNode, Loggable}
import nasa.nccs.wps.{RDDExecutionResult, RefExecutionResult, WPSProcessExecuteResponse}
import ucar.{ma2, nc2}

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.Try

object WorkflowNode {
  private val _nodeProducts = new mutable.HashMap[String,QueryResultCollection]
  def apply( operation: OperationContext, kernel: KernelImpl  ): WorkflowNode = { new WorkflowNode( operation, kernel ) }
  def apply( node: DAGNode ) : WorkflowNode = promote( node )
  def promote( node: DAGNode ) : WorkflowNode = node match {
    case workflowNode: WorkflowNode => workflowNode
    case _ => throw new Exception( "Unknown element in workflow: " + node.getClass.getName )
  }
  def addProduct( uid: String, product: QueryResultCollection ): Unit = { _nodeProducts += ( uid -> product ) }
  def getProduct( uid: String ): Option[QueryResultCollection] = _nodeProducts.get(uid)
}

class WorkflowNode( val operation: OperationContext, val kernel: KernelImpl  ) extends DAGNode with Loggable {
  import WorkflowNode._
  private val contexts = mutable.HashMap.empty[String,KernelContext]
  private var _isMergedSubworkflowRoot: Boolean = false;

  def markAsMergedSubworkflowRoot: WorkflowNode = { _isMergedSubworkflowRoot = true; this }
  def isMergedSubworkflowRoot: Boolean = _isMergedSubworkflowRoot

  def getResultId: String = operation.rid
  def getNodeId: String = operation.identifier

  def isSubworkflowBoundayNode: Boolean = isRoot || doesTimeReduction

  def cacheProduct( executionResult: KernelExecutionResult  ): Unit = if(executionResult.holdsData) {
    logger.info( s"WorkflowNode CACHE PRODUCT: ${operation.rid}" )
    WorkflowNode.addProduct( operation.rid, executionResult.results )
  }
  def getProduct: Option[QueryResultCollection] = {
    val rv = WorkflowNode.getProduct( operation.rid )
    logger.info( s"WorkflowNode GET PRODUCT: ${operation.rid}, success: ${rv.isDefined.toString}" )
    rv
  }

  def fatal( msg: String ) = throw new Exception( s"Workflow Node '${operation.identifier}' Error: " + msg )
  def getKernelOption( key: String , default: String = ""): String = kernel.options.getOrElse(key,default)
  def doesTimeReduction: Boolean = operation.operatesOnAxis('t' ) && kernel.doesAxisReduction

  def getKernelContext( executor: WorkflowExecutor ): KernelContext = contexts.getOrElseUpdate( executor.requestCx.jobId, KernelContext( operation, executor ) )

//  def mapInput(input: RDD[CDTimeSlice], context: KernelContext ): RDD[CDTimeSlice] = kernel.mapRDD( input, context )

//  def mapReduceInput(input: RDD[CDTimeSlice], context: KernelContext, batchIndex: Int  ): CDTimeSlice = kernel.mapReduce( input, context, batchIndex )

//  def reduce(mapresult: RDD[CDTimeSlice], context: KernelContext, batchIndex: Int ): CDTimeSlice = {
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


//  def regridRDDElems(input: RDD[CDTimeSlice], context: KernelContext): RDD[CDTimeSlice] =
//    input.mapValues( rec => regridKernel.map( context )(rec) ) map identity

//  def timeConversion(input: RDD[CDTimeSlice], partitioner: RangePartitioner, context: KernelContext, requestCx: RequestContext ): RDD[CDTimeSlice] = {
//    val trsOpt: Option[String] = context.trsOpt
//    val gridMap: Map[String,TargetGrid] = Map( (for( uid: String <- context.operation.inputs; targetGrid: TargetGrid = requestCx.getTargetGrid(uid) ) yield  uid -> targetGrid ) : _* )
//    val targetTrsGrid: TargetGrid = trsOpt match {
//      case Some( trs ) =>
//        val trs_input = context.operation.inputs.find( _.split('-')(0).equals( trs.substring(1) ) ).getOrElse( fatal( "Invalid trs configuration: " + trs ) )
//        gridMap.getOrElse( trs_input, fatal( "Invalid trs configuration: " + trs ) )
//      case None => gridMap.values.head
//    }
//    val toAxis: CoordinateAxis1DTime = targetTrsGrid.getTimeCoordinateAxis.getOrElse( fatal( "Missing time axis for configuration: " + trsOpt.getOrElse("None") ) )
//    val toAxisRange: ma2.Range = targetTrsGrid.getFullSection.getRange(0)
//    val new_partitioner: RangePartitioner = partitioner.colaesce
//    val conversionGridMap: Map[String,TargetGrid] = gridMap.filter { case (uid, grid) => grid.shape(0) != toAxis.getSize }
//    val fromAxisMap: Map[ Int, CoordinateAxis1DTime ] =  conversionGridMap map { case (uid, grid) => grid.shape(0) ->
//      requestCx.getTargetGridOpt(uid).getOrElse(throw new Exception("Missing Target Grid: " + uid))
//        .getTimeCoordinateAxis.getOrElse(throw new Exception("Missing Time Axis: " + uid) )    }
//    val conversionMap: Map[Int,TimeConversionSpec] = fromAxisMap mapValues ( fromAxis => { val converter = TimeAxisConverter( toAxis, fromAxis, toAxisRange ); converter.computeWeights(); } ) map (identity)
//    CDSparkContext.coalesce( input, context ).map { case ( pkey, rdd_part ) => ( new_partitioner.range, rdd_part.reinterp( conversionMap ) ) } repartitionAndSortWithinPartitions new_partitioner
//  }


//  def disaggPartitions(input: RDD[CDTimeSlice], context: KernelContext ): RDD[CDTimeSlice] = {
//    logger.info( "Executing map OP for Kernel " + kernel.id + ", OP = " + context.operation.identifier )
//    val keyedInput: RDD[CDTimeSlice] = input.mapPartitionsWithIndex( kernel.keyMapper )
//    keyedInput.mapValues( kernel.map(context) )
//  }
}



object Workflow {
  def apply( request: TaskRequest, executionMgr: EDASExecutionManager ): Workflow = {
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

  def getGridRefInput: Option[OperationDataInput] = {
    val optGridObjectRef = getGridObjectRef
    val opGridSpec = inputs.values.find(_.matchesReference(optGridObjectRef)).asInstanceOf[Option[OperationDataInput]]
    if( opGridSpec.isEmpty ) { getOperationDataInput } else { opGridSpec }
  }

  def getOperationDataInput: Option[OperationDataInput] = {
    inputs.values.foreach { case opInput: OperationDataInput => return Some(opInput); case _ => Unit }
    None
  }
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

case class KernelExecutionResult(results: QueryResultCollection, files: List[String] ) {
  val holdsData: Boolean = results.slices.nonEmpty
  val slice = results.getConcatSlice
}

class Workflow( val request: TaskRequest, val executionMgr: EDASExecutionManager ) extends Loggable {
  val nodes: List[WorkflowNode] = request.operations flatMap getWorkflowNodes toList
  val roots = findRootNodes()
  private val _nodeInputs: mutable.HashMap[String, OperationInput] = mutable.HashMap.empty[String, OperationInput]

  def createKernel(id: String): Kernel = executionMgr.getKernel(id)

  def getWorkflowNodes( operation: OperationContext ): List[WorkflowNode] = {
    val opName = operation.name.toLowerCase
    val moduleName = opName.split('.').head
    if( moduleName.toLowerCase.equals("util") ) { List.empty }
    else {
      val baseKernel = createKernel( opName )
      baseKernel.getWorkflowNodes( this, operation )
    }
  }

  def generateProduct( executor: WorkflowExecutor ): Option[WPSProcessExecuteResponse] = {
    val kernelExecutionResult = executeKernel( executor )
    if( executor.workflowCx.rootNode.isRoot ) { createResponse( kernelExecutionResult, executor ) }
    else { executor.workflowCx.rootNode.cacheProduct( kernelExecutionResult ); None }
  }

  def executeKernel(executor: WorkflowExecutor ):  KernelExecutionResult = {
    val root_node = executor.rootNode
    val kernelCx: KernelContext  = root_node.getKernelContext( executor )
    val isIterative = false // executor.hasBatch(1)
    var batchIndex = 0
    var aggResult = QueryResultCollection.empty
    var resultFiles = mutable.ListBuffer.empty[String]
    do {
      val ts0 = System.nanoTime()
      val batchResult:  QueryResultCollection = executeBatch( executor, kernelCx, batchIndex )
      val ts1 = System.nanoTime()
      if( kernelCx.doesTimeReduction || !isIterative  ) {
        val reduceOp = executor.getReduceOp(kernelCx)
        aggResult = aggResult.merge( batchResult, reduceOp )
      } else {
        if( executor.requestCx.task.getUserAuth > 0 ) {
          val resultMap = batchResult.concatSlices.slices.head.elements.mapValues( slice => slice.toCDFloatArray )
          resultFiles += EDASExecutionManager.saveResultToFile(executor, resultMap, batchResult.getMetadata, List.empty[nc2.Attribute])
          executor.releaseBatch
        } else {
          throw new Exception( "Must be authorized to execute a request this big- please contact the service administrator for instructions.")
        }
      }
      val ts2 = System.nanoTime()
      logger.info(s" @CDS@ BATCH mapReduce time = %.3f sec, agg time = %.3f sec, total processing time = %.3f sec   ********** \n".format( (ts1 - ts0)/1.0E9 , (ts2 - ts1)/1.0E9, (ts2 - ts0)/1.0E9 ) )
    } while ( { batchIndex+=1; false; /* executor.hasBatch(batchIndex) */ } )

    if( Try( executor.requestCx.config("unitTest","false").toBoolean ).getOrElse(false)  ) { root_node.kernel.cleanUp(); }
    KernelExecutionResult( aggResult, resultFiles.toList )
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
    val product_nodes = DAGNode.sort( nodes.filter( node => node.isRoot || node.doesTimeReduction ) ).toList
    val subworkflow_root_nodes: Seq[WorkflowNode] = pruneProductNodeList( product_nodes, requestCx ).map( _.markAsMergedSubworkflowRoot )
    val productNodeOpts = for( subworkflow_root_node <- subworkflow_root_nodes ) yield {
      val workflowCx = new WorkflowContext( getSubworkflowInputs( requestCx, subworkflow_root_node, true ), subworkflow_root_node )
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

  def executeBatch(executor: WorkflowExecutor, kernelCx: KernelContext, batchIndex: Int ):  QueryResultCollection  = {
    kernelCx.profiler.profile("processInputs") ( () => {
      processInputs( executor.rootNode, executor, kernelCx, batchIndex )
    } )
    kernelCx.profiler.profile("regrid") ( () => {
      executor.regrid( kernelCx.addVariableRecords( executor.variableRecs ) )
    } )
    kernelCx.profiler.profile("execute") ( () => {
      executor.execute(this, kernelCx, batchIndex )
    } )
  }

//  def executeBatch(executor: WorkflowExecutor, kernelCx: KernelContext, batchIndex: Int ):  TimeSliceCollection  = {
//    val t0 = System.nanoTime()
//    processInputs( executor.rootNode, executor, kernelCx, batchIndex)
//    val nSlices0 = executor.nSlices
//    val t1 = System.nanoTime()
//    logger.info( s" TTT processInputs: nSlices0: ${nSlices0}, time: ${(t1-t0)/1.0E9}")
//    executor.regrid( kernelCx.addVariableRecords( executor.variableRecs ) )
//    val nSlices1 = executor.nSlices
//    val t2 = System.nanoTime()
//    logger.info( s" TTT regrid: nSlices: ${nSlices1}, time: ${(t2-t1)/1.0E9}")
//    val rv = executor.execute( this, kernelCx, batchIndex )
//    val t3 = System.nanoTime()
//    logger.info( s" TTT execute: nSlices: ${rv.slices.length}, time: ${(t3-t2)/1.0E9}, total time: ${(t3-t0)/1.0E9}")
//    rv
//  }


  //  def streamMapReduceBatchRecursive( node: WorkflowNode, opInputs: Map[String, OperationInput], kernelContext: KernelContext, requestCx: RequestContext, batchIndex: Int ): Option[RDD[CDTimeSlice]] =
//    prepareInputs(node, opInputs, kernelContext, requestCx, batchIndex) map ( inputs => {
//      logger.info( s"Executing mapReduce Batch ${batchIndex.toString}" )
//      val mapresult = node.map( inputs, kernelContext )
//      val result: RDD[CDTimeSlice] = streamReduceNode( mapresult, node, kernelContext, batchIndex )
//      streamMapReduceBatchRecursive( node, opInputs, kernelContext, requestCx, batchIndex + 1 ) match {
//        case Some( next_result ) =>
//          val reduceOp = node.kernel.getReduceOp(kernelContext)
//          result.join(next_result).mapValues(rdds => node.kernel.combineRDD(kernelContext)(rdds._1, rdds._2))
//        case None => result
//      }})

//  def streamReduceNode(mapresult: RDD[CDTimeSlice], node: WorkflowNode, context: KernelContext, batchIndex: Int ): RDD[CDTimeSlice] = {
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
    executor.streamMapReduce( node, kernelContext, executionMgr.serverContext, batchIndex )
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
              val errorMsg = s" * Unidentified input in workflow node %s: '%s', available inputs: { ${nodes.map(_.getResultId).mkString(",")} }: This is typically due to an empty domain intersection with the dataset! \n ----> inputs ids = %s, input source keys = %s, input source values = %s, result ids = %s".format(
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
          Some( new RDDExecutionResult("WPS", List(node.kernel), node.operation.identifier, executionResult.slice, resultId) )// TODO: serviceInstance
        case "file" =>
          val resultFiles: List[String] = executionMgr.getResultFilePath( executionResult, executor )
          Some( new RefExecutionResult("WPS", node.kernel, node.operation.identifier, resultId, resultFiles) )
      }
  }

  def cacheResult( executionResult: KernelExecutionResult, executor: WorkflowExecutor  ): String = {
    if( executionResult.holdsData ) {
      val tvar = new RDDTransientVariable( executionResult.results, executor.rootNode.operation, executor.requestCx)
      collectionDataCache.putResult(executor.requestCx.jobId, tvar )
      if( ! executor.requestCx.getConf( "saveLocalFile", "" ).isEmpty ) {
        val resultMap = tvar.result.concatSlices.slices.flatMap(_.elements.headOption).toMap.mapValues( slice =>  slice.toCDFloatArray )
        saveResultToFile(executor, resultMap, tvar.result.getMetadata, List.empty[nc2.Attribute])
      }
    }
    executor.requestCx.jobId
  }

//  def needsRegrid(rdd: RDD[CDTimeSlice], requestCx: RequestContext, kernelContext: KernelContext ): Boolean = {
//    try {
//      val sampleRDDPart: CDTimeSlice = rdd.first._2
//      val targetGrid = requestCx.getTargetGridOpt(kernelContext.grid.uid).getOrElse(throw new Exception("Undefined Target Grid for kernel " + kernelContext.operation.identifier))
//      if (targetGrid.getGridSpec.startsWith("gspec")) return true
//      sampleRDDPart.elements.foreach { case (uid, data) => if (data.gridSpec != targetGrid.getGridSpec) kernelContext.crsOpt match {
//        case Some(crs) =>
//          return true
//        case None =>
//          requestCx.getTargetGridOpt(uid) match {
//            case Some(tgrid) => if (!tgrid.shape.sameElements(targetGrid.shape)) return true
//            case None => throw new Exception(s"Undefined Grid in input ${uid} for kernel " + kernelContext.operation.identifier)
//          }
//      }}
//    } catch { case err: Exception => logger.error( s"Empty input in needsRegrid: ${rdd.id} ")}
//    return false
//  }

//  def unifyGrids(rdd: RDD[CDTimeSlice], requestCx: RequestContext, kernelContext: KernelContext, node: WorkflowNode  ): RDD[CDTimeSlice] = {
//    logger.info( "unifyGrids: OP = " + node.operation.name )
//    if( needsRegrid(rdd,requestCx,kernelContext) )
//      node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
//    else rdd
//  }

  def processInputs(node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, batchIndex: Int ) = {
    val gridRefInput: OperationDataInput =  executor.getGridRefInput.getOrElse( throw new Exception("No grid ref input found for domainRDDPartition") )
    executor.getInputs(node).foreach { case (uid, opinput) =>
      opinput.processInput( uid, this, node, executor, kernelContext, gridRefInput, batchIndex)
    }
  }

//  def getTimeReferenceRdd(rddMap: Map[String,RDD[CDTimeSlice]], kernelContext: KernelContext ): Option[RDD[CDTimeSlice]] = kernelContext.trsOpt map { trs =>
//    rddMap.keys.find( _.split('-').dropRight(1).mkString("-").equals(trs.substring(1)) ) match {
//      case Some(trsKey) => rddMap.getOrElse(trsKey, throw new Exception( s"Error retreiving key $trsKey from rddMap with keys {${rddMap.keys.mkString(",")}}" ) )
//      case None => throw new Exception( s"Unmatched trs $trs in kernel ${kernelContext.operation.name}, keys = {${rddMap.keys.mkString(",")}}" )
//    }
//  }

//  def applyTimeConversion( new_partitioner: RangePartitioner, kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) ( rdd: RDD[CDTimeSlice] ): RDD[CDTimeSlice] = {
//    CDSparkContext.getPartitioner(rdd) match {
//      case Some( partitioner ) =>
//        val repart_result = if (partitioner.equals (new_partitioner) ) { rdd }
//        else {
//          val convertedResult = if ( partitioner.numElems != new_partitioner.numElems ) {
//            node.timeConversion (rdd, new_partitioner, kernelContext, requestCx)
//          } else { rdd }
//          CDSparkContext.repartition (convertedResult, new_partitioner)
//        }
//        if ( node.kernel.parallelizable ) { repart_result }
//        else { CDSparkContext.coalesce(repart_result, kernelContext) }
//      case None => rdd
//    }
//  }

//  def unifyRDDs(rddMap: Map[String,RDD[CDTimeSlice]], kernelContext: KernelContext, requestCx: RequestContext, node: WorkflowNode ) : RDD[CDTimeSlice] = {
//    logger.info( "unifyRDDs: " + rddMap.keys.mkString(", ") )
//    val t0 = System.nanoTime
//    val convertedRdds: Iterable[RDD[CDTimeSlice]] = if( node.kernel.extInputs ) { rddMap.values }
//    else {
//      val trsRdd: RDD[(RecordKey, CDTimeSlice)] = getTimeReferenceRdd(rddMap, kernelContext).getOrElse(rddMap.values.head)
//      CDSparkContext.getPartitioner(trsRdd) match {
//        case Some(timeRefPartitioner) => rddMap.values map applyTimeConversion(timeRefPartitioner, kernelContext, requestCx, node)
//        case None => rddMap.values
//      }
//    }
//    val matchedRdds = convertedRdds.map( rdd =>
//      if( needsRegrid(rdd,requestCx,kernelContext) ) node.regridRDDElems( rdd, kernelContext.conf(Map("gridSpec"->requestCx.getTargetGridSpec(kernelContext),"crs"->kernelContext.crsOpt.getOrElse(""))))
//      else rdd
//    )
//    logger.info( "Merge RDDs, unify time = %.4f sec".format( (System.nanoTime - t0) / 1.0E9 ) )
//    if( matchedRdds.size == 1 ) matchedRdds.head else matchedRdds.tail.foldLeft( matchedRdds.head )( CDSparkContext.merge )
//  }

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
    Some( domains.map(dc => targetGrid.grid.getSubSection( dc.axes ) match {
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


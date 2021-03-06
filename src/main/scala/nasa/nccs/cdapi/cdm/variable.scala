package nasa.nccs.cdapi.cdm

import nasa.nccs.caching._
import nasa.nccs.cdapi.data._
import nasa.nccs.cdapi.tensors.{CDByteArray, CDFloatArray}
import nasa.nccs.edas.engine.{Workflow, WorkflowNode}
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.kernels.KernelContext
import nasa.nccs.edas.rdd.{CDRecord, QueryResultCollection}
import nasa.nccs.edas.sources.{Aggregation, Collection}
import nasa.nccs.esgf.process.{DataFragmentSpec, _}
import ucar.{ma2, nc2, unidata}
import ucar.nc2.dataset.{CoordinateAxis1D, _}
import nasa.nccs.utilities.{EDTime, Loggable, cdsutils}
import ucar.nc2.constants.AxisType
import scala.xml
import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

object BoundsRole extends Enumeration { val Start, End = Value }

object CDSVariable extends Loggable {
  def toCoordAxis1D(coordAxis: CoordinateAxis): CoordinateAxis1D = coordAxis match {
    case coordAxis1D: CoordinateAxis1D =>
     //  if( coordAxis1D.getShortName.equalsIgnoreCase("time") ){coordAxis1D.setUnitsString( EDTime.units ) }
      coordAxis1D
    case _ => throw new IllegalStateException("CDSVariable: 2D Coord axes not yet supported: " + coordAxis.getClass.getName)
  }
  def empty = new CDSVariable( null, null )
  def key_equals(key_regex: Regex)(map_item: (String, nc2.Attribute)): Boolean = {
    key_regex.findFirstIn(map_item._1) match { case Some(x) => true; case None => false; }
  }
  def filterAttrMap(raw_metadata: Map[String, nc2.Attribute],  keyRegExp: Regex, default: String ): String = {
    raw_metadata.find(item=>key_equals(keyRegExp)(item)) match { case Some(x) => x._2.toString.split('=').last.replace('"',' ').trim; case None => default }
  }
  def findAttributeValue( attributes: Map[String,nc2.Attribute], keyRegExp: String, default_value: String ): String = filterAttrMap( attributes, keyRegExp.r, default_value )
}

class CDSVariable( val name: String, val collection: Collection ) extends Loggable with Serializable {
  import CDSVariable._
  val attributes: Map[String,nc2.Attribute] = nc2.Attribute.makeMap( collection.getVariableMetadata( name ) ).toMap
  val missing = findAttributeValue( attributes, "^.*missing.*$", "" ) match {
    case "" =>
      logger.warn( "Can't find missing value, attributes = " + attributes.keys.mkString(", ") )
      Float.MaxValue;
    case s =>
      logger.info( "Found missing attribute value: " + s )
      s.toFloat
  }
  def getAggregation: Aggregation = collection.getAggregation( name ).getOrElse( throw new Exception(s"Can't find Aggregation for variable ${name} in collection ${collection.id}") )
  def getAttributeValue( key: String, default_value: String  ) =  attributes.get( key ) match { case Some( attr_val ) => attr_val.toString.split('=').last.replace('"',' ').trim; case None => default_value }
  val description = getAttributeValue( "description", "" )
  val units = getAttributeValue( "units", "" )
  val dims = getAttributeValue( "dims", "" ).split(' ')
  val shape = getAttributeValue( "shape", "" ).split(',').map( _.toInt )
  val fullname = getAttributeValue( "fullname", "" )
  val section = new ma2.Section( shape )
  def getFullSection: ma2.Section = section
  override def toString = "\nCDSVariable(%s) { description: '%s', shape: %s, dims: %s, }\n  --> Variable Attributes: %s".format(name, description, shape.mkString("[", " ", "]"), dims.mkString("[", ",", "]"), attributes.mkString("\n\t\t", "\n\t\t", "\n"))
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase
  def getAttributeValue( name: String ): String =  attributes.getOrElse(name, new nc2.Attribute(new unidata.util.Parameter("",""))).getValue(0).toString
  def toXml: xml.Node =
    <variable name={name} fullname={fullname} description={description} shape={shape.mkString("[", " ", "]")} units={units}>
      { for( dim: nc2.Dimension <- collection.getGrid(name).dimensions; name=dim.getFullName; dlen=dim.getLength ) yield getCoordinateAxis( name ) match {
          case None=> <dimension name={name} length={dlen.toString}/>
          case Some(axis)=>
              val units = axis.getAxisType match { case AxisType.Time =>{EDTime.units} case x => axis.getUnitsString }
              <dimension name={name} length={dlen.toString} start={axis.getStart.toString} units={units} step={axis.getIncrement.toString} cfname={axis.getAxisType.getCFAxisName}/>
        }
      }
      { for( name <- attributes.keys ) yield <attribute name={name}> { getAttributeValue(name) } </attribute> }
    </variable>

  def toXmlHeader: xml.Node = {
      collection.getGrid(name).getVariable(name) match {
        case Some( (index, variable) ) =>   <variable name={name} fullname={fullname} description={description} units={units} shape={shape.mkString("[", " ", "]")} dims={variable.getDimensionsString}/>
        case None =>                        <variable name={name} fullname={fullname} description={description} units={units} shape={shape.mkString("[", " ", "]")} />
      }
  }

  //  def read( section: ma2.Section ) = ncVariable.read(section)
  def getTargetGrid( fragSpec: DataFragmentSpec ): TargetGrid = fragSpec.targetGridOpt match { case Some(targetGrid) => targetGrid;  case None => new TargetGrid( this, Some(fragSpec.getAxes) ) }
  def getCoordinateAxes: List[ CoordinateAxis1D ] = {
    dims.flatMap( dim => collection.getGrid(name).findCoordinateAxis( dim ).map( coordAxis => CDSVariable.toCoordAxis1D( coordAxis ) ) ).toList
  }

  def coordinateAxesFromDims: Map[String,CoordinateAxis] = { collection.getGrid(name).coordinateAxesFromDims }

  def getCoordinateAxis( cname: String ): Option[CoordinateAxis1D] = {
    val caxis = collection.getGrid(name).findCoordinateAxis(cname)
    caxis.map( CDSVariable.toCoordAxis1D(_) )
  }
  def getCoordinateAxesList = collection.getGrid(name).getCoordinateAxes
}

class InputConsumer( val operation: OperationContext ) {
  private var _satiated = false;
  val id: String = operation.identifier
  def satiate(): Unit = _satiated = true;
  def satiated: Boolean = _satiated
}

trait OperationInput {
  private val _consumers = mutable.HashMap.empty[String,InputConsumer]
  val transient = true
  def getKeyString: String
  def matchesReference( objRef: Option[String] ): Boolean = false
  private def _registerConsumer( consumer: InputConsumer ): OperationInput = { _consumers += (consumer.id -> consumer); this }
  def registerConsumer( operation: OperationContext  ): OperationInput = { _registerConsumer( new InputConsumer(operation) ); this }
  def satiated: Boolean =  _consumers forall { case (uid,consumer) =>  consumer.satiated }
  def disposable: Boolean = transient && satiated
  private def unknown( operation: OperationContext ) = throw new Exception(s"Unrecognized operation ${operation.identifier} in opInput ${getKeyString}")
  def consume( op: OperationContext ): Unit = _consumers.getOrElse( op.identifier, unknown(op) ).satiate()
  def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int )
}
class EmptyOperationInput() extends OperationInput {
  def getKeyString: String = "";
  def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {;}
}

class DependencyOperationInput( val inputNode: WorkflowNode, val opNode: WorkflowNode ) extends OperationInput with Loggable {
  def getKeyString: String =  inputNode.getNodeId + "->" + opNode.getNodeId

  def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {
    val contents = executor.contents
    logger.info( s" @WW@ ProcessInput ${uid} for kernel ${node.getNodeId}, Vault contents: ${contents.mkString(",")}")
    if( !contents.contains(uid) ) inputNode.getProduct match {
      case None =>
        logger.info("\n\n @WW@ ----------------------- NODE %s => BEGIN Stream DEPENDENCY Node: %s, input: %s, batch = %d, rID = %s, contents = [ %s ] -------\n".format(node.getNodeId, uid, inputNode.getNodeId, batchIndex, inputNode.getResultId, executor.contents.mkString(", ")))
        workflow.stream(inputNode, executor, batchIndex)
        logger.info("\n\n @WW@ ----------------------- NODE %s => END   Stream DEPENDENCY Node: %s, input: %s, batch = %d, rID = %s, contents = [ %s ] -------\n".format(node.getNodeId, uid, inputNode.getNodeId, batchIndex, inputNode.getResultId, executor.contents.mkString(", ")))
      case Some(results: QueryResultCollection) =>
        val opSection: Option[CDSection] = kernelContext.getDomainSections.headOption
        logger.info("\n\n ----------------------- NODE %s => Get Cached Result: %s, batch = %d, rID = %s, opSection= %s -------\n".format(node.getNodeId, inputNode.getNodeId, batchIndex, inputNode.getResultId, opSection.map(_.toString()).getOrElse("(EMPTY)")))
        executor.addOperationInput(workflow.executionMgr.serverContext, results, opSection, batchIndex)
    }
  }
}

class OperationTransientInput( val variable: RDDTransientVariable ) extends OperationInput with Loggable {
//  def domainDataFragment( partIndex: Int,  optSection: Option[ma2.Section] ): Option[DataFragment] = variable.
  def getKeyString: String =  variable.request.getInputSpec match {
    case Some( dataFrag )=> dataFrag.getKeyString
    case None => variable.operation.inputs.mkString(":")
  }
  def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {

  }
}

abstract class OperationDataInput( val fragmentSpec: DataFragmentSpec, val metadata: Map[String,nc2.Attribute] = Map.empty ) extends OperationInput with Loggable {
  def toBoundsString = fragmentSpec.toBoundsString
  def getKey: DataFragmentKey = fragmentSpec.getKey
  def getKeyString: String = fragmentSpec.getKeyString
  def size: Long = fragmentSpec.roi.computeSize
  def contains( requestedSection: ma2.Section ): Boolean = fragmentSpec.roi.contains( requestedSection )
  def getVariableMetadata(serverContext: ServerContext): Map[String,nc2.Attribute] = { fragmentSpec.getVariableMetadata(serverContext) ++ metadata }
  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] = { fragmentSpec.getDatasetMetadata(serverContext) }
  override def matchesReference( objRef: Option[String] ): Boolean =
    fragmentSpec.matchesReference( objRef )
  def getGrid: TargetGrid = fragmentSpec.targetGridOpt match  {
    case Some( myGrid ) => myGrid
    case None => throw new Exception( "Undefined target grid in matchGrids for input " + fragmentSpec.uid )
  }
  def data(partIndex: Int ): CDFloatArray
  def delete
}

class DirectOpDataInput(fragSpec: DataFragmentSpec, workflowNode: WorkflowNode  )
  extends OperationDataInput( fragSpec, workflowNode.operation.getConfiguration.map { case (key,value) => key -> new nc2.Attribute( key, value) } ) {

  def data(partIndex: Int ): CDFloatArray = CDFloatArray.empty

  def delete: Unit = Unit

  def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {

  }

  def domainSection( optSection: Option[ma2.Section] ): Option[ ( DataFragmentSpec, ma2.Section )] = {
    try {
      val domain_section = fragmentSpec.domainSectOpt match {
        case Some(dsect) => fragmentSpec.roi.intersect(dsect)
        case None => fragmentSpec.roi
      }
      val sub_section = optSection match {
        case Some(osect) => domain_section.intersect( osect )
        case None =>domain_section
      }
      val result_section = fragmentSpec.cutIntersection( sub_section ) match {
        case Some( cut_spec: DataFragmentSpec ) => Some( ( cut_spec, cut_spec.roi ) )
        case None =>None
      }
//      logger.info( s" @DSX: result_section: ${result_section.fold(-1)(_._2.getRange(0).first())}, fragmentSpec.domainSect: ${fragmentSpec.domainSectOpt.fold(-1)(_.getRange(0).first())}, sub_section: ${sub_section.getRange(0).first()}, domain_section: ${domain_section.getRange(0).first()}, fragmentSpec.roi: ${fragmentSpec.roi.getRange(0).first()}" )
      result_section
    } catch {
      case ex: Exception =>
        logger.warn( s"Failed getting data fragment: " + ex.toString )
        None
    }
  }

  def getRDDVariableSpec( uid: String, optSection: Option[ma2.Section] = None ): DirectRDDVariableSpec  =
    domainSection(optSection) match {
      case Some( ( domFragSpec, section ) ) =>
        new DirectRDDVariableSpec( uid, domFragSpec.getMetadata( Some(section)), domFragSpec.missing_value, CDSection(section), fragSpec.varname, fragSpec.collection.collId )
      case _ =>
        new DirectRDDVariableSpec( uid, fragSpec.getMetadata(), fragSpec.missing_value, CDSection.empty(fragSpec.getRank), fragSpec.varname, fragSpec.collection.collId )
    }

  def getRDDVariableSpec: DirectRDDVariableSpec  =
    new DirectRDDVariableSpec( fragmentSpec.uid, fragmentSpec.getMetadata( Some(fragmentSpec.roi)), fragmentSpec.missing_value, CDSection(fragmentSpec.roi), fragmentSpec.varname, fragmentSpec.collection.collId )

//  def getKeyedRDDVariableSpec( uid: String, optSection: Option[ma2.Section] ): ( RecordKey, DirectRDDVariableSpec ) =
//    domainSection(optSection) match {
//      case Some( ( domFragSpec, section ) ) =>
//        domFragSpec.getPartitionKey -> new DirectRDDVariableSpec( uid, domFragSpec.getMetadata(Some(section)), domFragSpec.missing_value, CDSection(section), fragSpec.varname, fragSpec.collection.collId )
//      case _ =>
//        fragSpec.getPartitionKey -> new DirectRDDVariableSpec( uid, fragSpec.getMetadata(), fragSpec.missing_value, CDSection.empty(fragSpec.getRank), fragSpec.varname, fragSpec.collection.collId )
//    }
}

class EDASDirectDataInput(fragSpec: DataFragmentSpec, partsConfig: Map[String,String], workflowNode: WorkflowNode ) extends DirectOpDataInput(fragSpec,workflowNode) {
  val test = 1
//  def getPartitioner( optSection: Option[ma2.Section] = None ): Option[EDASPartitioner] = domainSection( optSection ) map {
//    case( frag1, section) => new EDASPartitioner( fragSpec.uid, section, partsConfig, fragSpec.getTimeCoordinateAxis, fragSpec.numDataFiles, RegridSpec(fragSpec) )
//  }
  override def data(partIndex: Int ): CDFloatArray = {
    CDFloatArray.empty
  }

  override def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {
    val opSection: Option[CDSection] = workflow.getOpSectionIntersection( gridRefInput.getGrid, node ).map( CDSection(_) )
    val varSpec = getRDDVariableSpec(uid)
    executor.addFileInputs( workflow.executionMgr.serverContext, kernelContext, List(varSpec), opSection, batchIndex )
  }
}

class ExternalDataInput(fragSpec: DataFragmentSpec, workflowNode: WorkflowNode ) extends DirectOpDataInput(fragSpec,workflowNode) {
  override def data(partIndex: Int ): CDFloatArray = CDFloatArray.empty
  override def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {
    throw new Exception(" ExternalDataInput is not currently supported as a Kernel input ")
  }
}

class PartitionedFragment( val partitions: CachePartitions, val maskOpt: Option[CDByteArray], fragSpec: DataFragmentSpec, mdata: Map[String,nc2.Attribute] = Map.empty ) extends OperationDataInput(fragSpec,mdata) with Loggable {
  val dbgId = 0

  def delete = partitions.delete

  def data(partIndex: Int ): CDFloatArray = partitions.getPartData(partIndex, fragmentSpec.missing_value )

  override def processInput(uid: String, workflow: Workflow, node: WorkflowNode, executor: WorkflowExecutor, kernelContext: KernelContext, gridRefInput: OperationDataInput, batchIndex: Int ) = {
    throw new Exception(" PartitionedFragment is not currently supported as a Kernel input ")
  }

  def partFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.reSection( part.partSection( fragmentSpec.roi ) )
  }
  def matchGrids( targetGrid: TargetGrid ): Boolean = fragSpec.targetGridOpt match  {
    case Some( myGrid ) => myGrid.equals( targetGrid )
    case None => throw new Exception( "Undefined target grid in matchGrids for input " + fragSpec.uid )
  }

  def domainFragSpec( partIndex: Int ): DataFragmentSpec = {
    val part = partitions.getPart(partIndex)
    fragmentSpec.domainSpec.reSection( part.partSection( fragmentSpec.roi ) )
  }

  def partDataFragment( partIndex: Int ): DataFragment = {
    val partition = partitions.getPart(partIndex)
    DataFragment( partFragSpec(partIndex), partition.data( fragmentSpec.missing_value ) )
  }

//  def domainRDDPartition(partIndex: Int, optSection: Option[ma2.Section] ): Option[RDDPartition] = domainCDDataSection( partIndex, optSection ) match {
//    case Some((uid, section, metadata, data)) => Some(  RDDPartition( Map( uid -> HeapFltArray(data, section.getOrigin, metadata, None ) ) ) )
//    case None => None
//  }

//  def domainDataFragment(partIndex: Int, optSection: Option[ma2.Section] ): Option[DataFragment] = domainDataSection( partIndex, optSection ) match {
//    case Some((spec, data)) => Some( DataFragment(spec, data) )
//    case None => None
//  }
//
//  def domainDataSection( partIndex: Int,  optSection: Option[ma2.Section] ): Option[ ( DataFragmentSpec, CDFloatArray )] = {
//    try {
//      val partition = partitions.getPart(partIndex)
//      val partition_data = partition.data(fragmentSpec.missing_value)
//      domainSection(partition, optSection) map {
//        case (fragSpec, section) => (fragSpec, CDFloatArray(partition_data.section(section)))
//      }
//    } catch {
//      case ex: Exception => logger.warn(s"Failed getting data fragment $partIndex: " + ex.toString)
//        None
//    }
//  }

//  def domainDataFragment( partIndex: Int, context: EDASExecutionContext ): Option[DataFragment] = {
//    val optSection: Option[ma2.Section] = context.getOpSections match {
//      case None => return None
//      case Some( sections ) =>
////        logger.info( "OP sections: " + sections.map( _.toString ).mkString( "( ", ", ", " )") )
//        if( sections.isEmpty ) None
//        else {
//          val result = sections.foldLeft(sections.head)( _.intersect(_) )
////          logger.info( "OP sections: %s >>>>---------> intersection: %s".format( sections.map( _.toString ).mkString( "( ", ", ", " )"), result.toString ) )
//          if (result.computeSize() > 0) { Some(result) }
//          else return None
//        }
//    }
//  }

//  def domainCDDataSection( partIndex: Int,  optSection: Option[ma2.Section] ): Option[ ( String, ma2.Section, Map[String,String], CDFloatArray )] = {
//    try {
//      val partition = partitions.getPart(partIndex)
//      val partition_data = partition.data(fragmentSpec.missing_value)
//      domainSection( partition, optSection ) map {
//        case ( domFragSpec, section )  => ( domFragSpec.uid, section, domFragSpec.getMetadata(optSection), CDFloatArray( partition_data.section( section ) ) )
//      }
//    } catch {
//      case ex: Exception => logger.warn( s"Failed getting data fragment $partIndex: " + ex.toString )
//        None
//    }
//  }

  def getRDDVariableSpec(uid: String, partition: RegularPartition, optSection: Option[ma2.Section] ): RDDVariableSpec =
    domainSection(partition,optSection) match {
      case Some( ( domFragSpec, section ) ) =>
        new RDDVariableSpec( uid, domFragSpec.getMetadata( Some(section) ), domFragSpec.missing_value, CDSection(section) )
      case _ =>
        new RDDVariableSpec( uid, fragSpec.getMetadata(), fragSpec.missing_value, CDSection.empty(fragSpec.getRank) )
    }


  def domainSection(partition: RegularPartition, optSection: Option[ma2.Section] ): Option[ ( DataFragmentSpec, ma2.Section )] = {
    try {
      val frag_section = partition.partSection(fragmentSpec.roi)
      val domain_section = fragmentSpec.domainSectOpt match {
        case Some(dsect) => frag_section.intersect(dsect)
        case None => frag_section
      }
      val partFragSpec = domainFragSpec(partition.index)
      val sub_section = optSection match {
        case Some(osect) =>
          val rv = domain_section.intersect( osect )
//          logger.info( "OP section intersect: " + osect.toString + ", result = " + rv.toString )
          rv
        case None =>
//          logger.info( "OP section empty" )
          domain_section
      }
      partFragSpec.cutIntersection( sub_section ) match {
        case Some( cut_spec: DataFragmentSpec ) =>
//          val offset: Array[Int] = (0 until frag_section.getRank).map( index => if( index == 0 ) frag_section.getOrigin(0) else 0 ).toArray
//          val array_section = cut_spec.roi.shiftOrigin( new ma2.Section( offset, Array.fill[Int](frag_section.getRank)(1) ) )
          Some( ( cut_spec, cut_spec.roi ) )
        case None =>None
      }
    } catch {
      case ex: Exception =>
        logger.warn( s"Failed getting data fragment " + partition.index + ": " + ex.toString )
        //        logger.error( ex.getStackTrace.mkString("\n\t") )
        None
    }
  }

      //      val domainDataOpt: Option[CDFloatArray] = fragmentSpec.domainSectOpt match {
//        case None => Some( partition.data(fragmentSpec.missing_value) )
//        case Some(domainSect) =>
//          val pFragSpec = partFragSpec( partIndex )
//          pFragSpec.cutIntersection(domainSect) match {
//            case Some(newFragSpec) =>
//              val dataSection = partition.getRelativeSection( newFragSpec.roi ).shiftOrigin( domainSect )
//              logger.info ("Domain Partition(%d) Fragment: fragSect=(%s), newFragSect=(%s), domainSect=(%s), dataSection=(%s), partition.shape=(%s)".format (partIndex, pFragSpec.roi.toString, newFragSpec.roi, domainSect.toString, dataSection.toString, partition.shape.mkString(",")) )
//              Some( partition.data (fragmentSpec.missing_value).section (dataSection.getRanges.toList) )
//            case None =>
//              logger.warn( "Domain Partition(%d) EMPTY INTERSECTION: fragSect=(%s), domainSect=(%s)".format (partIndex, pFragSpec.roi.toString, domainSect.toString) )
//              None
//          }
//      }
//      domainDataOpt.map( new DataFragment(domainFragSpec(partIndex), _ ) )


  def isMapped(partIndex: Int): Boolean = partitions.getPartData( partIndex, fragmentSpec.missing_value ).isMapped
  def mask: Option[CDByteArray] = maskOpt
  def shape: List[Int] = partitions.getShape.toList
  def getValue(partIndex: Int, indices: Array[Int] ): Float = data(partIndex).getValue( indices )

  override def toString = { "{Fragment: shape = [%s], section = [%s]}".format( partitions.getShape.mkString(","), fragmentSpec.roi.toString ) }

  def cutIntersection( partIndex: Int, cutSection: ma2.Section, copy: Boolean = true ): Option[DataFragment] = {
    val pFragSpec = partFragSpec( partIndex )
    pFragSpec.cutIntersection(cutSection) map { newFragSpec =>
        val newDataArray: CDFloatArray = data (partIndex).section (newFragSpec.roi.shiftOrigin (pFragSpec.roi).getRanges.toList)
        DataFragment ( newFragSpec, if (copy) newDataArray.dup () else newDataArray )
    }
  }
}


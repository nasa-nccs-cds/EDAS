package nasa.nccs.esgf.process

import nasa.nccs.caching.{EDASPartitioner, JobRecord}
import nasa.nccs.cdapi.cdm.{CDSVariable, MetaCollectionFile, PartitionedFragment}
import nasa.nccs.cdapi.tensors.CDFloatArray.ReduceOpFlt
import nasa.nccs.cdapi.tensors.{CDCoordMap, CDFloatArray}
import ucar.{ma2, nc2}
import org.joda.time.{DateTime, DateTimeZone}
import nasa.nccs.utilities.Loggable
import java.util.UUID
import scala.xml
import nasa.nccs.cdapi.data.RDDVariableSpec
import nasa.nccs.edas.engine.spark.RecordKey
import nasa.nccs.edas.engine.{EDASExecutionManager, Workflow}
import nasa.nccs.edas.kernels.AxisIndices
import nasa.nccs.edas.sources.{Aggregation, Collection, CollectionLoadServices, Collections}
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.process.UID.ndigits

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.collection.{immutable, mutable}
import scala.collection.mutable.HashSet
import mutable.ListBuffer
import nasa.nccs.esgf.utilities.numbers.GenericNumber
import nasa.nccs.esgf.utilities.wpsNameMatchers
import nasa.nccs.esgf.wps.edasServiceProvider
import nasa.nccs.wps._
import org.apache.commons.lang.RandomStringUtils
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.CoordinateAxis1DTime

import scala.io.Source
import scala.util.Random

case class ErrorReport(severity: String, message: String) {
  override def toString = {
    s"ErrorReport { severity: $severity, message: $message }"
  }

  def toXml = {
    <error severity={severity} message={message}/>
  }
}

case class User( id: String="guest", authorization_level: Int = 0 ) { }

class TaskRequest(val id: UID,
                  val name: String,
                  val variableMap: Map[String, DataContainer],
                  val domainMap: Map[String, DomainContainer],
                  val operations: Seq[OperationContext] = List(),
                  val metadata: Map[String, String] = Map("id" -> "#META"),
                  val user: User = User())
    extends Loggable {
  val errorReports = new ListBuffer[ErrorReport]()
  val targetGridMap = scala.collection.mutable.HashMap.empty[String, TargetGrid]
  validate()
  val workflow = Workflow(this, edasServiceProvider.cds2ExecutionManager);
  logger.info(s"TaskRequest: name= $name, workflows= " + operations.mkString(",") + ", variableMap= " + variableMap.toString + ", domainMap= " + domainMap.toString + "\n @N@ nodes = " + workflow.nodes.map(_.toString).mkString(", "))
  val getUserAuth: Int = user.authorization_level

  def addErrorReport(severity: String, message: String) = {
    val error_rep = ErrorReport(severity, message)
    logger.error(error_rep.toString)
    errorReports += error_rep
  }
  def getTargetGrid(dataContainer: DataContainer): TargetGrid =
    targetGridMap.getOrElseUpdate( dataContainer.uid, createTargetGrid(dataContainer) )
  def getTargetGrid(uid: String): Option[TargetGrid] = targetGridMap.get(uid)

  private def createTargetGrid(dataContainer: DataContainer): TargetGrid = {
    val domainContainer: Option[DomainContainer] = dataContainer.getSource.getDomain flatMap ( getDomain )
    val roiOpt: Option[List[DomainAxis]] = domainContainer.map(domainContainer => domainContainer.axes)
    val t0 = System.nanoTime
    lazy val variable: CDSVariable = dataContainer.getVariable
    val t1 = System.nanoTime
    val rv = new TargetGrid(variable, roiOpt)
    val t2 = System.nanoTime
 //   logger.info( " CreateTargetGridT: %.4f %.4f ".format((t1 - t0) / 1.0E9, (t2 - t1) / 1.0E9))
    rv
  }

  def getProcess: WPSWorkflowProcess =
    new WPSWorkflowProcess(id.toString,
                           getDescription,
                           "Workflow " + name,
                           getInputs,
                           getOutputs)

  def getInputs: List[WPSDataInput] =
    inputVariables.map(_.toWPSDataInput).toList

  def getOutputs: List[WPSProcessOutput] =
    List(WPSProcessOutput(id.toString, "text/xml", "Workflow Output"))

  def getJobRec1(run_args: Map[String, String]): JobRecord = {
    val jobIds = for (node <- workflow.roots) yield node.operation.rid
    new JobRecord(jobIds.mkString(";"))
  }

  def getJobRec(run_args: Map[String, String]): JobRecord = {
    new JobRecord(name)
  }

  def isMetadataRequest: Boolean =
    name.split('.').last.toLowerCase().equals("metadata")

  def getDataAccessMode(): DataAccessMode =
    name.split('.').last.toLowerCase match {
      case "metadata" => DataAccessMode.MetaData
      case "cache" => DataAccessMode.Cache
      case _ => DataAccessMode.Read
    }

  def getDomain(data_source: DataSource): Option[DomainContainer] = data_source.getDomain.map( domId => domainMap.getOrElse(domId, throw new Exception("Undefined domain for dataset " + data_source.name + ", domain = " + domId) ) )
  def getDomain(domId: String): Option[DomainContainer] = domainMap.get(domId)

  def validate() = {
    for (variable <- inputVariables; if variable.isSource; domid <- variable.getSource.getDomain.iterator;  vid = variable.getSource.name; if !domid.isEmpty ) {
      if (!domainMap.contains(domid) && !domid.contains("|")) {
        var keylist = domainMap.keys.mkString("[", ",", "]")
        logger.error(s"Error, No $domid in $keylist in variable $vid")
        throw new Exception(s"Error, Missing domain $domid in variable $vid")
      }
    }
    for (operation <- operations; opid = operation.name; var_arg <- operation.inputs; if !var_arg.isEmpty) {
      if (!variableMap.contains(var_arg)) {
        var keylist = variableMap.keys.mkString("[", ",", "]")
        logger.error(s"Error, No $var_arg in $keylist in operation $opid")
        throw new Exception(
          s"Error, Missing variable $var_arg in operation $opid")
      }
    }
  }

  override def toString = {
    var taskStr =
      s"TaskRequest { \n\tname='$name', \n\tvariables = ${variableMap.mkString("\n\t\t","\n\t\t","\n\t\t")}, \n\tdomains=${domainMap.mkString("\n\t\t","\n\t\t","\n\t\t")}, \n\tworkflows=${operations.mkString("\n\t\t","\n\t\t","\n\t\t")} }"
    if (errorReports.nonEmpty) {
      taskStr += errorReports.mkString("\nError Reports: {\n\t", "\n\t", "\n}")
    }
    taskStr
  }

  def getDescription: String =
    "Processes { %s }".format(operations.map(_.toString))

  def toXml = {
    <task_request name={name}>
      <data>
        { inputVariables.map(_.toXml )  }
      </data>
      <domains>
        {domainMap.values.map(_.toXml ) }
      </domains>
      <operation>
        { operations.map(_.toXml ) }
      </operation>
      <error_reports>
        {errorReports.map(_.toXml ) }
      </error_reports>
    </task_request>
  }

  def inputVariables: Traversable[DataContainer] = {
    for (variableSource <- variableMap.values;
         if variableSource.isInstanceOf[DataContainer])
      yield variableSource.asInstanceOf[DataContainer]
  }
}

//object UID1 {
//  def apply() = new UID()
//}
//class UID1 {
//  val uid = UUID.randomUUID()
//  def +( id: String ): String = id + "-" + uid.toString
//}

object UID {
  def ndigits = 6
  def apply( rId: String = RandomStringUtils.random(ndigits, true, true) ) = new UID(rId)
}
class UID( val uid: String  ) {
  def +(id: String): String = id + "-" + uid.toString
  override def toString = uid
}

object TaskRequest extends Loggable {
  def apply(rId: String, process_name: String, datainputs: Map[String, Seq[Map[String, Any]]]) = {
    logger.info( "TaskRequest--> process_name: %s, datainputs: %s".format(process_name, datainputs.toString))
    val uid = UID(rId)
    val op_spec_list: Seq[Map[String, Any]] = datainputs .getOrElse("operation", List())
    val data_list: List[DataContainer] = datainputs.getOrElse("variable", List()).flatMap(DataContainer.factory(uid, _, op_spec_list.isEmpty )).toList
    val domain_list: List[DomainContainer] = datainputs.getOrElse("domain", List()).map(DomainContainer(_)).toList
    val opSpecs: Seq[Map[String, Any]] = if(op_spec_list.isEmpty) { getEmptyOpSpecs(data_list) } else { op_spec_list }
    val operation_map: Map[String,OperationContext] = Map( opSpecs.map (  op => OperationContext( uid, process_name, data_list.map(_.uid), op) ) map ( opc => opc.identifier -> opc ) :_* )
    val operation_list: Seq[OperationContext] = operation_map.values.toSeq
    val variableMap: Map[String, DataContainer] = buildVarMap(data_list, operation_list)
    val domainMap: Map[String, DomainContainer] = buildDomainMap(domain_list)
    inferDomains(operation_list, variableMap )
    val gridId = datainputs.getOrElse("grid", data_list.headOption.map(dc => dc.uid).getOrElse("#META")).toString
    val gridSpec = Map("id" -> gridId.toString)
    val rv = new TaskRequest( uid, process_name, variableMap, domainMap, operation_list, gridSpec )
    logger.info( " -> Generated TaskRequest, uid = " + uid.toString )
    rv
  }

  def inferDomains(operation_list: Seq[OperationContext], variableMap: Map[String, DataContainer] ): Unit =
    do operation_list.foreach( _.addInputDomains(variableMap) ) while( addVarDomainsFromOp(operation_list, variableMap) )

  def addVarDomainsFromOp( operation_list: Seq[OperationContext], variableMap: Map[String, DataContainer] ): Boolean =
    operation_list.exists( operation => operation.inputs.exists( vid => if( vid.isEmpty ) { false } else {
      val rv = variableMap.get(vid).exists( _.updateDomainsFromOperation( operation ) )
      rv
    } ) )

  def getEmptyOpSpecs( data_list: List[DataContainer] ): Seq[Map[String, Any]] = {
    val inputs: List[String] = data_list.flatMap( dc => if( dc.isSource ) { Some(dc.uid) } else { None } )
    IndexedSeq( Map( "name"->"CDSpark.noOp", "input"->inputs ) )
  }

  def buildVarMap( data: Seq[DataContainer], workflow: Seq[OperationContext]): Map[String, DataContainer] = {
    var data_var_items = for (data_container <- data)
      yield data_container.uid -> data_container
    var op_var_items = for (operation <- workflow; if !operation.rid.isEmpty)
      yield operation.rid -> DataContainer(operation)
    val var_map = Map(op_var_items ++ data_var_items: _*)
//    logger.info( "Created Variable Map: op_var_items = (%s), data_var_items = (%s)".format( op_var_items.map(_._1).mkString(","), data_var_items.map(_._1).mkString(",") ) )
    logger.info( "Created Variable Map: " + var_map.toString + " from data containers: " + data.map(data_container => ("id:" + data_container.uid)).mkString("[ ", ", ", " ]"))
    var_map
  }

  def buildDomainMap(domain_containers: List[DomainContainer]): Map[String, DomainContainer] = {
    val domain_map = domain_containers
      .map(domain_container => domain_container.name -> domain_container)
      .toMap
//    logger.info( "Created Domain Map: " + domain_map.toString )
    domain_map
  }
}

trait ContainerOps {
  def item_key(map_item: (String, Any)): String = map_item._1

  def normalize(sval: String): String = stripQuotes(sval).toLowerCase

  def normalizeOpt(sval: String): Option[String] = {
    val svalNorm = stripQuotes(sval).toLowerCase
    if( svalNorm.isEmpty ) None else Some( svalNorm )
  }

  def stripQuotes(sval: String): String = sval.replace('"', ' ').trim

  def getStringKeyMap(generic_map: Map[_, _]): Map[String, Any] = {
    assert(generic_map.isEmpty | generic_map.keys.head.isInstanceOf[String])
    generic_map.asInstanceOf[Map[String, Any]]
  }

  def key_equals(key_value: String)(map_item: (String, Any)): Boolean = {
    item_key(map_item) == key_value
  }

  def key_equals(key_regex: Regex)(map_item: (String, Any)): Boolean = {
    key_regex.findFirstIn(item_key(map_item)) match {
      case Some(x) => true;
      case None => false;
    }
  }

  //  def key_equals( key_expr: Iterable[Any] )( map_item: (String, Any) ): Boolean = { key_expr.map( key_equals(_)(map_item) ).find( ((x:Boolean) => x) ) }
  def filterMap(raw_metadata: Map[String, Any],
                key_matcher: (((String, Any)) => Boolean)): Option[Any] = {
    raw_metadata.find(key_matcher) match {
      case Some(x) => Some(x._2)
      case None => None
    }
  }

  def getGenericNumber(opt_val: Option[Any]): GenericNumber = {
    opt_val match {
      case Some(p) => GenericNumber(p)
      case None => GenericNumber()
    }
  }
  def getStringValue(opt_val: Option[Any]): String = {
    opt_val match {
      case Some(p) => p.toString
      case None => ""
    }
  }
}

class ContainerBase extends Loggable with ContainerOps {
  def toXml = <container>  { toString } </container>
}


class PartitionSpec(val axisIndex: Int, val nPart: Int, val partIndex: Int = 0) {
  override def toString =
    s"PartitionSpec { axis = $axisIndex, nPart = $nPart, partIndex = $partIndex }"
}

class DataSource(val name: String, val collection: Collection, val declared_domain: Option[String], val autoCache: Boolean, val fragIdOpt: Option[String] = None) extends Loggable {
  private val _inferredDomains = new scala.collection.mutable.HashSet[String]()
  declared_domain.foreach( _addDomain )
  def this(dsource: DataSource) = this(dsource.name, dsource.collection, dsource.getDomain, dsource.autoCache )
  override def toString: String = s"DataSource { name = $name, \n\t\t\tcollection = %s, domain = ${declared_domain}, %s }" .format(collection.toString, fragIdOpt.map(", fragment = " + _).getOrElse(""))
  def toXml: xml.Node =  <dataset name={name} domain={declared_domain.getOrElse("")}> {collection.toXml} </dataset>
  def isDefined: Boolean = !collection.isEmpty && !name.isEmpty
  def isReadable: Boolean = !collection.isEmpty && !name.isEmpty && !declared_domain.isEmpty
  def getKey: Option[DataFragmentKey] = fragIdOpt map DataFragmentKey.apply
  def getDomain: Option[String] = _inferredDomains.headOption

  private def _addDomain( domain_id: String ): Boolean = if( _inferredDomains.contains(domain_id) || domain_id.isEmpty ) { false; } else {
    logger.info( s" ----> Adding Domain ${domain_id}")
    _inferredDomains += domain_id;
    true
  }
  def getAggregation( varName: String  ): Option[Aggregation] = collection.getAggregation(varName)

  def updateDomainsFromOperation( operation: OperationContext ): Boolean = if( declared_domain.isEmpty ) {
    logger.info( s"DataSource(${collection.collId}:${name})--> Update Domains From Operation(${operation.name}) --> op domains: ${operation.getDomains.mkString(", ")}")
    val updated = operation.getDomains.exists( _addDomain )
    if( _inferredDomains.size > 1 ) { logger.warn( s"Ambiguous Domain for input ${name}, domains: ${_inferredDomains.mkString(", ")}") }
    updated
  } else { false }
}

class DataFragmentKey(val varname: String, val collId: String, val origin: Array[Int], val shape: Array[Int] ) extends Serializable {
  override def toString = "DataFragmentKey{ name = %s, collection = %s, origin = [ %s ], shape = [ %s ] }".format(varname, collId, origin.mkString(", "), shape.mkString(", "))
  def toStrRep = "%s|%s|%s|%s".format(varname,  collId,  origin.mkString(","),  shape.mkString(",") )
  def sameVariable(otherCollId: String, otherVarName: String): Boolean = (varname == otherVarName) && (collId == otherCollId)
  def getRoi: ma2.Section = new ma2.Section(origin, shape)
  def equalRoi(df: DataFragmentKey): Boolean = shape.sameElements(df.shape) && origin.sameElements(df.origin)
  def getSize: Int = shape.foldLeft(1)(_ * _)
  def contains(df: DataFragmentKey): Boolean = getRoi.contains(df.getRoi)
  def contains(df: DataFragmentKey, admitEquality: Boolean): Boolean =  if (admitEquality) contains(df) else containsSmaller(df)
  def containsSmaller(df: DataFragmentKey): Boolean =  !equalRoi(df) && contains(df)
}

object DataFragmentKey {
  def parseArray(arrStr: String): Array[Int] = {
    arrStr.split(',').map(_.toInt)
  }
  def apply(fkeyStr: String): DataFragmentKey = {
    val toks = fkeyStr.split('|')
    if( toks.length < 4 ) {
      throw new Exception( "Ill formed frag key (varname|collId|origin|shape): " + fkeyStr )
    }
    new DataFragmentKey( toks(0), toks(1), parseArray(toks(2)), parseArray(toks(3)) )
  }
  def sameVariable(fkeyStr: String, otherCollection: String, otherVarName: String): Boolean = {
    val toks = fkeyStr.split('|')
    (toks(0) == otherVarName) && (toks(1) == otherCollection)
  }
}

object DataFragmentSpec {

  def offset(section: ma2.Section, newOrigin: ma2.Section): ma2.Section = {
    assert(newOrigin.getRank == section.getRank, "Invalid Section rank in offset")
    val new_ranges = for (i <- (0 until section.getRank); range = section.getRange(i); origin = newOrigin.getRange(i))
      yield range.shiftOrigin(-origin.first())
    new ma2.Section(new_ranges)
  }
}

object MergeDataFragment {
  def apply(df: DataFragment): MergeDataFragment =
    new MergeDataFragment(Some(df))
  def apply(): MergeDataFragment = new MergeDataFragment()
}
class MergeDataFragment(val wrappedDataFragOpt: Option[DataFragment] = None) {
  def ++(dfrag: DataFragment): MergeDataFragment = wrappedDataFragOpt match {
    case None => MergeDataFragment(dfrag)
    case Some(wrappedDataFrag) => MergeDataFragment(wrappedDataFrag ++ dfrag)
  }
}
// DataFragmentSpec, SectionMerge.Status  DataFragmentSpec, SectionMerge.Status
object DataFragment {
  def apply(spec: DataFragmentSpec, data: CDFloatArray): DataFragment =
    new DataFragment(spec, Map("value" -> data))
  def apply(spec: DataFragmentSpec,
            data: CDFloatArray,
            weights: CDFloatArray): DataFragment =
    new DataFragment(spec, Map(("value" -> data), ("weights" -> weights)))
  def apply(spec: DataFragmentSpec,
            data: CDFloatArray,
            coordMap: CDCoordMap): DataFragment =
    new DataFragment(spec, Map(("value" -> data)), Some(coordMap))
  def apply(spec: DataFragmentSpec,
            data: CDFloatArray,
            weights: CDFloatArray,
            coordMap: CDCoordMap): DataFragment =
    new DataFragment(spec,
                     Map(("value" -> data), ("weights" -> weights)),
                     Some(coordMap))
  def combine(reductionOp: ReduceOpFlt,
              input0: DataFragment,
              input1: DataFragment): DataFragment = {
    val (data, (fragSpec, mergeStatus)) = input0.optCoordMap match {
      case Some(coordMap) =>
        (CDFloatArray.combine(reductionOp,
                              input1.data,
                              input0.data,
                              coordMap.subset(input1.spec.roi)),
         input1.spec.combine(input0.spec, false))
      case None =>
        input1.optCoordMap match {
          case Some(coordMap) =>
            (CDFloatArray.combine(reductionOp,
                                  input0.data,
                                  input1.data,
                                  coordMap.subset(input0.spec.roi)),
             input0.spec.combine(input1.spec, false))
          case None =>
            (CDFloatArray.combine(reductionOp, input0.data, input1.data),
             input0.spec.combine(input1.spec, true))
        }
    }
    DataFragment(fragSpec, data)
  }
  def combineCoordMaps(a0: DataFragment,
                       a1: DataFragment): Option[CDCoordMap] =
    a0.optCoordMap.flatMap(coordMap0 =>
      a1.optCoordMap.map(coordMap1 => coordMap0 ++ coordMap1))
  def combineDataMaps(a0: DataFragment,
                      a1: DataFragment): Map[String, CDFloatArray] =
    a0.dataMap flatMap {
      case (key, array0) =>
        a1.dataMap.get(key) map (array1 => (key -> array0.append(array1)))
    }
}

class DataFragment(val spec: DataFragmentSpec,
                   val dataMap: Map[String, CDFloatArray] =
                     Map.empty[String, CDFloatArray],
                   val optCoordMap: Option[CDCoordMap] = None)
    extends Serializable {
  import DataFragment._
  def ++(dfrag: DataFragment): DataFragment = {
    new DataFragment(spec.append(dfrag.spec),
                     combineDataMaps(this, dfrag),
                     combineCoordMaps(this, dfrag))
  }
  def data: CDFloatArray = dataMap.get("value").get
  def weights: Option[CDFloatArray] = dataMap.get("weights")
  def getReducedSpec(axes: AxisIndices): DataFragmentSpec =
    spec.reduce(Set(axes.getAxes: _*))
  def getReducedSpec(axisIndices: Set[Int],
                     newsize: Int = 1): DataFragmentSpec =
    spec.reduce(axisIndices, newsize)
  def subset(section: ma2.Section): Option[DataFragment] =
    spec.cutIntersection(section) map { dataFragSpec =>
      val new_section = dataFragSpec.getIntersection(section)
      new DataFragment(
        dataFragSpec,
        dataMap
          .mapValues(array =>
            CDFloatArray.cdArrayConverter(array.section(new_section)))
          .map(identity),
        optCoordMap) // map(identity) to work around scala serialization bug
    }
}

object SectionMerge {
  type Status = Int
  val Overlap: Status = 0
  val Append: Status = 1
  val Prepend: Status = 1
  def incommensurate(s0: ma2.Section, s1: ma2.Section) = {
    "Attempt to combine incommensurate sections: %s vs %s".format(s0.toString,
                                                                  s1.toString)
  }
}


class DataInputSpec( ) {

}

class DataFragmentSpec(val uid: String,
                       val varname: String,
                       val collection: Collection,
                       val fragIdOpt: Option[String] = None,
                       val targetGridOpt: Option[TargetGrid] = None,
                       val dimensions: String = "",
                       val units: String = "",
                       val longname: String = "",
                       private val _section: ma2.Section = new ma2.Section(),
                       private val _domSectOpt: Option[ma2.Section],
                       private val _metadata: Map[String,String],
                       val missing_value: Float,
                       val numDataFiles: Int,
                       val mask: Option[String] = None,
                       val autoCache: Boolean = false
                      ) extends Loggable with Serializable {

  val dbgInt = 0
//  logger.debug("DATA FRAGMENT SPEC: section: %s, _domSectOpt: %s".format(_section, _domSectOpt.getOrElse("null").toString))
  override def toString = "DataFragmentSpec { varname = %s, collection = %s, dimensions = %s, units = %s, longname = %s, roi = %s }".format(varname, collection, dimensions, units, longname, CDSection.serialize(roi))
  def sameVariable(otherCollection: String, otherVarName: String): Boolean = {
    (varname == otherVarName) && (collection == otherCollection)
  }
  def toXml = {
    mask match {
      case None =>
        <input uid={uid} varname={varname} longname={longname} units={units} roi={roi.toString} >{collection.toXml}</input>
      case Some(maskId) =>
        <input uid={uid} varname={varname} longname={longname} units={units} roi={roi.toString} mask={maskId} >{collection.toXml}</input>
    }
  }
  def matchesReference( objRefOpt: Option[String] ): Boolean =
    objRefOpt.fold(true)( objRef => objRef.equalsIgnoreCase(collection.id) || objRef.equalsIgnoreCase( uid.split('-').head ) )
  def getMetadata( key: String ): Option[String] = _metadata.get( key )

  def domainSection: Option[ DataFragmentSpec ] = {
    try {
      val domain_section = domainSectOpt match {
        case Some(dsect) => roi.intersect(dsect)
        case None => roi
      }
      cutIntersection( domain_section )
    } catch {
      case ex: Exception =>
        logger.warn( s"Failed getting data fragment: " + ex.toString )
        None
    }
  }

//  def getPartitionKey: RecordKey = targetGridOpt match {
//      case Some(grid) =>
//        val trange = roi.getRange(0)
//        val start = trange.first()
//        val startDate = grid.getCalendarDate(start,"getPartitionKey")
//        val startTime = startDate.getMillis / 1000
//        val end = trange.last()
//        val endDate = grid.getCalendarDate(end,"getPartitionKey")
//        val endTime = endDate.getMillis / 1000
//        RecordKey(startTime, endTime, start, end-start )
//    case None => throw new Exception( "Missing target grid ")
//  }

  def readData(section: ma2.Section) = collection.readVariableData(varname, section)
  def getVariableMetadata: Map[String, nc2.Attribute] = nc2.Attribute.makeMap(collection.getVariableMetadata(varname)).toMap

  def getMetadata( section: Option[ma2.Section] = None): Map[String, String] = Map (
      "name" -> varname,
      "collection" -> collection.id,
      "dataPath" -> collection.dataPath,
      "uri" -> collection.dataPath,
      "gridfile" -> collection.getGridFilePath( varname ), // "gridbnds" -> getBounds(section).map(_.toFloat).mkString(","),
      "fragment" -> fragIdOpt.getOrElse(""),
      "dimensions" -> dimensions,
      "units" -> units,
      "longname" -> longname,
      "uid" -> uid,
      "roi" -> CDSection.serialize(roi)
    ) ++ _metadata

  def getVariable: CDSVariable = collection.getVariable(varname)

  def combine(other: DataFragmentSpec, sectionMerge: Boolean = true): (DataFragmentSpec, SectionMerge.Status) = {
    val combined_varname = varname + ":" + other.varname
    val combined_longname = longname + ":" + other.longname
    val (combined_section, mergeStatus) = if (sectionMerge) combineRoi(other.roi) else (roi, SectionMerge.Overlap)
    new DataFragmentSpec(uid, combined_varname, collection, None, targetGridOpt, dimensions, units, combined_longname, combined_section, _domSectOpt, _metadata, missing_value, numDataFiles, mask, autoCache ) -> mergeStatus
  }
  def roi = targetGridOpt match {
    case None =>
      new ma2.Section(_section)
    case Some(targetGrid) =>
      targetGrid.addSectionMetadata(_section)
  }
  def cdsection: CDSection =
    CDSection(roi)

  def domainSectOpt = _domSectOpt.map(sect => new ma2.Section(sect))

  def toBoundsString = { targetGridOpt.map(_.toBoundsString).getOrElse("") }

  def reshape(newSection: ma2.Section): DataFragmentSpec =
    new DataFragmentSpec(uid,
                         varname,
                         collection,
                         fragIdOpt,
                         targetGridOpt,
                         dimensions,
                         units,
                         longname,
                         new ma2.Section(newSection),
                         domainSectOpt,
                         _metadata,
                         missing_value,
                         numDataFiles,
                         mask, autoCache )

  def getBounds(section: Option[ma2.Section] = None): Array[Double] = {
    val bndsOpt = targetGridOpt.flatMap(targetGrid =>
      targetGrid.getBounds(section.getOrElse(roi)))
    bndsOpt.getOrElse(
      throw new Exception("Can't get bounds from FragmentSpec: " + toString))
  }

  private def collapse(range: ma2.Range, newsize: Int = 1): ma2.Range =
    newsize match {
      case 1 =>
        val mid_val = (range.first + range.last) / 2;
        new ma2.Range(range.getName, mid_val, mid_val)
      case ns =>
        val incr = math.round((range.last - range.first) / ns.toFloat);
        new ma2.Range(range.getName, range.first(), range.last, incr)
    }
  def getRange(dimension_name: String): Option[ma2.Range] = {
    val dims = dimensions.toLowerCase.split(',')
    dims.indexOf(dimension_name.toLowerCase) match {
      case -1 => None
      case x => Option(roi.getRange(x))
    }
  }

  def getRangeCF(CFName: String): Option[ma2.Range] = Option(roi.find(CFName))

  def getShape = roi.getShape
  def getOrigin = roi.getOrigin
  def getRank = roi.getShape.length

  def getGridShape: Array[Int] = {
    val grid_axes = List("x", "y")
    val ranges = dimensions.toLowerCase.split(' ').flatMap(getRange)
    ranges.map(range =>
      if (grid_axes.contains(range.getName.toLowerCase)) range.length else 1)
  }

//  def getAxisType( cfName: String ):  DomainAxis.Type.Value = targetGridOpt.flatMap( _.grid.getAxisSpec(cfName).map( _.getAxisType ) )

  def getAxes: List[DomainAxis] = roi.getRanges.map( (range: ma2.Range) => new DomainAxis( DomainAxis.fromCFAxisName(range.getName), range.first, range.last,  "indices") ).toList

  def getKey: DataFragmentKey = {
    new DataFragmentKey(varname, collection.id, roi.getOrigin, roi.getShape )
  }
  def getSize: Long = roi.getShape.foldLeft(1L)(_ * _)

  def getKeyString: String = getKey.toString

  def domainSpec: DataFragmentSpec = domainSectOpt match {
    case None => this;
    case Some(cutSection) =>
      new DataFragmentSpec(uid,
                           varname,
                           collection,
                           fragIdOpt,
                           targetGridOpt,
                           dimensions,
                           units,
                           longname,
                           roi.intersect(cutSection),
                           domainSectOpt,
                           _metadata,
                           missing_value, numDataFiles,
                           mask, autoCache )
  }

  def intersectRoi(cutSection: ma2.Section): ma2.Section = {
    val base_sect = roi;
    val raw_intersection = base_sect.intersect(cutSection)
    val ranges = for (ir <- raw_intersection.getRanges.indices;
                      r0 = raw_intersection.getRange(ir);
                      r1 = base_sect.getRange(ir))
      yield new ma2.Range(r1.getName, r0)
    new ma2.Section(ranges)
  }
  def combineRoi(
      otherSection: ma2.Section): (ma2.Section, SectionMerge.Status) = {
    logger.info(
      "\n\nCombine SECTIONS: %s - %s \n\n".format(_section.toString,
                                                  otherSection.toString))
    var sectionMerge: SectionMerge.Status = SectionMerge.Overlap
    val new_ranges: IndexedSeq[ma2.Range] =
      for (iR <- _section.getRanges.indices; r0 = _section.getRange(iR);
           r1 = otherSection.getRange(iR)) yield {
        if (r0 == r1) { r0 } else if ((r0.last + 1) == (r1.first)) {
          assert(sectionMerge == SectionMerge.Overlap,
                 SectionMerge.incommensurate(_section, otherSection))
          sectionMerge = SectionMerge.Append;
          new ma2.Range(r0.first, r1.last, r0.stride)
        } else if ((r1.last + 1) == (r0.first)) {
          assert(sectionMerge == SectionMerge.Overlap,
                 SectionMerge.incommensurate(_section, otherSection))
          sectionMerge = SectionMerge.Prepend;
          new ma2.Range(r1.first, r0.last, r0.stride)
        } else
          throw new Exception(
            SectionMerge.incommensurate(_section, otherSection))
      }
    (new ma2.Section(new_ranges: _*) -> sectionMerge)
  }

  def cutIntersection(cutSection: ma2.Section): Option[DataFragmentSpec] = {
    val mySection = roi
    if (mySection.intersects(cutSection)) {
      val intersection = intersectRoi(cutSection)
      //      logger.info( "DOMAIN INTERSECTION:  %s <-> %s  => %s".format( roi.toString, cutSection.toString, intersection.toString ))
      Some(
        new DataFragmentSpec(uid,
                             varname,
                             collection,
                             fragIdOpt,
                             targetGridOpt,
                             dimensions,
                             units,
                             longname,
                             intersection,
                             domainSectOpt,
                             _metadata,
                             missing_value, numDataFiles,
                             mask, autoCache ))
    } else None
  }

  def getReducedSection(axisIndices: Set[Int], newsize: Int = 1): ma2.Section = {
    new ma2.Section(
      roi.getRanges.zipWithIndex.map(rngIndx =>
        if (axisIndices(rngIndx._2)) collapse(rngIndx._1, newsize)
        else rngIndx._1): _*)
  }

  def getOffsetSection(axisIndex: Int, offset: Int): ma2.Section = {
    new ma2.Section(
      roi.getRanges.zipWithIndex.map(
        rngIndx =>
          if (axisIndex == rngIndx._2)
            new ma2.Range(rngIndx._1.first() + offset,
                          rngIndx._1.last() + offset)
          else rngIndx._1): _*)
  }

  def reduce(axisIndices: Set[Int], newsize: Int = 1): DataFragmentSpec =
    reSection(getReducedSection(axisIndices, newsize))

  def getIntersection(subsection: ma2.Section): ma2.Section = {
    subsection.intersect(_section)
  }
  def intersects(subsection: ma2.Section): Boolean = {
    subsection.intersects(_section)
  }

  def getSubSection(subsection: ma2.Section): ma2.Section = {
    new ma2.Section(roi.getRanges.zipWithIndex.map(rngIndx => {
      val ss = subsection.getRange(rngIndx._2)
      rngIndx._1.compose(ss)
    }))

  }

  def getVariableMetadata(serverContext: ServerContext): Map[String, nc2.Attribute] = {
    var v: CDSVariable = serverContext.getVariable(collection, varname)
    v.attributes ++ Map(
      "description" -> new nc2.Attribute("description", v.description),
      "units" -> new nc2.Attribute("units", v.units),
      "fullname" -> new nc2.Attribute("fullname", v.fullname),
      "axes" -> new nc2.Attribute("axes", dimensions),
      "varname" -> new nc2.Attribute("varname", varname),
      "collection" -> new nc2.Attribute("collection", collection.id)
    )
  }

  def getDatasetMetadata(serverContext: ServerContext): List[nc2.Attribute] =
    collection.getDatasetMetadata()
  def getCollection: Collection = collection

  def reduceSection(dimensions: Int*): DataFragmentSpec = {
    var newSection = roi;
    for (dim: Int <- dimensions) {
      newSection = newSection.setRange(dim, new ma2.Range(0, 0, 1))
    }
    reSection(newSection)
  }

  def append(dfSpec: DataFragmentSpec, dimIndex: Int = 0): DataFragmentSpec = {
    val combinedRange =
      roi.getRange(dimIndex).union(dfSpec.roi.getRange(dimIndex))
    val newSection: ma2.Section = roi.replaceRange(dimIndex, combinedRange)
    reSection(newSection)
  }

  def reSection(newSection: ma2.Section): DataFragmentSpec = {
//    println( " ++++ ReSection: newSection=(%s), roi=(%s)".format( newSection.toString, roi.toString ) )
    val newRanges = for (iR <- roi.getRanges.indices; r0 = roi.getRange(iR);
                         rNew = newSection.getRange(iR))
      yield new ma2.Range(r0.getName, rNew)
    new DataFragmentSpec(uid,
                         varname,
                         collection,
                         fragIdOpt,
                         targetGridOpt,
                         dimensions,
                         units,
                         longname,
                         new ma2.Section(newRanges),
                         domainSectOpt,
                          _metadata,
                         missing_value, numDataFiles,
                         mask, autoCache )
  }
  def reSection(fkey: DataFragmentKey): DataFragmentSpec =
    reSection(fkey.getRoi)
//  private var dataFrag: Option[PartitionedFragment] = None
  //  def setData( fragment: PartitionedFragment ) = { assert( dataFrag == None, "Overwriting Data Fragment in " + toString ); dataFrag = Option(fragment) }
  //  def getData: Option[PartitionedFragment] = dataFrag
}

//object OperationSpecs {
//  def apply(op: OperationContext) =
//    new OperationSpecs( op.identifier, op.name, op.getConfiguration )
//}
//class OperationSpecs( val id: String, name: String, val optargs: Map[String, String]) {
//  val ids = mutable.HashSet(name)
//  def ==(oSpec: OperationSpecs) = (optargs == oSpec.optargs)
//  def merge(oSpec: OperationSpecs) = { ids ++= oSpec.ids }
//  def getSpec(id: String, default: String = ""): String =
//    optargs.getOrElse(id, default)
// }

class DataContainer(val uid: String, private val source: Option[DataSource] = None, private val operation: Option[OperationContext] = None)  extends ContainerBase {
  assert(source.isDefined || operation.isDefined, s"Empty DataContainer: variable uid = $uid")
  assert(source.isEmpty || operation.isEmpty, s"Conflicted DataContainer: variable uid = $uid")
//  private val optSpecs = mutable.ListBuffer[OperationSpecs]()
  private lazy val variable = {
    val source = getSource; source.collection.getVariable(source.name)
  }
  def getInputDomain: Option[String] = source.flatMap( _.getDomain )

/*  def getDomains: List[String] = _domains.toList
  private val _domains = new scala.collection.mutable.HashSet[String]()
  private def _addDomain( domain_id: String ): Boolean = if( _domains.contains(domain_id) ) { false; } else { _domains += domain_id; true }
  def addOperationDomains(operation_map: Map[String,OperationContext]): Boolean = {
    var rv = false
    val operations = optSpecs.flatMap( op => operation_map.get(op.ide ) )
    rv
  }*/

  def toWPSDataInput: WPSDataInput = WPSDataInput(uid.toString, 1, 1)
  def getVariable: CDSVariable = variable
  def updateDomainsFromOperation( operation: OperationContext ): Boolean = source.exists( _.updateDomainsFromOperation(operation) )

  override def toString = {
    val embedded_val: String =
      if (source.isDefined) source.get.toString else operation.get.toString
    s"DataContainer ( $uid ) { \n\t\t\t$embedded_val }"
  }
  override def toXml = {
    val embedded_xml =
      if (source.isDefined) source.get.toXml else operation.get.toXml
    <dataset uid={uid}> embedded_xml </dataset>
  }
  def isSource: Boolean = source.isDefined

  def isOperation: Boolean = operation.isDefined

  def getSource: DataSource = {
    assert( isSource, s"Attempt to access an operation based DataContainer($uid) as a data source" )
    source.get
  }
  def getOperation: OperationContext = {
    assert( isOperation, s"Attempt to access a source based DataContainer($uid) as an operation")
    operation.get
  }
  def getSourceOpt = { source }

//  def addOpSpec(operation: OperationContext): Unit = { // used to inform data container what types of ops will be performed on it.
//    def mergeOpSpec(oSpecList: mutable.ListBuffer[OperationSpecs], oSpec: OperationSpecs): Unit = oSpecList.headOption match {
//      case None => oSpecList += oSpec
//      case Some(head) =>
//        if (head == oSpec) head merge oSpec
//        else mergeOpSpec(oSpecList.tail, oSpec)
//    }
//    mergeOpSpec(optSpecs, OperationSpecs(operation))
//  }
//  def getOpSpecs: List[OperationSpecs] = optSpecs.toList
}

object DataContainer extends ContainerBase {
  private val random = new Random(System.currentTimeMillis)

  def apply(operation: OperationContext): DataContainer = {
    new DataContainer(uid = operation.rid, operation = Some(operation))
  }
  def absPath(path: String): String =
    new java.io.File(path).getAbsolutePath.toLowerCase

  def vid(varId: String, uid: Boolean) = {
    val idItems = varId.split(Array(':', '|'))
    if (uid) { if (idItems.length < 2) idItems.head else idItems(1) } else
      idItems.head
  }

  def getCollection( metadata: Map[String, Any]): (Option[Collection], Option[String] ) = {
    val uri = metadata.getOrElse("uri", "").toString
    val varsList: List[String] =
      if (metadata.keySet.contains("id")) metadata.getOrElse("id", "").toString.split(",").map(item => stripQuotes(vid(item, false))).toList
      else metadata.getOrElse("name", "").toString.split(",").map(item => stripQuotes(vid(item, false))).toList
    val path = metadata.getOrElse("path", "").toString
    val collection = metadata.getOrElse("collection", "").toString
    val title = metadata.getOrElse("title", "").toString
    val fileFilter = metadata.getOrElse("fileFilter", "").toString
    val id = parseUri(uri)
    logger.info(s" >>>>>>>>>>>----> getCollection, uri=$uri, id=$id")
    val colId = if (!collection.isEmpty) { collection } else
      uri match {
        case colUri if (colUri.startsWith("collection")) => id
        case fragUri if (fragUri.startsWith("fragment")) => vid(id, true)
        case x => ""
      }
    val fragIdOpt = if (uri.startsWith("fragment")) Some(id) else None
    logger.info( s"Looking for collection ${colId}, available: [ ${Collections.idSet.mkString(", ")} ]")
    Collections.findCollection(colId) match {
      case Some(collection) =>  (Some(collection), fragIdOpt)
      case None =>
        if( CollectionLoadServices.loadCollection( colId ) ) {
          Collections.findCollection(colId) match {
            case Some(collection) =>  (Some(collection), fragIdOpt)
            case None => (None, fragIdOpt)
          }
        } else { (None, fragIdOpt) }
    }
  }

  def validateInputMethods( metadata: Map[String, Any] ): Unit = {
    val allowed_input_methods = appParameters("inputs.methods.allowed","collection").split(',').map( _.toLowerCase.trim )
    val uri = metadata.getOrElse("uri", throw new Exception("Missing required parameter 'uri' in variable spec") ).toString
    val input_method = uri.split(':').head.toLowerCase
    if( !allowed_input_methods.contains( input_method ) ) { throw new Exception( s"Input method '$input_method' is not permitted, allowed methods = ${allowed_input_methods.mkString("[ ",", "," ]")} ")}
  }

  def factory(uid: UID, metadata: Map[String, Any], noOp: Boolean ): Array[DataContainer] = {
    validateInputMethods( metadata )
    val fullname =
      if (metadata.keySet.contains("id")) metadata.getOrElse("id", "").toString
      else metadata.getOrElse("name", "").toString
    val domain = metadata.getOrElse("domain", "").toString
    val (collectionOpt, fragIdOpt) = getCollection(metadata)
    val base_index = random.nextInt(Integer.MAX_VALUE)
    val autocache = metadata.getOrElse( "cache", false ).toString.toBoolean
    collectionOpt match {
      case None =>
        val var_names: Array[String] = fullname.toString.split(',')
        val dataPath = metadata.getOrElse("uri", metadata.getOrElse("url", uid)).toString
        val cid = dataPath.split('/').last
        val collection = Collections.addCollection( cid ) getOrElse {
          throw new Exception(s"Attempt to acess a non existent collection '$cid', collections = ${Collections.getCollections.mkString(", ")}")
        }
        for ((name, index) <- var_names.zipWithIndex) yield {
          val name_items = name.split(Array(':', '|'))
          val dsource = new DataSource( stripQuotes(name_items.head), collection, normalizeOpt(domain), autocache )
          val vid = stripQuotes(name_items.last)
          val vname = normalize(name_items.head)
          val dcid =
            if (vid.isEmpty) uid + s"c-$base_index$index"
            else if (vname.isEmpty) vid
            else uid + vid
          new DataContainer(dcid, source = Some(dsource))
        }
      case Some(collection) =>
        val var_names: Array[String] =
          if (fullname.equals("*")) collection.varNames.toArray
          else fullname.toString.split(',')
        fragIdOpt match {
          case Some(fragId) =>
            val name_items = var_names.head.split(Array(':', '|'))
            val dsource = new DataSource(stripQuotes(name_items.head), collection, normalizeOpt(domain), autocache, fragIdOpt)
            val vid = normalize(name_items.last)
            Array(
              new DataContainer(if (vid.isEmpty) uid + s"c-$base_index"
                                else uid + vid,
                                source = Some(dsource)))
          case None =>
            for ((name, index) <- var_names.zipWithIndex) yield {
              val name_items = name.split(Array(':', '|'))
              val dsource = new DataSource(stripQuotes(name_items.head), collection, normalizeOpt(domain), autocache )
              val vid = stripQuotes(name_items.last)
              val vname = normalize(name_items.head)
              val dcid =
                if (vid.isEmpty) uid + s"c-$base_index$index"
                else if (vname.isEmpty) vid
                else uid + vid
              new DataContainer(dcid, source = Some(dsource))
            }
        }
    }
  }

  def parseUri(uri: String): String = {
    if (uri.isEmpty) ""
    else {
      val recognizedUrlTypes = List("file", "collection", "fragment")
      val uri_parts = uri.split(":")
      val url_type = normalize(uri_parts.head)
      if ( url_type.startsWith("http") ) {
        uri
      } else {
        if (recognizedUrlTypes.contains(url_type)) {
          val value = uri_parts.last.toLowerCase
          if (List("collection", "fragment").contains(url_type))
            value.stripPrefix("/").stripPrefix("/")
          else value
        } else
          throw new Exception(
            "Unrecognized uri format: " + uri + ", type = " + uri_parts.head + ", nparts = " + uri_parts.length.toString + ", value = " + uri_parts.last)
      }
    }
  }
}

class DomainContainer(val name: String,
                      val axes: List[DomainAxis] = List.empty[DomainAxis],
                      _metadata: Map[String, String] = Map.empty,
                      val mask: Option[String] = None) extends ContainerBase with Serializable {
  override def toString = {
    s"DomainContainer { name = $name, axes = ${axes.mkString("\n\t\t\t","\n\t\t\t","\n\t\t\t")} }"
  }
  val metadata = extractMetadata( _metadata, axes )
  def toDataInput: Map[String, Any] =
    Map(axes.map(_.toDataInput): _*) ++ Map(("name" -> name))

  override def toXml = {
    <domain name={name}>
      <axes> { axes.map( _.toXml ) } </axes>
      { mask match { case None => Unit; case Some(maskId) => <mask> { maskId } </mask> } }
    </domain>
  }
  def extractMetadata(base_metadata: Map[String, String], axes: List[DomainAxis] ): Map[String, String] = {
    val axis_metadata = axes.filter( ! _.filter.isEmpty ).map( axis => "filter:" + axis.getCFAxisName -> axis.filter )
    base_metadata ++ axis_metadata
  }
}

object DomainAxis extends ContainerBase {
  object Type extends Enumeration { val X, Y, Z, T = Value }
  def fromCFAxisName(cfName: String): Type.Value =
    cfName.toLowerCase match {
      case "x" => Type.X; case "y" => Type.Y; case "z" => Type.Z;
      case "t" => Type.T;
    }

  def coordAxisName(axistype: DomainAxis.Type.Value): String = {
    import DomainAxis.Type._
    axistype match {
      case X => "lon"; case Y => "lat"; case Z => "level"; case T => "time"
    }
  }

  def apply(axistype: Type.Value, start: Int, end: Int): Option[DomainAxis] = {
    Some(new DomainAxis(axistype, start, end, "indices"))
  }

  def apply(axistype: Type.Value, axis_spec: Option[Any]): Option[DomainAxis] = {
    axis_spec match {
      case Some(generic_axis_map: Map[_, _]) =>
        val axis_map = getStringKeyMap(generic_axis_map)
        val start = getGenericNumber(axis_map.get("start"))
        val end = getGenericNumber(axis_map.get("end"))
        val filter: String = axis_map.getOrElse("filter","").toString
        val system = axis_map.getOrElse("system", axis_map.getOrElse("crs", "values") ).toString
        val bounds = getStringValue(axis_map.get("bounds"))
        Some( new DomainAxis(axistype, start,  end,  normalize(system),  normalize(bounds),  filter))
      case Some(sval: String) =>
        val gval = getGenericNumber(Some(sval))
        Some(new DomainAxis(axistype, gval, gval, "values"))
      case None => None
      case _ =>
        val msg = "Unrecognized DomainAxis spec: " + axis_spec.toString
        logger.error(msg)
        throw new Exception(msg)
    }
  }
}

class DomainAxis(val axistype: DomainAxis.Type.Value,
                 val start: GenericNumber,
                 val end: GenericNumber,
                 val system: String,
                 val bounds: String = "",
                 val filter: String = "") extends ContainerBase  with Serializable {
  import DomainAxis.Type._
  val name = axistype.toString
  def getCFAxisName: String = axistype match {
    case X => "X"; case Y => "Y"; case Z => "Z"; case T => "T"
  }
  def getCoordAxisName: String = DomainAxis.coordAxisName(axistype)
  override def toString = s"DomainAxis { name = $name, start = $start, end = $end, system = $system, bounds = $bounds }"
  def toBoundsString = s"$name:[$start,$end,$system]"
  def toDataInput: (String, Map[String, String]) = getCoordAxisName -> Map("start" -> start.toString, "end" -> end.toString, "system" -> system)

  override def toXml = {
    <axis name={name} start={start.toString} end={end.toString} system={system} bounds={bounds} />
  }

  def matches(axisType: nc2.constants.AxisType): Boolean = {
    import nc2.constants.AxisType, DomainAxis.Type._
    axistype match {
      case X =>
        List(AxisType.Lon, AxisType.GeoX, AxisType.RadialDistance)
          .contains(axisType)
      case Y =>
        List(AxisType.Lat, AxisType.GeoY, AxisType.RadialAzimuth)
          .contains(axisType)
      case Z =>
        List(AxisType.Pressure, AxisType.Height, AxisType.RadialElevation)
          .contains(axisType)
      case T => (AxisType.Time == axisType)
    }
  }
}

object DomainContainer extends ContainerBase {

  def refKey(item: (String, Any)) =
    !(key_equals(wpsNameMatchers.xAxis)(item) || key_equals(
      wpsNameMatchers.yAxis)(item)
      || key_equals(wpsNameMatchers.zAxis)(item) || key_equals(
      wpsNameMatchers.tAxis)(item) || key_equals(wpsNameMatchers.id)(item))

  def apply(metadata: Map[String, Any]): DomainContainer = {
    var items = new ListBuffer[Option[DomainAxis]]()
    try {
      val name =
        if (metadata.keySet.contains("id")) metadata.getOrElse("id", "")
        else metadata.getOrElse("name", "")
      items += DomainAxis(
        DomainAxis.Type.Y,
        filterMap(metadata, key_equals(wpsNameMatchers.yAxis)))
      items += DomainAxis(
        DomainAxis.Type.X,
        filterMap(metadata, key_equals(wpsNameMatchers.xAxis)))
      items += DomainAxis(
        DomainAxis.Type.Z,
        filterMap(metadata, key_equals(wpsNameMatchers.zAxis)))
      items += DomainAxis(
        DomainAxis.Type.T,
        filterMap(metadata, key_equals(wpsNameMatchers.tAxis)))
      val mask: Option[String] =
        filterMap(metadata, key_equals("mask")) match {
          case None => None; case Some(x) => Some(x.toString)
        }
      new DomainContainer(
        normalize(name.toString),
        items.flatten.toList,
        metadata.filter(refKey).mapValues(_.toString).map(identity),
        mask)
    } catch {
      case e: Exception =>
        logger.error("Error creating DomainContainer: " + e.getMessage)
        logger.error(e.getStackTrace.mkString("\n"))
        throw new Exception(e.getMessage, e)
    }
  }

  def empty(name: String): DomainContainer = new DomainContainer(name)
}

object OperationContext extends ContainerBase {
  private var resultIndex = 0;
  def getRid (resultId: String) (inputId: String): String = inputId.split('-').head + "-" + resultId

  def apply( uid: UID, process_name: String, uid_list: List[String], metadata: Map[String, Any] ): OperationContext = {
    val op_inputs: Iterable[String] = metadata.get("input") match {
      case Some(input_values: List[_]) =>   input_values.map( input_value => { val sval = input_value.asInstanceOf[String]; if( sval.endsWith( uid.toString ) ) { sval } else { uid + sval.trim } } )
      case Some(input_value: String) =>     if( input_value.isEmpty ) { uid_list } else { input_value.split(',').map(uid + _.trim) }
      case None =>                          uid_list.map(uid + _.trim)
      case x => throw new Exception("Unrecognized input in operation spec: " + x.toString)
    }
    val op_name = metadata.getOrElse("name", process_name).toString.trim

    val optargs: Map[String, String] = metadata.filterNot((item) => List("input", "name").contains(item._1)).mapValues(_.toString.trim).map(identity) // map(identity) to work around scala serialization bug
    val input = metadata.getOrElse("input", "").toString
    val opLongName = op_name + "-" + (List(input) ++ optargs.toList.map(item => item._1 + "=" + item._2)).filterNot((item) => item.isEmpty).mkString("(", "_", ")")
    val dt: DateTime = new DateTime(DateTimeZone.getDefault())
    val rid = metadata.get("id") match {
      case Some(result_id) =>
        uid + result_id.toString
      case None =>
        metadata.get("result") match {
          case Some(result_id) =>
            uid + result_id.toString
          case None =>
            resultIndex += 1
            uid + op_name + "-" + resultIndex.toString
        }
    }
    new OperationContext( identifier = UID() + op_name, name = op_name, rid = rid, inputs = op_inputs.toList, optargs )
  }
}

class OperationContext(val identifier: String,
                       val name: String,
                       val rid: String,
                       val inputs: List[String],
                       private val configuration: Map[String, String]) extends ContainerBase with ScopeContext with Serializable {
  import OperationContext._
  def getConfiguration: Map[String, String] = configuration
  def getConfParm( key: String ): Option[String] = configuration.get(key)
  private val _domains = new scala.collection.mutable.HashSet[String]()
  configuration.get("domain").foreach( _addDomain )
  val moduleName: String = name.toLowerCase.split('.').head
  override def toString =  s"OperationContext { id = $identifier,  name = $name, rid = $rid, inputs = $inputs, configurations = $configuration }"
  override def toXml = <proc id={identifier} name={name} rid={rid} inputs={inputs.toString} configurations={configuration.toString}/>
  def operatesOnAxis( axis: Char ): Boolean = configuration.getOrElse("axes","").contains(axis)
  def reconfigure( new_configuration: Map[String, String] ): OperationContext = new OperationContext( identifier, name, rid, inputs, new_configuration )
  def getDomains: List[String] = _domains.toList
  def outputs: List[String] = inputs map ( inputId =>  getRid(rid)( inputId ) )
  def output( inputId: String ): String = getRid(rid)( inputId )
  def getRenameOp: (String)=>String = getRid(rid)

  private def _addDomain( domain_id: String ): Boolean = if( _domains.contains(domain_id) ) { false; } else { _domains += domain_id; true }

  def addInputDomains( variableMap: Map[String, DataContainer] ): Boolean = if( _domains.size > 0 ) { false } else {
    inputs.exists( variableMap.get(_).exists( addDomainsForInput ) )
  }

  def addDomainsForInput( dc: DataContainer): Boolean = dc.getInputDomain.exists( _addDomain )
}

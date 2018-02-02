package nasa.nccs.edas.sources

import java.io._
import java.net.URI
import java.nio.file.{FileSystems, Path, Paths}
import java.util.Formatter
import java.util.concurrent.{Executors, Future, TimeUnit}

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cdapi.tensors.CDDoubleArray
import nasa.nccs.edas.sources.netcdf.{NCMLWriter, NetcdfDatasetMgr}
import nasa.nccs.utilities._
import org.apache.commons.lang.RandomStringUtils
import ucar.nc2.Group
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset._
import ucar.nc2.time.CalendarDate

import scala.collection.mutable
import collection.mutable.{HashMap, ListBuffer}
import collection.JavaConversions._
import collection.JavaConversions._
import scala.io.Source
import scala.util.matching.Regex
import scala.xml.Utility



//class NCMLSerialWriter(val args: Iterator[String]) {
//  val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
//  val nFiles = files.length
//  val fileHeaders = NCMLWriter.getFileHeadersSerial(files)
//
//  def getNCML: xml.Node = {
//    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
//      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
//      <aggregation dimName="time" units={EDTime.units} type="joinExisting">
//        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
//      </aggregation>
//    </netcdf>
//  }
//}



class DatasetFileHeaders(val aggDim: String, val aggFileMap: Seq[FileHeader]) {
  def getNElems: Int = {
    assert( aggFileMap.nonEmpty, "Error, aggregated dataset has no files!")
    aggFileMap.head.nElem
  }
  def getAggAxisValues: Array[Double] =
    aggFileMap.foldLeft(Array[Double]()) { _ ++ _.axisValues }
}


object FileHeader extends Loggable {
  val maxOpenAttempts = 1
  val retryIntervalSecs = 10
  private val _instanceCache = new ConcurrentLinkedHashMap.Builder[String, FileHeader].initialCapacity(64).maximumWeightedCapacity(100000).build()
  private val _pool = Executors.newFixedThreadPool( Runtime.getRuntime.availableProcessors )

  def apply( uri: URI, timeRegular: Boolean ): FileHeader = apply( uri.toString, timeRegular )
  def apply( file: File, timeRegular: Boolean ): FileHeader = apply( file.getCanonicalPath, timeRegular )

  def apply( filePath: String, timeRegular: Boolean ): FileHeader = _instanceCache.getOrElse( filePath, {
    val ncDataset: NetcdfDataset =  NetcdfDatasetMgr.aquireFile(filePath, 2.toString)
    try {
      val (axisValues, boundsValues) = FileHeader.getTimeCoordValues(ncDataset)
      val (variables, coordVars): (List[nc2.Variable], List[nc2.Variable]) = FileMetadata.getVariableLists(ncDataset)
      val fileHeader = new FileHeader(filePath, axisValues, boundsValues, timeRegular, variables map { _.getShortName }, coordVars map { _.getShortName } )
      _instanceCache.put( filePath, fileHeader )
      fileHeader
    } finally {
      ncDataset.close()
    }
  })

  def term() = {
    _pool.shutdown()
    _pool.awaitTermination(60,TimeUnit.SECONDS)
  }

  def isCached( path: String ): Boolean = _instanceCache.keys.contains( path )

  def clearCache = _instanceCache.clear()
  def filterCompleted( seq: IndexedSeq[Future[_]] ): IndexedSeq[Future[_]] = seq.filterNot( _.isDone )

  def waitUntilDone( seq: IndexedSeq[Future[_]] ): Unit = if( seq.isEmpty ) { return } else {
    //    print( s"Waiting on ${seq.length} tasks (generating NCML files)")
    Thread.sleep( 500 )
    waitUntilDone( filterCompleted(seq) )
  }

  def factory(files: IndexedSeq[String], timeRegular: Boolean = false ):Unit = {
    val futures: IndexedSeq[Future[_]] = files.filter { file => !isCached(file) } map { file => _pool.submit( new FileHeaderGenerator(file,timeRegular) ) }
    waitUntilDone( futures )
  }

  def getFileHeaders(files: IndexedSeq[String], timeRegular: Boolean = false ): IndexedSeq[FileHeader] = {
    factory( files, timeRegular )
    files.map( file => FileHeader( file, timeRegular ) ).sortBy(_.startDate)
  }

  def getNumCommonElements( elemList: IndexedSeq[Array[String]] ): Int = {
    if( elemList.length > 1 ) { elemList.indices.foreach { elemIndex => if (elemList.map(array => array(elemIndex)).toSet.size > 1) return elemIndex } }
    elemList.head.length - 1
  }

  def extractSubpath( headers: IndexedSeq[FileHeader] ): ( String, IndexedSeq[FileHeader] ) = {
    val elemList: IndexedSeq[Array[String]] = headers.map( header => header.filePath.split('/'))
    val nCommon = getNumCommonElements(elemList)
    ( elemList.head.take(nCommon).mkString("/"), headers.map( _.dropPrefix(nCommon) ) )
  }

  def getTimeAxisRegularity(ncFile: URI): Boolean = {
    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.aquireFile(ncFile.toString, 3.toString)
    try {
      Option(ncDataset.findCoordinateAxis(AxisType.Time)) match {
        case Some(coordAxis) =>
          coordAxis match {
            case coordAxis: CoordinateAxis1D => coordAxis.isRegular
            case _ => throw new Exception("Time axis of this type not currently supported: " + coordAxis.getClass.getName)
          }
        case None =>
          throw new Exception("ncFile does not have a time axis: " + ncFile)
      }
    } finally {
      ncDataset.close()
    }
  }

  def getTimeValues(ncDataset: NetcdfDataset, coordAxis: VariableDS, start_index: Int = 0, end_index: Int = -1, stride: Int = 1): ( Array[Double], Array[Array[Double]] ) = {
    val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(ncDataset, coordAxis, new Formatter())
    val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
    val bounds: Array[Array[Double]] = ((0 until timeAxis.getShape(0)) map (index => timeAxis.getCoordBoundsDate(index) map ( EDTime.toValue ))).toArray
    val timeValues = timeCalValues.map( EDTime.toValue ).toArray
    val datesSample = timeCalValues.subList(0,5)
    val timeValuesSample = timeValues.slice(0,5)
    logger.info( s" Writing Time values, dates: [ ${datesSample.map(_.toString).mkString(", ")} ], ${EDTime.units}: [ ${timeValuesSample.map(_.toString).mkString(", ")} ] ")
    ( timeValues, bounds )
  }


  def getTimeCoordValues(ncDataset: NetcdfDataset): ( Array[Double], Array[Array[Double]] ) = {
    val result = Option(ncDataset.findCoordinateAxis(AxisType.Time)) match {
      case Some(timeAxis) => getTimeValues(ncDataset, timeAxis)
      case None => throw new Exception( "ncDataset does not have a time axis: " + ncDataset.getReferencedFile.getLocation )
    }
    result
  }
}

class FileHeader(val filePath: String,
                 val axisValues: Array[Double],
                 val boundsValues: Array[Array[Double]],
                 val timeRegular: Boolean,
                 val varNames: List[String],
                 val coordVarNames: List[String]
                ) {
  def nElem: Int = axisValues.length
  def startValue: Double = boundsValues.head(0)
  def endValue: Double = boundsValues.last(1)
  def dt = ( endValue + 1 - startValue ) / boundsValues.length
  def startDate: String = EDTime.toDate(startValue).toString
  override def toString: String = " *** FileHeader { path='%s', nElem=%d, startValue=%d startDate=%s} ".format(filePath, nElem, startValue, startDate)
  def dropPrefix( nElems: Int ): FileHeader = new FileHeader( filePath.split("/").drop(nElems).mkString("/"), axisValues, boundsValues, timeRegular, varNames, coordVarNames )
}





object FileMetadata extends Loggable {
  def apply(file: String): FileMetadata = {
    val dataset  = NetcdfDatasetMgr.aquireFile(file.toString, 4.toString)
    new FileMetadata(dataset)
  }
  def getVariableLists(ncDataset: NetcdfDataset): ( List[nc2.Variable], List[nc2.Variable] ) = {
    val all_vars = ncDataset.getVariables
    val all_vars_grouped = all_vars groupBy { _.isCoordinateVariable }
    val variables: List[nc2.Variable] = all_vars_grouped.getOrElse( false, List.empty ).toList
    val coord_variables: List[nc2.Variable] = all_vars_grouped.getOrElse( true, List.empty ).toList
    val bounds_vars: List[String] = ( all_vars flatMap { v => Option( v.findAttributeIgnoreCase("bounds") ) }  map { _.getStringValue } ).toList
    val data_variables = variables filterNot { v => bounds_vars.contains(v.getShortName) }
    ( data_variables, coord_variables )
  }
}

class FileMetadata(val ncDataset: NetcdfDataset) {
  import FileMetadata._
  val coordinateAxes: List[CoordinateAxis] = ncDataset.getCoordinateAxes.toList
  val dimensions: List[nc2.Dimension] = ncDataset.getDimensions.toList
  val (variables, coordVars): (List[nc2.Variable], List[nc2.Variable] ) = getVariableLists(ncDataset)
  val attributes: List[nc2.Attribute] = ncDataset.getGlobalAttributes.toList
  val dimNames: List[String] = dimensions.map(AggregationWriter.getName(_))
  def close = ncDataset.close()
  def getCoordinateAxis(name: String): Option[nc2.dataset.CoordinateAxis] = coordinateAxes.find(p => AggregationWriter.getName(p).equalsIgnoreCase(name))

  def getAxisType(variable: nc2.Variable): AxisType = variable match {
    case coordVar: CoordinateAxis1D => coordVar.getAxisType;
    case _ => AxisType.RunTime
  }


}

//

object CDScan extends Loggable {
  val usage = """
    Usage: mkcoll [-d {collectionBifurcationDepth: Int)}] [-t {collectionNameTemplate: RegExp}] <collectionID> <datPath>
  """

  def main(args: Array[String]) {
    if( args.length < 1 ) { println( usage ); return }
    var optionMap = mutable.HashMap.empty[String, String]
    var inputs = mutable.ListBuffer.empty[String]
    EDASLogManager.isMaster
    val argIter = args.iterator
    while( argIter.hasNext ) {
      val arg = argIter.next
      if(arg(0) == '-') arg match {
        case "-d" => optionMap += (( "depth", argIter.next ))
        case "-t" => optionMap += (( "template", argIter.next ))
        case x => throw new Exception( "Unrecognized option: " + x )
      } else { inputs += arg }
    }
    if( inputs.length < 2 ) { throw new Exception( "Missing input(s): " + usage ) }
    val collectionId = inputs(0).toLowerCase
    val pathFile = new File(inputs(1))
    AggregationWriter.extractAggregations( collectionId, pathFile.toPath, optionMap.toMap )
    FileHeader.term()
  }
}

object CDMultiScan extends Loggable {
  def main(args: Array[String]) {
    if( args.length < 1 ) { println( "Usage: 'mkcolls <collectionsMetaFile>'"); return }
    EDASLogManager.isMaster
    val collectionsMetaFile = new File(args(0))    // cols:  depth, template, collectionID, collectionRootPath
    if( !collectionsMetaFile.isFile ) { throw new Exception("Collections file does not exits: " + collectionsMetaFile.toString) }
    val ncmlDir = Collections.getAggregationPath.toFile
    ncmlDir.mkdirs
    AggregationWriter.generateAggregations( collectionsMetaFile )
    FileHeader.term()
  }
}

class FileHeaderGenerator(file: String, timeRegular: Boolean ) extends Runnable {
  override def run(): Unit = { FileHeader( file, timeRegular ) }
}



//object LegacyCDScan extends Loggable {
//  val usage = """
//    Usage: mkcoll [-m] <collectionID> <datPath>
//        -m: Process each subdirectory of <datPath> as a separate collection
//  """
//
//  def main(args: Array[String]) {
//    if( args.length < 1 ) { println( usage ); return }
//    var optionMap = mutable.HashMap.empty[String, String]
//    var inputs = mutable.ListBuffer.empty[String]
//    EDASLogManager.isMaster
//    for( arg <- args ) if(arg(0) == '-') arg match {
//      case "-m" => optionMap += (( "multi", "true" ))
//      case x => throw new Exception( "Unrecognized option: " + x )
//    } else { inputs += arg }
//    if( inputs.length < 2 ) { throw new Exception( "Missing input(s): " + usage ) }
//
//    val collectionId = inputs(0).toLowerCase
//    val subCollectionId = collectionId + "-sub"
//    val pathFile = new File(inputs(1))
//    val ncmlFile = AggregationWriter.getCachePath("NCML").resolve(subCollectionId + ".ncml").toFile
//    if ( ncmlFile.exists ) { throw new Exception("Collection already exists, defined by: " + ncmlFile.toString) }
//    logger.info(s"Creating NCML file for collection ${collectionId} from path ${pathFile.toString}")
//    ncmlFile.getParentFile.mkdirs
//    val AggregationWriter = AggregationWriter(pathFile)
//    val variableMap = new collection.mutable.HashMap[String,String]()
//    val varNames: List[String] = AggregationWriter.writeNCML(ncmlFile)
//    varNames.foreach( vname => variableMap += ( vname -> subCollectionId ) )
//    writeCollectionDirectory( collectionId, variableMap.toMap )
//  }
//}

//object CDMultiScanLegacy extends Loggable {
//  def main(args: Array[String]) {
//    if( args.length < 1 ) { println( "Usage: 'mkcolls <collectionsMetaFile>'"); return }
//    EDASLogManager.isMaster
//    val collectionsMetaFile = new File(args(0))    // If first col == 'mult' then each subdir is treated as a separate collection.
//    if( !collectionsMetaFile.isFile ) { throw new Exception("Collections file does not exits: " + collectionsMetaFile.toString) }
//    val ncmlDir = AggregationWriter.getCachePath("NCML").toFile
//    ncmlDir.mkdirs
//    AggregationWriter.updateNCMLFiles( collectionsMetaFile, ncmlDir )
//  }
//}

object CDScanTest {
  def main(args: Array[String]) {
    val collectionId = "giss_r1i1p1_agg-test"
    val dataPath = "/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1_agg"
    val pathFile = new File(dataPath)
    AggregationWriter.extractAggregations(collectionId, pathFile.toPath )
    FileHeader.term()
  }
}


package nasa.nccs.edas.sources

import java.io._
import java.nio.channels.Channels
import java.nio.file.{FileSystems, Files, Path, Paths}
import javax.xml.parsers.{ParserConfigurationException, SAXParserFactory}
import java.util.concurrent.{ExecutorService, Executors}

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import collection.mutable
import nasa.nccs.caching.FragmentPersistence
import nasa.nccs.cdapi.cdm.{CDGrid, CDSVariable, DiskCacheFileMgr}
import nasa.nccs.edas.sources.netcdf.{NCMLWriter, NetcdfDatasetMgr}
import nasa.nccs.utilities.Loggable
import ucar.nc2.dataset.NetcdfDataset
import ucar.{ma2, nc2}
import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.xml.factory.XMLLoader
import scala.xml.{Node, SAXParser, XML}

object AxisNames {
  def apply( x: String = "", y: String = "", z: String = "", t: String = "" ): Option[AxisNames] = {
    val nameMap = Map( 'x' -> x, 'y' -> y, 'z' -> z, 't' -> t )
    Some( new AxisNames( nameMap ) )
  }
}
class AxisNames( val nameMap: Map[Char,String]  ) {
  def apply( dimension: Char  ): Option[String] = nameMap.get( dimension ) match {
    case Some(name) => if (name.isEmpty) None else Some(name)
    case None=> throw new Exception( s"Not an axis: $dimension" )
  }
}

object EDAS_XML extends XMLLoader[Node]  {

  override def parser: SAXParser = {
    val f = SAXParserFactory.newInstance()
    f.setNamespaceAware(false)
    f.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    f.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
    f.setFeature("http://xml.org/sax/features/external-general-entities", false)
    f.setFeature("http://xml.org/sax/features/external-parameter-entities", false)
    f.setXIncludeAware(false)
    f.newSAXParser()
  }
}


trait XmlResource extends Loggable {
  val Encoding = "UTF-8"

  def saveXML( fileName: String, node: xml.Node ) = {
    val pp = new xml.PrettyPrinter( 800, 2 )
    logger.info( "Persisting resource to file "+ fileName )
    val fos = new FileOutputStream(fileName)
    val writer = Channels.newWriter(fos.getChannel(), Encoding)
    try {
      writer.write("<?xml version='1.0' encoding='" + Encoding + "'?>\n")
      writer.write(pp.format(node))
    } finally {
      writer.close()
    }
  }

  def getFilePath(resourcePath: String) = Option( getClass.getResource(resourcePath) ) match {
    case None => Option( getClass.getClassLoader.getResource(resourcePath) ) match {
      case None =>
        throw new Exception(s"Resource $resourcePath does not exist!")
      case Some(r) => r.getPath
    }
    case Some(r) => r.getPath
  }

  def attr( node: xml.Node, att_name: String ): String = { node.attribute(att_name) match { case None => ""; case Some(x) => x.toString }}
  def attrOpt( node: xml.Node, att_name: String ): Option[String] = node.attribute(att_name).map( _.toString )
  def normalize(sval: String): String = sval.stripPrefix("\"").stripSuffix("\"").toLowerCase
  def nospace( value: String ): String  = value.filter(_!=' ')
}

object Mask  {
  def apply( mtype: String, resource: String ) = { new Mask(mtype,resource) }
}
class Mask( val mtype: String, val resource: String ) extends XmlResource {
  override def toString = "Mask( mtype=%s, resource=%s )".format( mtype, resource )
  def getPath: String = getFilePath( resource )
}

object Masks extends XmlResource {
  val mid_prefix: Char = '#'
  val masks = loadMaskXmlData(getFilePath("/masks.xml"))

  def isMaskId( maskId: String ): Boolean = (maskId(0) == mid_prefix )

  def loadMaskXmlData(filePath:String): Map[String,Mask] = {
    Map(EDAS_XML.loadFile(filePath).child.flatMap( node => node.attribute("id") match {
      case None => None;
      case Some(id) => Some( (mid_prefix +: id.toString) -> createMask(node)); }
    ) :_* )
  }
  def createMask( n: xml.Node ): Mask = { Mask( attr(n,"mtype"), attr(n,"resource") ) }

  def getMask( id: String ): Option[Mask] = masks.get(id)

  def getMaskIds: Set[String] = masks.keySet
}

object CollectionLoadServices {
  val fastPoolSize: Int = 4
  val slowPoolSize: Int = 1
  val loaderServicePool = Executors.newFixedThreadPool(1)
  private var _optLoadService: Option[CollectionLoadService] = None

  def startService(): CollectionLoadService = {
    assert( !loaderServicePool.isShutdown, "Can't restart CollectionLoadService once it has been shut down")
    if( _optLoadService.isEmpty  ) {
      val loadService = new CollectionLoadService(fastPoolSize, slowPoolSize)
      _optLoadService = Some( loadService );
      loaderServicePool.submit(loadService)
    }
    _optLoadService.get
  }

  def term() = {
    _optLoadService.foreach( _.term )
    _optLoadService = None
    loaderServicePool.shutdown()
  }

  def loadCollection( collId: String ): Boolean = { startService.loadCollection(collId) }
}

class Collection( val ctype: String, val id: String, val dataPath: String, val aggregations: Map[String,Aggregation], val fileFilter: String = "", val scope: String="local", val title: String= "", val vars: List[String] = List(), optGrid: Option[CDGrid] = None ) extends Serializable with Loggable {
  val collId = id // Collections.idToFile(id)
  private val variables = new ConcurrentLinkedHashMap.Builder[String, CDSVariable].initialCapacity(10).maximumWeightedCapacity(500).build()
  override def toString = "Collection( id=%s, ctype=%s, path=%s, title=%s, fileFilter=%s )".format(id, ctype, dataPath, title, fileFilter)
  def isEmpty = dataPath.isEmpty
  lazy val varNames = vars.map( varStr => varStr.split(Array(':', '|')).head )
  lazy val grid = optGrid.getOrElse( CDGrid(id, dataPath) )
  def getAggregation( varName: String ): Option[Aggregation] = aggregations.get(varName)

  def isMeta: Boolean = dataPath.endsWith(".csv")
  def deleteAggregation() = grid.deleteAggregation
  def getVariableMetadata(varName: String): List[nc2.Attribute] = grid.getVariableMetadata(varName)
  def getGridFilePath = grid.gridFilePath
  def getVariable(varName: String): CDSVariable = variables.getOrElseUpdate(varName, new CDSVariable(varName, this))

  def getDatasetMetadata(): List[nc2.Attribute] = List(
    new nc2.Attribute("variables", varNames),
    new nc2.Attribute("path", dataPath),
    new nc2.Attribute("ctype", ctype)
  ) ++ grid.attributes

//  def generateAggregation(): xml.Elem = {
//    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.aquireFile(grid.gridFilePath, 10.toString, true)
//    try {
//      _aggCollection(ncDataset)
//    } catch {
//      case err: Exception => logger.error("Can't aggregate collection for dataset " + ncDataset.toString); throw err
//    }
//  }

  def readVariableData(varShortName: String, section: ma2.Section): ma2.Array =
    NetcdfDatasetMgr.readVariableData(varShortName, dataPath, section )

//  private def _aggCollection(dataset: NetcdfDataset): xml.Elem = {
//    val vars = dataset.getVariables.filter(!_.isCoordinateVariable).map(v => Collections.getVariableString(v)).toList
//    val title: String = Collections.findAttribute(dataset, List("Title", "LongName"))
//    val newCollection = new Collection(ctype, id, dataPath, fileFilter, scope, title, vars)
//    Collections.updateCollection(newCollection)
//    newCollection.toXml
//  }
//  def url(varName: String = "") = ctype match {
//    case "http" => dataPath
//    case _ => "file://" + dataPath
//  }


//  def getVarNodes: Seq[(String,Node)] = if(isMeta) {
//    val subCollections = new MetaCollectionFile(dataPath).aggregations
//    subCollections flatMap ( _.getVarNodes )
//  } else {
//    val vnames: List[String] = vars.map( _.split(':').head ).filter( !_.endsWith("_bnds") )
//    vnames.map( vname => ( vname, getVariable( vname ).toXmlHeader ) )
//  }

  def getResolution: String = try {
    grid.resolution.map{ case (key,value)=> s"$key:" + f"$value%.2f"}.mkString(";")
  } catch {
    case ex: Exception =>
      print( s"Exception in Collection.getResolution: ${ex.getMessage}" )
      ex.printStackTrace()
      ""
  }


  def toXml: xml.Elem = {
    <collection id={id} title={title} resolution={getResolution}>
      { aggregations.values.toSet.map { agg: Aggregation => agg.toXml } }
    </collection>
  }

  // <variable name={elems.head} axes={elems.last}> {v} </variable> } ) }

//  def createNCML( pathFile: File, collectionId: String  ): String = {
//    val _ncmlFile = Collections.getAggregationPath.resolve(collectionId).toFile
//    val recreate = appParameters.bool("ncml.recreate", false)
//    if (!_ncmlFile.exists || recreate) {
//      logger.info( s"Creating NCML file for collection ${collectionId} from path ${pathFile.toString}")
//      _ncmlFile.getParentFile.mkdirs
//      val ncmlWriter = NCMLWriter(pathFile.toString)
//      val variableMap = new collection.mutable.HashMap[String,String]()
//      val varNames: List[String] = ncmlWriter.writeNCML(_ncmlFile)
//      varNames.foreach( vname => variableMap += ( vname -> collectionId ) )
//      NCMLWriter.writeCollectionDirectory( collectionId, variableMap.toMap )
//    }
//    _ncmlFile.toString
//  }

//  def getDataFilePath( uri: String, collectionId: String ) : String = ctype match {
//    case "csv" =>
//      val pathFile: File = new File(toFilePath(uri))
//      createNCML( pathFile, collectionId )
//    case "txt" =>
//      val pathFile: File = new File(toFilePath(uri))
//      createNCML( pathFile, collectionId )
//    case "file" =>
//      val pathFile: File = new File(toFilePath(uri))
//      if( pathFile.isDirectory ) createNCML( pathFile, collectionId )
//      else pathFile.toString
//    case "dap" => uri
//    case _ => throw new Exception( "Unexpected attempt to create Collection data file from ctype " + ctype )
//  }

  def toFilePath(path: String): String = path.split(':').last.trim

  def toFilePath1(path: String): String = {
    if (path.startsWith("file:")) path.substring(5)
    else path
  }
}


//object Collection extends Loggable {
////  def apply( id: String, ncmlFile: File ) = {
////    new Collection( "file", id, ncmlFile.toString )
////  }
//  def apply( id: String,  dataPath: String, fileFilter: String = "", scope: String="", title: String= "", vars: List[String] = List() ) = {
//    val ctype = dataPath match {
//      case url if(url.startsWith("http")) => "dap"
//      case url if(url.startsWith("file:")) => "file"
//      case col if(col.startsWith("collection:")) => "collection"
//      case dpath if(dpath.toLowerCase.endsWith(".csv")) => "csv"
//      case dpath if(dpath.toLowerCase.endsWith(".txt")) => "txt"
//      case fpath if(new File(fpath).isFile) => "file"
//      case dir if(new File(dir).isDirectory) => "file"
//      case _ => throw new Exception( "Unrecognized Collection type, dataPath = " + dataPath )
//    }
//    new Collection( ctype, id, dataPath, fileFilter, scope, title, vars )
//  }
//
//}


class CollectionLoadService( val fastPoolSize: Int = 4, val slowPoolSize: Int = 1 ) extends Runnable {
  val collPath= Paths.get( DiskCacheFileMgr.getDiskCachePath("collections").toString, "agg" )
  val fastPool: ExecutorService = Executors.newFixedThreadPool(fastPoolSize)
  val slowPool: ExecutorService = Executors.newFixedThreadPool(slowPoolSize)
  val ncmlExtensions = List( ".csv" )
  private var _active = true;

  def loadCollection( requested_collId: String ): Boolean = {
    val collectionFiles: List[File] = collPath.toFile.listFiles.filter(_.isFile).toList.filter { file => ncmlExtensions.exists(file.getName.toLowerCase.endsWith(_)) }
    for ( collectionFile <- collectionFiles;  fileName = collectionFile.getName;  collId = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase; if collId.equalsIgnoreCase(requested_collId) ) {
      if( ! Collections.hasCollection(collId) ) {
        val loader = new CollectionLoader(collId,collectionFile)
        loader.run()
        true
      }
    }
    false
  }

  def run() {
    try {
      val loadingCollections = mutable.HashMap.empty[String,String]
      while (_active) {
        val collectionFiles: List[File] = collPath.toFile.listFiles.filter(_.isFile).toList.filter { file => ncmlExtensions.exists(file.getName.toLowerCase.endsWith(_)) }
        for ( collectionFile <- collectionFiles;  fileName = collectionFile.getName;  collId = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase; if collId != "local_collections" ) {
          if( Collections.hasCollection(collId) ) {
            loadingCollections.remove(collId)
          } else if( !loadingCollections.contains(collId) ) {
            loadingCollections += ( collId -> collId )
            fastPool.execute(new CollectionLoader(collId,collectionFile))
            if( !Collections.hasGridFile(collectionFile) ) {
              slowPool.execute(new CollectionGridFileLoader(collId,collectionFile))
            }
          }
        }
        Thread.sleep( 500 )
      }
    } finally {
      term()
    }
  }

  def term(): Unit = {
    _active = false;
    fastPool.shutdownNow()
    slowPool.shutdownNow()
  }
}

class CollectionLoader( val collId: String, val collectionFile: File ) extends Runnable  with Loggable  {
  def run() {
    logger.info( s" ---> Loading collection $collId" )
    Collections.addCollection( collId, Some( collectionFile.toString ) )
  }
}

class CollectionGridFileLoader( val collId: String, val collectionFile: File ) extends Runnable  with Loggable  {
  def run() {
    logger.info( s" ---> Loading collection $collId" )
    val optCollection = Collections.addCollection( collId, Some( collectionFile.toString ) )
    optCollection foreach { collection =>
      logger.info(s"Creating grid file ${collection.grid.gridFilePath} for collection ${collId}")
    }
  }
}



object Collections extends XmlResource with Loggable {
  val maxCapacity: Int=500
  val initialCapacity: Int=10
  private val _datasets: TrieMap[String,Collection] =  TrieMap.empty[String,Collection]

  //  def initCollectionList = if( datasets.isEmpty ) { refreshCollectionList }

  def hasGridFiles( collectionFile: File ): Boolean = {
    val gridFiles = for( line <- Source.fromFile( collectionFile ).getLines; elems = line.split(",").map(_.trim) ) yield ( elems.last.split('.').dropRight(1) + "nc" ).mkString(".")
    gridFiles.forall( gFile => Files.exists( Paths.get( gFile)  ) )
  }
  def hasGridFile( collectionFile: File ): Boolean = {
    val gridFile = ( collectionFile.toString.split('.').dropRight(1) + "nc" ).mkString(".")
    Files.exists( Paths.get( gridFile )  )
  }

  def getCollectionFromPath( path: String ): Option[Collection] = _datasets.values.find( _.dataPath.equals( path ) )
  def getCollectionPaths: Iterable[String] = _datasets.values.map( _.dataPath )

  def getCacheDir: String = {
    val collection_file_path =
      Collections.getCacheFilePath("local_collections.xml")
    new java.io.File(collection_file_path).getParent.stripSuffix("/")
  }

  def isCollectionFile(file: File): Boolean = {
    val fname = file.getName.toLowerCase
    file.isFile && fname.endsWith(".txt")
  }

  def getCachePath(subdir: String): Path = {
    FileSystems.getDefault.getPath(getCacheDir, subdir)
  }

  def getAggregationPath: Path = getCachePath("agg")

  //  def refreshCollectionList = {
  //    var collPath: Path = null
  //    try {
  //      collPath= Paths.get( DiskCacheFileMgr.getDiskCachePath("collections").toString, "NCML" )
  //      val ncmlExtensions = List( ".ncml", ".csv" )
  //      val ncmlFiles: List[File] = collPath.toFile.listFiles.filter(_.isFile).toList.filter { file => ncmlExtensions.exists(file.getName.toLowerCase.endsWith(_)) }
  ////      val collFuts = Future.sequence( for (  ncmlFile <- ncmlFiles;  fileName = ncmlFile.getName;  collId = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase;
  ////                            if (collId != "local_collections") && !datasets.containsKey(collId) ) yield Future { Collections.addCollection(collId, ncmlFile.toString) } )
  ////      Await.result( collFuts, Duration.Inf )
  //
  //      for (  ncmlFile <- ncmlFiles;  fileName = ncmlFile.getName;  collId = fileName.substring(0, fileName.lastIndexOf('.')).toLowerCase;
  //             if (collId != "local_collections") && !datasets.containsKey(collId) ) { Collections.addCollection(collId, ncmlFile.toString) }
  //
  //      logger.info( " ---> Updating Collections from files: \n\t" + ncmlFiles.map(_.toString).sorted.mkString("\n\t") + "\n----> Collections = \n\t" + Collections.getCollectionKeys.sorted.mkString("\n\t"))
  //
  //    } catch { case ex: Exception => logger.error( " Error refreshing Collection List from '%s': %s".format( collPath , ex.getMessage ) ) }
  //  }

  def toXml: xml.Elem = {
    <collections>
      {for ( (id: String, collection: Collection) <- _datasets; if collection.isMeta ) yield collection.toXml}
    </collections>
  }

  def toXml( scope: String ): xml.Elem = {
    <collections>
      { for( ( id: String, collection:Collection ) <- _datasets; if collection.scope.equalsIgnoreCase(scope) ) yield collection.toXml }
    </collections>
  }

  def getCollectionMetadata( collId: String  ): List[nc2.Attribute] = {
    findCollection( collId ) match {
      case None => List.empty[nc2.Attribute]
      case Some( coll ) => coll.getDatasetMetadata()
    }
  }

  def findAttribute( dataset: NetcdfDataset, possible_names: List[String] ): String =
    dataset.getGlobalAttributes.toList.find( attr => possible_names.contains( attr.getShortName ) ) match { case Some(attr) => attr.getStringValue; case None => "" }

//  def updateVars = {
//    for( ( id: String, collection:Collection ) <- _datasets; if collection.scope.equalsIgnoreCase("local") ) {
//      logger.info( "Opening NetCDF dataset(4) at: " + collection.dataPath )
//      val dataset: NetcdfDataset = NetcdfDatasetMgr.aquireFile( collection.dataPath, 13.toString )
//      val vars = dataset.getVariables.filter(!_.isCoordinateVariable).map(v => getVariableString(v) ).toList
//      val title = findAttribute( dataset, List( "Title", "LongName" ) )
//      val newCollection = new Collection( collection.ctype, id, collection.dataPath, collection.fileFilter, "local", title, vars)
//      println( "\nUpdating collection %s, vars = %s".format( id, vars.mkString(";") ))
//      _datasets.put( collection.id, newCollection  )
//      dataset.close()
//    }
//    //    persistLocalCollections()
//  }

  def getVariableString( variable: nc2.Variable ): String = variable.getShortName + ":" + variable.getDimensionsString.replace(" ",",") + ":" + variable.getDescription+ ":" + variable.getUnitsString
  def getCacheFilePath( fileName: String ): String = DiskCacheFileMgr.getDiskCacheFilePath( "collections", fileName)
  def getNCMLDirectory: Path = DiskCacheFileMgr.getCacheDirectory( "collections", "agg")

  def getVariableListXml(vids: Array[String]): xml.Elem = {
    <collections>
      { for (vid: String <- vids; vidToks = vid.split('!'); varName=vidToks(0); cid=vidToks(1) ) yield Collections.findCollection(cid) match {
      case Some(collection) => <variables cid={collection.id}> { collection.getVariable(varName).toXml } </variables>
      case None => <error> {"Unknown collection id in identifier: " + cid } </error>
    }}
    </collections>
  }

  def getPersistedVariableListXml: xml.Elem = FragmentPersistence.getFragmentListXml

  def idSet: Set[String] = _datasets.keySet.toSet
  def values: Iterator[Collection] = _datasets.valuesIterator

  def uriToFile( uri: String ): String = {
    uri.toLowerCase.split(":").last.stripPrefix("/").stripPrefix("/").replaceAll("[-/]","_").replaceAll("[^a-zA-Z0-9_.]", "X") + ".ncml"
  }
  def idToFile( id: String, ext: String = ".ncml" ): String = id.replaceAll("[-/]","_").replaceAll("[^a-zA-Z0-9_.]", "X") + ext
  def fileToId( file: File ): String = { file.getName.split('.').dropRight(1).mkString(".") }

  def removeCollections( collectionIds: Array[String] ): Array[String] = {
    val removedCids = collectionIds.flatMap( collectionId  => {
      findCollection(collectionId) match {
        case Some(collection) =>
          logger.info("Removing collection: " + collection.id )
          _datasets.remove( collection.id )
          collection.deleteAggregation()
          Some(collection.id)
        case None => logger.error("Attempt to delete collection that does not exist: " + collectionId ); None
      }
    } )
    //    persistLocalCollections()
    removedCids
  }

  //  def aggregateCollection( id: String, dataPath: String, fileFilter: String, title: String, vars: List[String] ): Collection = {
  //    val cvars = if(vars.isEmpty) getVariableList( dataPath ) else vars
  //    val collection = Collection( id, dataPath, fileFilter, "local", title, cvars )
  //    collection.generateAggregation()
  //    _datasets.put( id, collection  )
  ////    persistLocalCollections()
  //    collection
  //  }

  def addCollection( id: String, optFilePath: Option[String] = None ): Option[Collection] = {
    val collectionFilePath = optFilePath getOrElse { getNCMLDirectory.resolve(id + ".csv").toString }
    try {
      val collection = _datasets.getOrElseUpdate(id, {
        val vars = for (line <- Source.fromFile(collectionFilePath).getLines; elems = line.split(",").map(_.trim)) yield elems.head
        val aggregations: Map[String,Aggregation] = getAggregations(collectionFilePath)
        new Collection("file", id, collectionFilePath, aggregations, "", "", "Aggregated Collection", vars.toList)
      })
      Some(collection)
    } catch {
      case err: Exception =>
        val msg = err.getMessage
        logger.error(s"Error reading collection ${id} from ncml ${collectionFilePath}: ${msg}")
        None
    }
  }

  def getCollections: List[String] = getNCMLDirectory.toFile.listFiles.filter(_.isFile).toList.filter { _.getName.endsWith(".csv") } map { _.getName.split('.').dropRight(1).mkString(".") }

  def getAggregations(collectionFilePath: String ): Map[String,Aggregation] = {
    val aggs = mutable.HashMap.empty[String,Aggregation]
    val agFiles: Iterator[(String,String)] = for (line <- Source.fromFile(collectionFilePath).getLines; elems = line.split(",").map(_.trim)) yield elems.head -> new File( elems.last ).getAbsolutePath
    val agMapIter = agFiles.map { case ( varId, file) => varId -> aggs.getOrElseUpdate( file, Aggregation.read( file ) ) }
    agMapIter.toMap[String,Aggregation]
  }




  //  def addCollection(  id: String, dataPath: String, title: String, vars: List[String] ): Collection = {
  //    val collection = Collection( id, dataPath, "", "local", title, vars )
  ////    collection.generateAggregation
  //    _datasets.put( id, collection  )
  ////    persistLocalCollections()
  //    collection
  //  }

  def updateCollection( collection: Collection ): Collection = {
    _datasets.put( collection.id, collection  )
    logger.info( " *----> Persist New Collection: " + collection.id )
    //    persistLocalCollections()
    collection
  }

  def isNcFileName(fName: String): Boolean = {
    val fname = fName.toLowerCase;
    fname.endsWith(".nc4") || fname.endsWith(".nc") || fname.endsWith(".hdf") || fname.endsWith(".ncml")
  }

  def isNcFile(file: File): Boolean = {
    file.isFile && isNcFileName(file.getName.toLowerCase)
  }

  def findNcFile(file: File): Option[File] = {
    file.listFiles.filter( _.isFile ) foreach {
      f =>  if( f.getName.startsWith(".") ) return None
      else if ( isNcFile(f) ) return Some(f)
    }
    file.listFiles.filter( _.isDirectory ) foreach { f => findNcFile(f) match { case Some(f) => return Some(f); case None => Unit } }
    None
  }

  def hasChildNcFile(file: File): Boolean = { findNcFile(file).isDefined }

  def getVariableList( path: String ): List[String] = {
    findNcFile( new File(path) ) match {
      case Some(f) =>
        logger.info( "Opening NetCDF dataset(5) at: " + f.getAbsolutePath )
        val dset: NetcdfDataset = NetcdfDatasetMgr.aquireFile( f.getAbsolutePath, 15.toString )
        dset.getVariables.toList.flatMap( v => if(v.isCoordinateVariable) None else Some(v.getFullName) )
      case None => throw new Exception( "Can't find any nc files in dataset path: " + path )
    }
  }

  //  def loadCollectionTextData(url:URL): Map[String,Collection] = {
  //    val lines = scala.io.Source.fromURL( url ).getLines
  //    val mapItems = for( line <- lines; toks =  line.split(';')) yield
  //      nospace(toks(0)) -> Collection( url=nospace(toks(1)), url=nospace(toks(1)), vars=getVarList(toks(3)) )
  //    mapItems.toMap
  //  }

  def isChild( subDir: String,  parentDir: String ): Boolean = Paths.get( subDir ).toAbsolutePath.startsWith( Paths.get( parentDir ).toAbsolutePath )
  def findCollectionByPath( subDir: String ): Option[Collection] = _datasets.values.toList.find { case collection => if( collection.dataPath.isEmpty) { false } else { isChild( subDir, collection.dataPath ) } }

//  def loadCollectionXmlData( filePaths: Map[String,String] = Map.empty[String,String] ): TrieMap[String,Collection] = {
//    val maxCapacity: Int=100000
//    val initialCapacity: Int=250
//    val datasets = TrieMap.empty[String, Collection]
//    for ( ( scope, filePath ) <- filePaths.iterator ) if( Files.exists( Paths.get(filePath) ) ) {
//      try {
//        logger.info( "Loading collections from file: " + filePath )
//        val children = EDAS_XML.loadFile(filePath).child
//        children.foreach( node => node.attribute("id") match {
//          case None => None;
//          case Some(id) => try {
//            val collection = getCollection(node, scope)
//            datasets.put(id.toString.toLowerCase, collection)
//            logger.info("Loading collection: " + id.toString.toLowerCase)
//          } catch { case err: Exception =>
//            logger.warn( "Skipping collection " + id.toString + " due to error: " + err.toString )
//          }
//        })
//      } catch {
//        case err: Throwable =>
//          logger.error("Error opening collection data file {%s}: %s".format(filePath, err.getMessage))
//          logger.error( "\n\t\t" + err.getStackTrace.mkString("\n\t") )
//      }
//    } else {
//      logger.warn( "Collections file does not exist: " + filePath )
//    }
//    datasets
//  }

  def persistLocalCollections(prettyPrint: Boolean = true) = {
    if(prettyPrint) saveXML( getCacheFilePath("local_collections.xml"), toXml("local") )
    else XML.save( getCacheFilePath("local_collections.xml"), toXml("local") )
  }

  def getVarList( var_list_data: String  ): List[String] = var_list_data.filter(!List(' ','(',')').contains(_)).split(',').toList
//  def getCollection( n: xml.Node, scope: String ): Collection = {
//    Collection( attr(n,"id"), attr(n,"path"), attr(n,"fileFilter"), scope, attr(n,"title"), n.text.split(";").toList )
//  }

  def hasCollection( collectionId: String ): Boolean = _datasets.containsKey( collectionId.toLowerCase )

  def findCollection( collectionId: String ): Option[Collection] =
    _datasets.get( collectionId.toLowerCase )

  def getCollectionXml( collectionId: String ): xml.Elem = {
    _datasets.get( collectionId.toLowerCase ) match {
      case Some( collection: Collection ) => collection.toXml
      case None => <error> { "Invalid collection id:" + collectionId } </error>
    }
  }
  def parseUri( uri: String ): ( String, String ) = {
    if (uri.isEmpty) ("", "")
    else {
      val uri_parts = uri.split(":/")
      val url_type = normalize(uri_parts.head)
      (url_type, uri_parts.last)
    }
  }

  //  def getCollection(collection_uri: String, var_names: List[String] = List()): Option[Collection] = {
  //    parseUri(collection_uri) match {
  //      case (ctype, cpath) => ctype match {
  //        case "file" => Some(Collection( url = collection_uri, vars = var_names ))
  //        case "collection" =>
  //          val collection_key = cpath.stripPrefix("/").stripSuffix("\"").toLowerCase
  //          logger.info( " getCollection( %s ) ".format(collection_key) )
  //          datasets.get( collection_key )
  //      }
  //    }
  //  }

  def getCollectionKeys: Array[String] = _datasets.keys.toArray
}








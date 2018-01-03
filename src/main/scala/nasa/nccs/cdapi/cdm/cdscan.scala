package nasa.nccs.cdapi.cdm

import java.io._
import java.net.URI
import java.nio._
import java.nio.file.{FileSystems, Path, Paths}
import java.util.Formatter

import nasa.nccs.cdapi.cdm.CDScan.logger
import nasa.nccs.cdapi.cdm.FileMetadata.logger
import nasa.nccs.cdapi.cdm.NCMLWriter.{generateNCML, logger, writeCollectionDirectory}
import nasa.nccs.cdapi.tensors.CDDoubleArray
import nasa.nccs.edas.loaders.Collections
import nasa.nccs.edas.utilities.{appParameters, runtime}
import nasa.nccs.utilities.{EDASLogManager, Loggable, cdsutils}
import org.apache.commons.lang.RandomStringUtils
import ucar.nc2.{FileWriter => _, _}
import ucar.{ma2, nc2}
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{NetcdfDataset, _}
import ucar.nc2.time.CalendarDate

import scala.collection.mutable
import collection.mutable.{HashMap, ListBuffer}
import collection.JavaConversions._
import collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.util.matching.Regex

object NCMLWriter extends Loggable {
  val ncExtensions = Seq( "nc", "nc4")
  val colIdSep = "."

  def apply(path: File): NCMLWriter = { new NCMLWriter(Array(path).iterator) }
  def getName(node: nc2.CDMNode): String = node.getFullName
  def isNcFileName(fName: String): Boolean = {
    val fname = fName.toLowerCase;
    fname.endsWith(".nc4") || fname.endsWith(".nc") || fname.endsWith(".hdf") || fname.endsWith(".ncml")
  }

  def backup( dir: File, backupDir: File ): Unit = {
    backupDir.mkdirs()
    for( f <- backupDir.listFiles ) { f.delete() }
    for( f <- dir.listFiles ) { f.renameTo( new File( backupDir, f.getName ) ) }
  }

  def generateNCMLFiles( collectionsFile: File ): Unit = {
    logger.info(s"Generate NCML file from specs in " + collectionsFile.getAbsolutePath )
    for (line <- Source.fromFile( collectionsFile.getAbsolutePath ).getLines; tline = line.trim; if !tline.isEmpty && !tline.startsWith("#")  ) {
      val mdata = tline.split(",").map(_.trim)
      extractSubCollections( mdata(0), Paths.get( mdata(1) ) )
    }
  }


//  def updateNCMLFiles( collectionsFile: File, ncmlDir: File ): Unit = {
//    backup( ncmlDir, new File( ncmlDir, "backup") )
//    logger.info(s"Update NCML file from specs in " + collectionsFile.getAbsolutePath )
//    for (line <- Source.fromFile( collectionsFile.getAbsolutePath ).getLines; tline = line.trim; if !tline.isEmpty && !tline.startsWith("#")  ) {
//      val mdata = tline.split(",").map(_.trim)
//      val agg_type: String = mdata.head
//      val cspecs = mdata.tail
//      val collectionId = cspecs.head
//      val variableMap = new collection.mutable.HashMap[String,String]()
//      val paths: Array[File] = cspecs.tail.filter(!_.isEmpty).map(fpath => new File(fpath))
//      agg_type match {
//        case multi if multi.startsWith("m") =>
//          for( path <- paths; if path.isDirectory ) {
//            for (subdir <- path.listFiles; if subdir.isDirectory) {
//              val subCollectionId = collectionId + "_" + subdir.getName
//              val varNames = generateNCML(subCollectionId, Array(subdir))
//              varNames.foreach(vname => variableMap += (vname -> subCollectionId))
//            }
//            val dataFiles = path.listFiles.filter(_.isFile)
//            getFileGroups(dataFiles) foreach { case ( group_name, files  ) =>
//              val subCollectionId = collectionId + "_" + group_name
//              val varNames = generateNCML( subCollectionId, files )
//              varNames.foreach(vname => variableMap += (vname -> subCollectionId))
//            }
//          }
//        case singl if singl.startsWith("s") =>
//          val varNames = generateNCML( collectionId, paths )
//          varNames.foreach( vname => variableMap += ( vname -> collectionId ) )
//        case _ => throw new Exception( "Unrecognized aggregation type: " + agg_type )
//      }
//      writeCollectionDirectory( collectionId, variableMap.toMap )
//    }
//  }

  def isNcDataFile( file: File ): Boolean = {
    file.isFile &&  ncExtensions.contains( file.getName.split('.').last )
  }

  def recursiveListNcFiles( rootPath: Path, optSearchSubDir: Option[Path] = None ): Array[Path] = {
    val children = optSearchSubDir.fold( rootPath )( rootPath.resolve ).toFile.listFiles
//    print( s"\nrecursiveListNcFiles-> root: ${rootPath.toString}, rel: ${optSearchSubDir.fold("")(_.toString)}, children: [ ${children.map(_.toString).mkString(", ")} ]" )
    val files = children.filter( isNcDataFile ).map( child => rootPath.relativize( child.toPath  ) )
    files ++ children.filter( _.isDirectory ).flatMap( dir => recursiveListNcFiles( rootPath, Some( rootPath.relativize(dir.toPath) ) ) )
  }

  def extractSubCollections( collectionId: String, dataLocation: Path, options: Map[String,String] = Map.empty ): Unit = {
    assert( dataLocation.toFile.exists, s"Data location ${dataLocation.toString} does not exist:")
//    logger.info(s" %C% Extract collection $collectionId from " + dataLocation.toString)
    val ncSubPaths = recursiveListNcFiles(dataLocation)
    val bifurDepth: Int = options.getOrDefault("depth","0").toInt
    val nameTemplate: Regex = options.getOrDefault("template",".*").r
    var subColIndex: Int = 0
    val varMap: Seq[(String,String)] = getPathGroups(dataLocation, ncSubPaths, bifurDepth, nameTemplate ) flatMap { case (group_key, (subCol_name, files)) =>
      val subCollectionId = collectionId + "-" + { if( subCol_name.trim.isEmpty ) { group_key } else subCol_name }
      logger.info(s" %X% Extract SubCollections($collectionId)-> group_key=$group_key, subCol_name=$subCol_name, files=${files.mkString(";")}" )
      val varNames = generateNCML(subCollectionId, files.map(fp => dataLocation.resolve(fp).toFile))
      varNames.map(vname => vname -> subCollectionId)
    }
//    logger.info(s" %C% extractSubCollections varMap: " + varMap.map(_.toString()).mkString("; ") )
    val contextualizedVarMap: Seq[(String,String)] = varMap.groupBy { _._1 } .values.map( scopeRepeatedVarNames ).toSeq.flatten
    writeCollectionDirectory( collectionId, Map( contextualizedVarMap:_* ) )
  }

  def rid( len: Int = 6 ) = RandomStringUtils.random( 6, true, true )

  def scopeRepeatedVarNames( singleVarMaps: Seq[(String,String)] ): Seq[(String,String)] = {
    if (singleVarMaps.size == 1) { singleVarMaps }
    else {
      val collIds: Seq[Array[String]] = singleVarMaps.map( _._2.split('-').last.split('.') )
//      logger.info(s" %C% scopeRepeatedVarNames CollIds: " + collIds.map(_.mkString("(",", ",")")).mkString("; ") )
      val scopeElems: IndexedSeq[Seq[String]] = collIds.head.indices.map( index => collIds.map( a => a(index))).filter( _.groupBy( x => x ).size > 1 )
      val scopes: Map[Int,String] = Map( getScopes(scopeElems).zipWithIndex map { case (elem, i) => (i -> elem) }: _* )
      val result = singleVarMaps.zipWithIndex map { case (elem, i) => ( scopes.getOrElse(i,rid()) + "/" + elem._1, elem._2 ) }
//      logger.info(s" %C% scopeRepeatedVarNames[${singleVarMaps.size}]\n\tINPUT: [${singleVarMaps.map(_.toString()).mkString(", ")}] \n\tRESULT: ${result.map(_.toString()).mkString(", ")}" )
      result
    }
  }

  def getScopes( scopeElems: IndexedSeq[Seq[String]] ): IndexedSeq[String] = {
//    scopeElems.head.indices.map( index => scopeElems.map( a => a(index) ) ).map (_.mkString("."))
    scopeElems.map (_.mkString("."))
  }




//    for (line <- Source.fromFile( collectionsFile.getAbsolutePath ).getLines; tline = line.trim; if !tline.isEmpty && !tline.startsWith("#")  ) {
//      val mdata = tline.split(",").map(_.trim)
//      val agg_type: String = mdata.head
//      val cspecs = mdata.tail
//      val collectionId = cspecs.head
//      val variableMap = new collection.mutable.HashMap[String,String]()
//      val paths: Array[File] = cspecs.tail.filter(!_.isEmpty).map(fpath => new File(fpath))
//      agg_type match {
//        case multi if multi.startsWith("m") =>
//          for( path <- paths; if path.isDirectory ) {
//            for (subdir <- path.listFiles; if subdir.isDirectory) {
//              val subCollectionId = collectionId + "_" + subdir.getName
//              val varNames = generateNCML(subCollectionId, Array(subdir))
//              varNames.foreach(vname => variableMap += (vname -> subCollectionId))
//            }
//            val dataFiles = path.listFiles.filter(_.isFile)
//            getFileGroups(dataFiles) foreach { case ( group_name, files ) =>
//              val subCollectionId = collectionId + "_" + group_name
//              val varNames = generateNCML( subCollectionId, files )
//              varNames.foreach(vname => variableMap += (vname -> subCollectionId))
//            }
//          }
//        case singl if singl.startsWith("s") =>
//          val varNames = generateNCML( collectionId, paths )
//          varNames.foreach( vname => variableMap += ( vname -> collectionId ) )
//        case _ => throw new Exception( "Unrecognized aggregation type: " + agg_type )
//      }
//      writeCollectionDirectory( collectionId, variableMap.toMap )
//    }
//  }

//  def getFileGroups(dataFiles: Seq[File]): Map[String,Array[File]] = {
//    val groupMap = mutable.HashMap.empty[String,mutable.ListBuffer[File]]
//    dataFiles.foreach( df => groupMap.getOrElseUpdate( getVariablesKey( df ), mutable.ListBuffer.empty[File] ) += df )
//    groupMap.mapValues(_.toArray).toMap
//  }

  def getPathGroups(rootPath: Path, relFilePaths: Seq[Path], bifurDepth: Int, nameTemplate: Regex ): Seq[(String,(String,Array[Path]))] = {
    val groupMap = mutable.HashMap.empty[String,mutable.ListBuffer[Path]]
    relFilePaths.foreach(df => groupMap.getOrElseUpdate(getPathKey(rootPath, df), mutable.ListBuffer.empty[Path]) += df)
//    logger.info(s" %X% relFilePaths: \n\t ----> ${groupMap.mapValues(_.map(_.toString).mkString("[",",","]")).mkString("\n\t ----> ")} " )
    if( bifurDepth == 0 ) {
      groupMap.mapValues(df => (getSubCollectionName(df), df.toArray)).toSeq
    } else {
      val unsimplifiedResult = groupMap.toSeq map { case (groupKey, grRelFilePaths) =>
        val discrimPathElems: Iterable[Seq[String]] = filterCommonElements(grRelFilePaths.map(df => df.subpath(0, bifurDepth).map(_.toString).toSeq).toList,true)
        val CollIdNames = discrimPathElems.head
        val result = ( groupKey, ( CollIdNames, grRelFilePaths.toArray)  )
        result
      }
      val filteredColIds: IndexedSeq[String] = filterCommonElements( unsimplifiedResult.map( _._2._1).toList, false ).map( _.mkString(colIdSep) ).toIndexedSeq
      unsimplifiedResult.zipWithIndex  map { case (elem, index) => ( elem._1, ( filteredColIds(index), elem._2._2 ) ) }
    }
  }

  def filterCommonElements( paths: List[ Seq[String] ], keepCommon: Boolean ): Iterable[ Seq[String] ] = {
    val nElems = paths.head.length
    logger.info(s" %XX% filterCommonElements[$keepCommon]: paths=\n\t ${paths.map(_.mkString("[",",","]")).mkString("\n\t ")}]")
    val isCommon: Array[Boolean] = (0 until nElems).map( index => paths.map( seq => seq(index)).groupBy(x => x).size == 1 ).toArray
    val result = paths.map( seq => seq.zipWithIndex flatMap { case (name, index) =>
      if( isCommon(index) ) { if (keepCommon) Some(name) else None }
      else { if (keepCommon) None else Some(name) } }
    )
    logger.info(s" %XX% filterCommonElements: isCommon=[${isCommon.mkString(";")}] paths=[${paths.mkString(";")}] result=[${result.map(_.mkString("/")).mkString(";")}]")
    result
  }

  def trimCommonNameElements( paths: Iterable[ Seq[String] ], prefix: Boolean ): Iterable[ Seq[String] ] = {
    val pathElements: Iterable[Seq[String]] = paths.map( _.iterator().map(_.toString).toSeq )
    if(  pathElements.groupBy { if(prefix) _.head else _.last }.size == 1 ) {
      trimCommonNameElements( pathElements.map { if(prefix) _.drop(1) else _.dropRight(1) }, prefix )
    } else { paths }
  }

  def extractCommonPrefix( pathElements: Iterable[Seq[String]], commonPrefixElems: Seq[String] = Seq.empty ): Seq[String] = if( pathElements.size < 2 ) {
    commonPrefixElems
  } else {
//    logger.info(s" %ECP% ExtractCommonPrefix --> pathElements:  [ ${pathElements.map(_.mkString(":")).mkString("; ")} ] ,  commonPrefixElems: [ ${commonPrefixElems.mkString("; ")} ]  ")
    if( pathElements.groupBy( _.headOption.getOrElse( RandomStringUtils.random( 6, true, true ) ) ).size == 1 ) {
      extractCommonPrefix( pathElements.map( _.drop(1) ),  commonPrefixElems ++ Seq( pathElements.head.head ) )
    } else if( commonPrefixElems.isEmpty ) { Seq( extractCommonString( pathElements ) ) } else { commonPrefixElems }
  }

  def extractCommonString( pathElements: Iterable[Seq[String]] ): String = commonPrefix( pathElements.map( _.mkString("~") ).toSeq )

  def commonPrefix( elems: Seq[String] ): String = {
    val result = elems.foldLeft("")((_,_) => (elems.min.view,elems.max.view).zipped.takeWhile(v => v._1 == v._2).unzip._1.mkString)
    logger.info(s" %ECP% commonPrefix: ${elems.mkString(", ")}; result = ${result}" )
    result
  }

  def trimCommonNameElements( paths: Iterable[Path] ): Iterable[Path] =
    trimCommonNameElements( trimCommonNameElements( paths.map( _.iterator().map(_.toString).toSeq ) ,false ), true ).map( seq => Paths.get( seq.mkString("/") ) )

  def getSubCollectionName( paths: Iterable[Path] ): String = extractCommonPrefix( paths.map( _.iterator().flatMap( _.toString.split("[_.-]")).toSeq ) ).mkString(".")

//  def getVariablesKey( file: File ): String = {
//    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.openFile( file.toString )
//    val all_vars = ncDataset.getVariables groupBy { _.isCoordinateVariable }
//    val variables: List[nc2.Variable] = all_vars.getOrElse( false, List.empty ).toList
//    val coord_variables: List[nc2.Variable] = all_vars.getOrElse( true, List.empty ).toList
//    val bounds_vars: List[String] = variables flatMap { v => Option( v.findAttributeIgnoreCase("bounds") ) }  map { _.getStringValue }
//    val vkey = variables map { _.getShortName } filterNot { bounds_vars.contains } mkString "-"
//    logger.info( s" %K% getVariablesKey: bounds_vars = [ ${bounds_vars.mkString(", ")} ], vkey = ${vkey}")
//    vkey
//  }

  def getPathKey( rootPath: Path, relFilePath: Path ): String = {
    val ncDataset: NetcdfDataset = NetcdfDatasetMgr.aquireFile(rootPath.resolve(relFilePath).toString, 1.toString )
    try {
      val (variables, coordVars): (List[nc2.Variable], List[nc2.Variable]) = FileMetadata.getVariableLists(ncDataset)
      relFilePath.mkString(".") + "-" + ( variables map { _.getShortName } mkString "." )
    } finally {
      ncDataset.close
    }
  }

  def writeCollectionDirectory( collectionId: String, variableMap: Map[String,String] ): Unit = {
    val dirFile = getCachePath("NCML").resolve(collectionId + ".csv").toFile
    logger.info( s"Generating CollectionDirectory ${dirFile.toString} from variableMap: \n\t" + variableMap.mkString(";\n\t") )
    val pw = new PrintWriter( dirFile )
    variableMap foreach { case ( varName, subCollectionId ) =>
      val collectionFile = getCachePath("NCML").resolve(subCollectionId + ".ncml").toString
      pw.write(s"$varName, ${collectionFile}\n")
    }
    pw.close
  }

  def generateNCML( collectionId: String, paths: Array[File] ): List[String] = {
    try {
      val ncmlFile = getCachePath("NCML").resolve(collectionId + ".ncml").toFile
      logger.info(s"Creating NCML file for collection ${collectionId} from paths ${paths.map(_.getAbsolutePath).mkString(", ")}")
      val writer = new NCMLWriter(paths.iterator)
      writer.writeNCML(ncmlFile)
    } catch {
      case err: Exception =>
        logger.error( s"Error writing NCML file for collection ${collectionId}: ${err.getMessage}")
        List.empty[String]
    }
  }

  def isNcFile(file: File): Boolean = {
    file.isFile && isNcFileName(file.getName.toLowerCase)
  }

  def isCollectionFile(file: File): Boolean = {
    val fname = file.getName.toLowerCase
    file.isFile && fname.endsWith(".txt")
  }
  def getCacheDir: String = {
    val collection_file_path =
      Collections.getCacheFilePath("local_collections.xml")
    new java.io.File(collection_file_path).getParent.stripSuffix("/")
  }

  def getCachePath(subdir: String): Path = {
    FileSystems.getDefault.getPath(getCacheDir, subdir)
  }

  def getNcURIs(file: File): Iterable[URI] = {
    if (isCollectionFile(file)) {
      val bufferedSource = Source.fromFile(file)
      val entries =
        for (line <- bufferedSource.getLines; if isNcFileName(line))
          yield new URI(line)
      entries.toIterable
    } else {
      getNcFiles(file).map(_.toURI)
    }
  }

  def getNcFiles(file: File): Iterable[File] = {
    try {
      if (isNcFile(file)) {
        Seq(file)
      } else {
        val children = new Iterable[File] {
          def iterator =
            if (file.isDirectory) file.listFiles.iterator else Iterator.empty
        }
        Seq(file) ++: children.flatMap(getNcFiles) filter NCMLWriter.isNcFile
      }
    } catch {
      case err: NullPointerException =>
        logger.warn("Empty collection directory: " + file.toString)
        Iterable.empty[File]
    }
  }

  def getNcFiles(args: Iterator[File]): Iterator[File] =
    args.map((arg: File) => NCMLWriter.getNcFiles(arg)).flatten
  def getNcURIs(args: Iterator[File]): Iterator[URI] =
    args.map((arg: File) => NCMLWriter.getNcURIs(arg)).flatten

  def getFileHeaders(files: IndexedSeq[URI], nReadProcessors: Int): IndexedSeq[FileHeader] = {
    if (files.nonEmpty) {
      val groupSize = cdsutils.ceilDiv(files.length, nReadProcessors)
      logger.info( " Processing %d files with %d files/group with %d processors" .format(files.length, groupSize, nReadProcessors))
      val fileGroups = files.grouped(groupSize).toIndexedSeq
      val fileHeaderFuts = Future.sequence(
        for (workerIndex <- fileGroups.indices; fileGroup = fileGroups(workerIndex))
          yield Future { FileHeader.factory(fileGroup, workerIndex) })
      Await.result(fileHeaderFuts, Duration.Inf).flatten sortWith { (afr0, afr1) => afr0.startValue < afr1.startValue }
    } else IndexedSeq.empty[FileHeader]
  }
}

//class NCMLSerialWriter(val args: Iterator[String]) {
//  val files: IndexedSeq[File] = NCMLWriter.getNcFiles(args).toIndexedSeq
//  val nFiles = files.length
//  val fileHeaders = NCMLWriter.getFileHeadersSerial(files)
//
//  def getNCML: xml.Node = {
//    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
//      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
//      <aggregation dimName="time" units={cdsutils.baseTimeUnits} type="joinExisting">
//        { for( fileHeader <- fileHeaders ) yield { <netcdf location={"file:" + fileHeader.path} ncoords={fileHeader.nElem.toString}> { fileHeader.axisValues.mkString(", ") } </netcdf> } }
//      </aggregation>
//    </netcdf>
//  }
//}

class NCMLWriter(args: Iterator[File], val maxCores: Int = 8)  extends Loggable {
  import NCMLWriter._
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors, maxCores )
  private val files: IndexedSeq[URI] = NCMLWriter.getNcURIs(args).toIndexedSeq
  if (files.isEmpty) { throw new Exception( "Error, empty collection at: " + args.map(_.getAbsolutePath).mkString(",")) }
  private val nFiles = files.length
  val fileHeaders = NCMLWriter.getFileHeaders(files, nReadProcessors)
  val outerDimensionSize: Int = fileHeaders.foldLeft(0)(_ + _.nElem)
  val ignored_attributes = List("comments")
  val overwriteTime = fileHeaders.length > 1

  def isIgnored(attribute: nc2.Attribute): Boolean = {
    ignored_attributes.contains(getName(attribute))
  }
  def getDimCoordRef(fileMetadata: FileMetadata, dim: nc2.Dimension): String = {
    val dimname = NCMLWriter.getName(dim)
    fileMetadata.coordVars.map(NCMLWriter.getName(_)).find(vname => (vname equals dimname) || (vname.split(':')(0) == dimname.split(':')(0))) match {
      case Some( dimCoordRef ) => dimCoordRef
      case None => dim.getLength.toString
    }
  }

  def getAttribute(attribute: nc2.Attribute): xml.Node =
    if (attribute.getDataType == ma2.DataType.STRING) {
      if (attribute.getLength > 1) {
        val sarray: IndexedSeq[String] = (0 until attribute.getLength).map(
          i =>
            attribute
              .getStringValue(i)
              .filter(ch => org.jdom2.Verifier.isXMLCharacter(ch)))
          <attribute name={getName(attribute) } value={sarray.mkString("|")} separator="|"/>
      } else {
          <attribute name={getName(attribute)} value={attribute.getStringValue(0)}/>
      }
    } else {
      if (attribute.getLength > 1) {
        val sarray: IndexedSeq[String] = (0 until attribute.getLength).map(i =>
          attribute.getNumericValue(i).toString)
          <attribute name={getName(attribute)} type={attribute.getDataType.toString} value={sarray.mkString(" ")}/>
      } else {
          <attribute name={getName(attribute)} type={attribute.getDataType.toString} value={attribute.getNumericValue(0).toString}/>
      }
    }

  def getDims( fileMetadata: FileMetadata, variable: nc2.Variable ): String =
    variable.getDimensions.map( dim =>
      if (dim.isShared) getDimCoordRef( fileMetadata, dim )
      else if (dim.isVariableLength) "*"
      else dim.getLength.toString
    ).toArray.mkString(" ")

  def getDimension(axis: CoordinateAxis): Option[xml.Node] = {
    axis match {
      case coordAxis: CoordinateAxis1D =>
        val nElems =
          if (coordAxis.getAxisType == AxisType.Time) outerDimensionSize
          else coordAxis.getSize
        val dimension = coordAxis.getDimension(0)
        val node =
            <dimension name={getName(dimension)} length={nElems.toString} isUnlimited={dimension.isUnlimited.toString} isVariableLength={dimension.isVariableLength.toString} isShared={dimension.isShared.toString}/>
        Some(node)
      case x =>
        logger.warn( "This Coord axis type not currently supported: " + axis.getClass.getName + " for axis " + axis.getNameAndDimensions(true) )
        None
    }
  }

  def getAggDatasetTUC(fileHeader: FileHeader,
                       timeRegular: Boolean = false): xml.Node =
      <netcdf location={fileHeader.filePath} ncoords={fileHeader.nElem.toString} timeUnitsChange="true"/>

  def getAggDataset(fileHeader: FileHeader, timeRegular: Boolean = false): xml.Node =
    if (timeRegular || !overwriteTime)
        <netcdf location={fileHeader.filePath} ncoords={fileHeader.nElem.toString}/>
    else
        <netcdf location={fileHeader.filePath} ncoords={fileHeader.nElem.toString} coordValue={fileHeader.axisValues.map( x => "%d".format(x)).mkString(", ")}/>

  def getVariable(fileMetadata: FileMetadata, variable: nc2.Variable,  timeRegularSpecs: Option[(Double, Double)]): xml.Node = {
    val axisType = fileMetadata.getAxisType(variable)
    <variable name={getName(variable)} shape={getDims(fileMetadata,variable)} type={variable.getDataType.toString}> {
        if( axisType == AxisType.Time )  <attribute name="_CoordinateAxisType" value="Time"/>  <attribute name="units" value={if(overwriteTime) cdsutils.baseTimeUnits else variable.getUnitsString}/>
        else for (attribute <- variable.getAttributes; if !isIgnored( attribute ) ) yield getAttribute(attribute)
    }
    {
      if( (axisType != AxisType.Time) && (axisType != AxisType.RunTime) ) variable match {
        case coordVar: CoordinateAxis1D => getData(variable, coordVar.isRegular)
        case _ => getData(variable, false)
      }
    }
    </variable>
  }



  def getData(variable: nc2.Variable, isRegular: Boolean): xml.Node = {
    val dataArray: Array[Double] = CDDoubleArray.factory(variable.read).getArrayData()
    if (isRegular) {
        <values start={"%.3f".format(dataArray(0))} increment={"%.6f".format(dataArray(1)-dataArray(0))}/>
    } else {
      <values> { dataArray.map(dv => "%.3f".format(dv)).mkString(" ") } </values>
    }
  }

  def getTimeSpecs: Option[(Long, Long)] = {
    val t0 = fileHeaders.head.startValue
    val dt = if (fileHeaders.head.nElem > 1) {
      fileHeaders.head.axisValues(1) - fileHeaders.head.axisValues(0)
    } else { fileHeaders(1).startValue - fileHeaders(0).startValue }
    Some(t0 -> dt)
  }

  def makeFullName(tvar: nc2.Variable): String = {
    val g: Group = tvar.getGroup
    if ((g == null) || g.isRoot ) getName(tvar);
    else g.getFullName + "/" + tvar.getShortName;
  }

  def getTimeVarName(fileMetadata: FileMetadata): String = findTimeVariable(fileMetadata) match {
    case Some(tvar) => getName(tvar)
    case None => {
      logger.error(s"Can't find time variable, vars: ${fileMetadata.variables
        .map(v => getName(v) + ": " + fileMetadata.getAxisType(v).toString)
        .mkString(", ")}"); "time"
    }
  }

  def getAggregationTUC(fileMetadata: FileMetadata, timeRegular: Boolean): xml.Node = {
    <aggregation dimName={getTimeVarName(fileMetadata: FileMetadata)} type="joinExisting">  { for (fileHeader <- fileHeaders) yield { getAggDatasetTUC(fileHeader, timeRegular) } } </aggregation>
  }

  def getAggregation(fileMetadata: FileMetadata, timeRegular: Boolean): xml.Node = {
    <aggregation dimName={getTimeVarName(fileMetadata: FileMetadata)} type="joinExisting">  { for (fileHeader <- fileHeaders) yield { getAggDataset(fileHeader, timeRegular) } } </aggregation>
  }

  def findTimeVariable(fileMetadata: FileMetadata): Option[nc2.Variable] =
    fileMetadata.coordVars find (fileMetadata.getAxisType(_) == AxisType.Time)

  def getNCMLVerbose: ( List[String], xml.Node ) = {
    val fileMetadata = FileMetadata(files.head)
    try {
      logger.info(s"\n\n -----> FileMetadata: variables = ${fileMetadata.variables.map(_.getShortName).mkString(", ")}\n\n")
      val timeRegularSpecs = None //  getTimeSpecs( fileMetadata )
      logger.info("Processing %d files with %d workers".format(nFiles, nReadProcessors))
      val result = <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
        <explicit/>
        <attribute name="title" type="string" value="NetCDF aggregated dataset"/>{for (attribute <- fileMetadata.attributes) yield getAttribute(attribute)}{(for (coordAxis <- fileMetadata.coordinateAxes) yield getDimension(coordAxis)).flatten}{for (variable <- fileMetadata.coordVars) yield getVariable(fileMetadata, variable, timeRegularSpecs)}{for (variable <- fileMetadata.variables) yield getVariable(fileMetadata, variable, timeRegularSpecs)}{getAggregation(fileMetadata, timeRegularSpecs.isDefined)}

      </netcdf>
      val varNames: List[String] = fileMetadata.variables.map(_.getShortName)
      (varNames, result)
    } finally {
      fileMetadata.close
    }
  }

//  def defineNewTimeVariable: xml.Node =
//    <variable name={getTimeVarName}>
//      <attribute name="units" value={cdsutils.baseTimeUnits}/>
//      <attribute name="_CoordinateAxisType" value="Time" />
//    </variable>

//  def getNCMLTerse: xml.Node = {
//    val timeRegularSpecs = None // getTimeSpecs
//    println(
//      "Processing %d files with %d workers".format(nFiles, nReadProcessors))
//    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
//      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
//      { (for (coordAxis <- fileMetadata.coordinateAxes) yield getDimension(coordAxis)).flatten }
//      { if(overwriteTime) defineNewTimeVariable }
//      { getAggregation( timeRegularSpecs.isDefined ) }
//    </netcdf>
//  }

//  def getNCMLSimple: xml.Node = {
//    val timeRegularSpecs = None // getTimeSpecs
//    println( "Processing %d files with %d workers".format(nFiles, nReadProcessors) )
//    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
//      <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
//      { getAggregationTUC( timeRegularSpecs.isDefined ) }
//    </netcdf>
//  }

  def writeNCML(ncmlFile: File): List[String] = {
    logger.info("Writing *NCML* File: " + ncmlFile.toString)
    val bw = new BufferedWriter(new FileWriter(ncmlFile))
    val ( varNames, result ) = getNCMLVerbose
    bw.write(result.toString)
    bw.close()
    varNames
  }

}

object FileHeader extends Loggable {
  val maxOpenAttempts = 1
  val retryIntervalSecs = 10
  def apply(file: URI, timeRegular: Boolean): FileHeader = {
    val ncDataset: NetcdfDataset =  NetcdfDatasetMgr.aquireFile(file.toString, 2.toString)
    try {
      val (axisValues, boundsValues) = FileHeader.getTimeCoordValues(ncDataset)
      new FileHeader(file.toString, axisValues, boundsValues, timeRegular)
    } finally {
      ncDataset.close()
    }
  }

  def factory(files: IndexedSeq[URI], workerIndex: Int): IndexedSeq[FileHeader] = {
    var retryFiles = new ListBuffer[URI]()
    val timeRegular = false // getTimeAxisRegularity( files.head )
    val firstPass = for (iFile <- files.indices; file = files(iFile)) yield {
      try {
        val t0 = System.nanoTime()
        val fileHeader = FileHeader(file, timeRegular)
        val t1 = System.nanoTime()
        println(
          "Worker[%d]: Processing file[%d] '%s', start = %s, ncoords = %d, time = %.4f "
            .format(workerIndex,
              iFile,
              file,
              fileHeader.startDate,
              fileHeader.nElem,
              (t1 - t0) / 1.0E9))
        if ((iFile % 5) == 0) runtime.printMemoryUsage(logger)
        Some(fileHeader)
      } catch {
        case err: Exception =>
          logger.error(
            "Worker[%d]: Encountered error Processing file[%d] '%s': '%s'"
              .format(workerIndex, iFile, file, err.toString))
          if ((iFile % 10) == 0) {
            logger.error(err.getStackTrace.mkString("\n"))
          }
          retryFiles += file; None
      }
    }
    val secondPass =
      for (iFile <- retryFiles.indices; file = retryFiles(iFile)) yield {
        println(
          "Worker[%d]: Reprocessing file[%d] '%s'"
            .format(workerIndex, iFile, file))
        FileHeader(file, timeRegular)
      }
    firstPass.flatten ++ secondPass
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

  def getTimeValues(ncDataset: NetcdfDataset, coordAxis: VariableDS, start_index: Int = 0, end_index: Int = -1, stride: Int = 1): (Array[Long], Array[Double]) = {
    val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(ncDataset, coordAxis, new Formatter())
    val timeCalValues: List[CalendarDate] = timeAxis.getCalendarDates.toList
    val bounds: Array[Double] = ((0 until timeAxis.getShape(0)) map (index => timeAxis.getCoordBoundsDate(index) map (_.getMillis / 1000.0))).toArray.flatten
    ( timeCalValues.map(_.getMillis / 1000 ).toArray, bounds )
  }


  def getTimeCoordValues(ncDataset: NetcdfDataset): (Array[Long], Array[Double]) = {
    val result = Option(ncDataset.findCoordinateAxis(AxisType.Time)) match {
      case Some(timeAxis) => getTimeValues(ncDataset, timeAxis)
      case None => throw new Exception( "ncDataset does not have a time axis: " + ncDataset.getReferencedFile.getLocation )
    }
    result
  }
}

class DatasetFileHeaders(val aggDim: String, val aggFileMap: Seq[FileHeader]) {
  def getNElems: Int = {
    assert( aggFileMap.nonEmpty, "Error, aggregated dataset has no files!")
    aggFileMap.head.nElem
  }
  def getAggAxisValues: Array[Long] =
    aggFileMap.foldLeft(Array[Long]()) { _ ++ _.axisValues }
}

class FileHeader(val filePath: String,
                 val axisValues: Array[Long],
                 val boundsValues: Array[Double],
                 val timeRegular: Boolean) {
  def nElem: Int = axisValues.length
  def startValue: Long = axisValues.headOption.getOrElse(Long.MinValue)
  def startDate: String = CalendarDate.of(startValue).toString
  override def toString: String = " *** FileHeader { path='%s', nElem=%d, startValue=%d startDate=%s} ".format(filePath, nElem, startValue, startDate)
}

object FileMetadata extends Loggable {
  def apply(file: URI): FileMetadata = {
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
  val dimNames: List[String] = dimensions.map(NCMLWriter.getName(_))
  def close = ncDataset.close()
  def getCoordinateAxis(name: String): Option[nc2.dataset.CoordinateAxis] = coordinateAxes.find(p => NCMLWriter.getName(p).equalsIgnoreCase(name))

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
    NCMLWriter.extractSubCollections( collectionId, pathFile.toPath, optionMap.toMap )
  }
}

//object CDMultiScan extends Loggable {
//  def main(args: Array[String]) {
//    if( args.length < 1 ) { println( "Usage: 'mkcolls <collectionsMetaFile>'"); return }
//    EDASLogManager.isMaster
//    val collectionsMetaFile = new File(args(0))    // If first col == 'mult' then each subdir is treated as a separate collection.
//    if( !collectionsMetaFile.isFile ) { throw new Exception("Collections file does not exits: " + collectionsMetaFile.toString) }
//    val ncmlDir = NCMLWriter.getCachePath("NCML").toFile
//    ncmlDir.mkdirs
//    NCMLWriter.generateNCMLFiles( collectionsMetaFile )
//  }
//}


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
//    val ncmlFile = NCMLWriter.getCachePath("NCML").resolve(subCollectionId + ".ncml").toFile
//    if ( ncmlFile.exists ) { throw new Exception("Collection already exists, defined by: " + ncmlFile.toString) }
//    logger.info(s"Creating NCML file for collection ${collectionId} from path ${pathFile.toString}")
//    ncmlFile.getParentFile.mkdirs
//    val ncmlWriter = NCMLWriter(pathFile)
//    val variableMap = new collection.mutable.HashMap[String,String]()
//    val varNames: List[String] = ncmlWriter.writeNCML(ncmlFile)
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
//    val ncmlDir = NCMLWriter.getCachePath("NCML").toFile
//    ncmlDir.mkdirs
//    NCMLWriter.updateNCMLFiles( collectionsMetaFile, ncmlDir )
//  }
//}

object CDScanTest {
  def main(args: Array[String]) {
    val collectionId = "MERRA2-daily-test1"
    val dataPath = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/JAN"
    val pathFile = new File(dataPath)
    NCMLWriter.extractSubCollections(collectionId, pathFile.toPath )
  }
}


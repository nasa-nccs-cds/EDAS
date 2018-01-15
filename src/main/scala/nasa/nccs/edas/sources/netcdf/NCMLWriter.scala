package nasa.nccs.edas.sources.netcdf

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.net.URI
import java.nio.file.{Path, Paths}

import nasa.nccs.cdapi.tensors.CDDoubleArray
import ucar.{ma2, nc2}
import nasa.nccs.edas.sources.{Aggregation, Collections, FileHeader, FileMetadata}
import nasa.nccs.utilities.{Loggable, XMLParser, cdsutils}
import org.apache.commons.lang.RandomStringUtils
import ucar.nc2.Group
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by tpmaxwel on 1/14/18.
  */
object NCMLWriter extends Loggable {
  val ncExtensions = Seq( "nc", "nc4")
  val colIdSep = "."

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

  def isInt( ival: String ): Boolean = try { ival.toInt; true } catch { case ex: Throwable => false }

  def generateNCMLFiles( collectionsFile: File ): Unit = {
    logger.info(s"Generate NCML file from specs in " + collectionsFile.getAbsolutePath )
    for (line <- Source.fromFile( collectionsFile.getAbsolutePath ).getLines; tline = line.trim; if !tline.isEmpty && !tline.startsWith("#")  ) {
      val mdata = tline.split(",").map(_.trim)
      assert( ((mdata.length == 4) && isInt(mdata(0)) && (new File(mdata(3))).exists ), s"Format error in Collections csv file, columns = { depth: Int, template: RegEx, CollectionId: String, rootCollectionPath: String }, incorrect line: { $tline }" )
      extractAggregations( mdata(2), Paths.get( mdata(3) ), Map( "depth" -> mdata(0), "template" -> mdata(1) ) )
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

  def extractAggregations(collectionId: String, dataLocation: Path, options: Map[String,String] = Map.empty ): Unit = {
    assert( dataLocation.toFile.exists, s"Data location ${dataLocation.toString} does not exist:")
//    logger.info(s" %C% Extract collection $collectionId from " + dataLocation.toString)
    val ncSubPaths = recursiveListNcFiles(dataLocation)
    FileHeader.factory( ncSubPaths.map( relFilePath => dataLocation.resolve(relFilePath).toFile.getCanonicalPath ) )
    val bifurDepth: Int = options.getOrElse("depth","0").toInt
    val nameTemplate: Regex = options.getOrElse("template",".*").r
    var subColIndex: Int = 0
    val agFormat = "ag1"
    val varMap: Seq[(String,String)] = getPathGroups(dataLocation, ncSubPaths, bifurDepth, nameTemplate ) flatMap { case (group_key, (subCol_name, files)) =>
      val aggregationId = collectionId + "-" + { if( subCol_name.trim.isEmpty ) { group_key } else subCol_name }
//      logger.info(s" %X% extract Aggregations($collectionId)-> group_key=$group_key, aggregatoinId=$aggregatoinId, files=${files.mkString(";")}" )
      val varNames = Aggregation.write(aggregationId, files.map(fp => dataLocation.resolve(fp).toString), agFormat )
      varNames.map(vname => vname -> aggregationId)
    }
//    logger.info(s" %C% extract Aggregations varMap: " + varMap.map(_.toString()).mkString("; ") )
    val contextualizedVarMap: Seq[(String,String)] = varMap.groupBy { _._1 } .values.map( scopeRepeatedVarNames ).toSeq.flatten
    writeCollection( collectionId, Map( contextualizedVarMap:_* ), agFormat )
    FileHeader.clearCache
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
    relFilePaths.foreach(df => groupMap.getOrElseUpdate(getPathKey(rootPath, df, bifurDepth), mutable.ListBuffer.empty[Path]) += df)
//    logger.info(s" %X% relFilePaths: \n\t ----> ${groupMap.mapValues(_.map(_.toString).mkString("[",",","]")).mkString("\n\t ----> ")} " )
    if( bifurDepth == 0 ) {
      groupMap.mapValues(df => (getSubCollectionName(df), df.toArray)).toSeq
    } else {
      val unsimplifiedResult = groupMap.toSeq map { case (groupKey, grRelFilePaths) =>
        val paths: Iterable[ Seq[String] ] = grRelFilePaths.map( df => df.subpath( 0, bifurDepth).map(_.toString).toSeq )
        val collIdNames: Seq[String] = extractCommonElements( paths )
        val result = ( groupKey, ( collIdNames, grRelFilePaths.toArray)  )
        result
      }
      val filteredColIds: IndexedSeq[String] = filterCommonElements( unsimplifiedResult.map( _._2._1) ).map( _.mkString(colIdSep) ).toIndexedSeq
      unsimplifiedResult.zipWithIndex  map { case (elem, index) => ( elem._1, ( filteredColIds(index), elem._2._2 ) ) }
    }
  }

  def extractCommonElements( paths: Iterable[ Seq[String] ] ): Seq[String] = paths.head.filter( elem => paths.forall( _.contains(elem) ) )

  def filterCommonElements( paths: Iterable[ Seq[String] ] ): Iterable[ Seq[String] ] = {
    val commonElements: Seq[String]  = extractCommonElements( paths )
    paths.map( _.filterNot( elem => commonElements.contains(elem) ) )
  }

//  def trimCommonNameElements( paths: Iterable[ Seq[String] ], prefix: Boolean ): Iterable[ Seq[String] ] = {
//    val pathElements: Iterable[Seq[String]] = paths.map( _.iterator().map(_.toString).toSeq )
//    if(  pathElements.groupBy { if(prefix) _.head else _.last }.size == 1 ) {
//      trimCommonNameElements( pathElements.map { if(prefix) _.drop(1) else _.dropRight(1) }, prefix )
//    } else { paths }
//  }
//
//  def extractCommonPrefix( pathElements: Iterable[Seq[String]], commonPrefixElems: Seq[String] = Seq.empty ): Seq[String] = if( pathElements.size < 2 ) {
//    commonPrefixElems
//  } else {
////    logger.info(s" %ECP% ExtractCommonPrefix --> pathElements:  [ ${pathElements.map(_.mkString(":")).mkString("; ")} ] ,  commonPrefixElems: [ ${commonPrefixElems.mkString("; ")} ]  ")
//    if( pathElements.groupBy( _.headOption.getOrElse( RandomStringUtils.random( 6, true, true ) ) ).size == 1 ) {
//      extractCommonPrefix( pathElements.map( _.drop(1) ),  commonPrefixElems ++ Seq( pathElements.head.head ) )
//    } else if( commonPrefixElems.isEmpty ) { Seq( extractCommonString( pathElements ) ) } else { commonPrefixElems }
//  }
//
//  def extractCommonString( pathElements: Iterable[Seq[String]] ): String = commonPrefix( pathElements.map( _.mkString("~") ).toSeq )
//
//  def commonPrefix( elems: Seq[String] ): String = {
//    val result = elems.foldLeft("")((_,_) => (elems.min.view,elems.max.view).zipped.takeWhile(v => v._1 == v._2).unzip._1.mkString)
//    logger.info(s" %ECP% commonPrefix: ${elems.mkString(", ")}; result = ${result}" )
//    result
//  }
//
//  def trimCommonNameElements( paths: Iterable[Path] ): Iterable[Path] =
//    trimCommonNameElements( trimCommonNameElements( paths.map( _.iterator().map(_.toString).toSeq ) ,false ), true ).map( seq => Paths.get( seq.mkString("/") ) )

  def getSubCollectionName( paths: Iterable[Path] ): String = extractCommonElements( paths.map( _.iterator().flatMap( _.toString.split("[_.-]")).toSeq ) ).mkString(".")

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

  def getPathKey( rootPath: Path, relFilePath: Path, bifurDepth: Int ): String = {
    val fileHeader = FileHeader( rootPath.resolve(relFilePath).toFile, false )
    Seq( getRelPathKey(relFilePath, bifurDepth), Option( fileHeader.varNames.mkString( "." ) ) ).flatten.mkString("-")
  }

  def getRelPathKey( relFilePath: Path, bifurDepth: Int ): Option[String] = try {
    if( bifurDepth < 1 ) { None } else { Option( relFilePath.subpath(0, bifurDepth).mkString(".") ) }
  } catch { case err: Exception =>
    logger.error(  s" Can't get subpath of length $bifurDepth from relPath ${relFilePath.toString}")
    Option( relFilePath.mkString(".") )
  }


  def writeCollection(collectionId: String, variableMap: Map[String,String], agFormat: String ): Unit = {
    val dirFile = Collections.getCachePath("NCML").resolve(collectionId + ".csv").toFile
    logger.info( s"Generating Collection ${dirFile.toString} from variableMap: \n\t" + variableMap.mkString(";\n\t") )
    val pw = new PrintWriter( dirFile )
    variableMap foreach { case ( varName, aggregation ) =>
      val agFile = Collections.getCachePath("NCML").resolve( aggregation + "." + agFormat ).toString
      pw.write(s"$varName, ${agFile}\n")
    }
    pw.close
  }



  def isNcFile(file: File): Boolean = {
    file.isFile && isNcFileName(file.getName.toLowerCase)
  }

  def getNcURIs(file: File): Iterable[URI] = {
    if (Collections.isCollectionFile(file)) {
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
}

class NCMLWriter(fileHeaders: IndexedSeq[FileHeader], val maxCores: Int = 8)  extends Loggable {
  import NCMLWriter._
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors, maxCores )
  private val nFiles = fileHeaders.length
  val outerDimensionSize: Int = fileHeaders.foldLeft(0)(_ + _.nElem)
  val ignored_attributes = List("comments")
  val overwriteTime = fileHeaders.length > 1

  def uriToString( uri: URI ): String = {
    if( uri.getScheme.equals("file") || uri.getScheme.isEmpty ) { uri.getPath }
    else { uri.toString }
  }

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

//  def getTimeSpecs: Option[(Long, Long)] = {
//    val t0 = fileHeaders.head.startValue
//    val dt = if (fileHeaders.head.nElem > 1) {
//      fileHeaders.head.axisValues(1) - fileHeaders.head.axisValues(0)
//    } else { fileHeaders(1).startValue - fileHeaders(0).startValue }
//    Some(t0 -> dt)
//  }

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
    <aggregation dimName={getTimeVarName(fileMetadata: FileMetadata)} type="joinExisting">
      { for (fileHeader <- fileHeaders) yield {
      getAggDataset(fileHeader, timeRegular)
    }  }
    </aggregation>
  }

  def findTimeVariable(fileMetadata: FileMetadata): Option[nc2.Variable] =
    fileMetadata.coordVars find (fileMetadata.getAxisType(_) == AxisType.Time)

  def getNCMLVerbose: ( List[String], xml.Node ) = {
    val fileMetadata = FileMetadata(fileHeaders.head.filePath)
    try {
      logger.info(s"\n\n -----> FileMetadata: variables = ${fileMetadata.variables.map(_.getShortName).mkString(", ")}\n\n")
      val timeRegularSpecs = None //  getTimeSpecs( fileMetadata )
      logger.info("Processing %d files with %d workers".format(nFiles, nReadProcessors))
      val result = <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
        <explicit/>
        <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
            {for (attribute <- fileMetadata.attributes) yield getAttribute(attribute)}
            {(for (coordAxis <- fileMetadata.coordinateAxes) yield getDimension(coordAxis)).flatten}
            {for (variable <- fileMetadata.coordVars) yield getVariable(fileMetadata, variable, timeRegularSpecs)}
            {for (variable <- fileMetadata.variables) yield getVariable(fileMetadata, variable, timeRegularSpecs)}
            {getAggregation(fileMetadata, timeRegularSpecs.isDefined)}
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
    bw.write( XMLParser.serialize(result).toString )
    bw.close()
    varNames
  }
}
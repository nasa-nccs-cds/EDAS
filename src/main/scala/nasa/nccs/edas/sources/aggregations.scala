package nasa.nccs.edas.sources

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.Date

import nasa.nccs.edas.sources.netcdf.NCMLWriter
import nasa.nccs.esgf.process.CDSection
import nasa.nccs.utilities.{EDTime, Loggable}
import org.apache.commons.lang.RandomStringUtils
import ucar.nc2
import ucar.nc2.time.CalendarDate
import ucar.ma2

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

case class FileInput(fileIndex: Int, startTime: Long, firstRowIndex: Int, nRows: Int, path: String ) extends Serializable {
  def lastRowIndex = firstRowIndex + nRows - 1
  def getRowIndexRange: ma2.Range = new ma2.Range( firstRowIndex, firstRowIndex + nRows - 1 )
  def intersects( row_index_range: ma2.Range ) = getRowIndexRange.intersects( row_index_range )
  def intersect( row_index_range: ma2.Range ): ma2.Range = getRowIndexRange.intersect( row_index_range )
  override def toString = s"FileInput(${fileIndex})[ ${path}, firstRow: ${firstRowIndex}, nRows: ${nRows}, time: ${startTime} ]"
}

case class Variable( name: String, shape: Array[Int], dims: String, units: String ) extends Serializable {
  def toXml: xml.Elem = { <variable name={name} shape={shape.mkString(",")} dims={dims} units={units} /> }
  override def toString: String = s"name:${name};shape:${shape.mkString(",")};dims:${dims};units:${units}"
  def toMap: Map[String,String] = Seq( "name"->name, "shape"->shape.mkString(","), "dims"->dims, "units"->units ).toMap
}
case class Coordinate( name: String, shape: Array[Int], dims: String="", units: String="" ) extends Serializable
case class Axis( name: String, ctype: String, shape: Array[Int], units: String, minval: Float, maxval: Float ) extends Serializable

object AggregationWriter extends Loggable {
  val ncExtensions = Seq( "nc", "nc4")
  val colIdSep = "."

  def getName(node: nc2.CDMNode): String = node.getShortName

  def backup( dir: File, backupDir: File ): Unit = {
    backupDir.mkdirs()
    for( f <- backupDir.listFiles ) { f.delete() }
    for( f <- dir.listFiles ) { f.renameTo( new File( backupDir, f.getName ) ) }
  }

  def isInt( ival: String ): Boolean = try { ival.toInt; true } catch { case ex: Throwable => false }

  def generateAggregations(collectionsFile: File ): Unit = {
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

  def extractAggregations(collectionId: String, dataPath: Path, options: Map[String,String] = Map.empty ): Unit = {
    val dataLocation: Path = if(dataPath.toFile.exists) { dataPath } else { Paths.get( new java.io.File(".").getCanonicalPath, dataPath.toString ) }
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
      val fileHeaders = Aggregation.write(aggregationId, files.map(fp => dataLocation.resolve(fp).toString), agFormat )
      val writer = new NCMLWriter( aggregationId, fileHeaders )
      val varNames = writer.writeNCML( Collections.getAggregationPath.resolve(aggregationId + ".ncml").toFile )
      varNames.map(vname => vname -> aggregationId)
    }
    //    logger.info(s" %C% extract Aggregations varMap: " + varMap.map(_.toString()).mkString("; ") )
    val contextualizedVarMap: Seq[(String,String)] = varMap.groupBy { _._1 } .values.map( scopeRepeatedVarNames ).toSeq.flatten
    addAggregations( collectionId, Map( contextualizedVarMap:_* ), agFormat )
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


  def addAggregations(collectionId: String, variableMap: Map[String,String], agFormat: String ): Unit = {
    val dirFile = Collections.getAggregationPath.resolve(collectionId + ".csv").toFile
    logger.info( s"Generating Collection ${dirFile.toString} from variableMap: \n\t" + variableMap.mkString(";\n\t") )
    val pw = new PrintWriter( dirFile )
    variableMap foreach { case ( varName, aggregation ) =>
      val agFile = Collections.getAggregationPath.resolve( aggregation + "." + agFormat ).toString
      pw.write(s"$varName, ${agFile}\n")
    }
    pw.close
  }

  def isNcFileName(fName: String): Boolean = {
    val fname = fName.toLowerCase;
    fname.endsWith(".nc4") || fname.endsWith(".nc") || fname.endsWith(".hdf") || fname.endsWith(".ncml")
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
        Seq(file) ++: children.flatMap(getNcFiles) filter isNcFile
      }
    } catch {
      case err: NullPointerException =>
        logger.warn("Empty collection directory: " + file.toString)
        Iterable.empty[File]
    }
  }

  def getNcFiles(args: Iterator[File]): Iterator[File] =
    args.map((arg: File) => getNcFiles(arg)).flatten
  def getNcURIs(args: Iterator[File]): Iterator[URI] =
    args.map((arg: File) => getNcURIs(arg)).flatten
}

object BoundedIndex {
  val InRange = 0
  val AboveRange = 1
  val BelowRange = -1
  val RangeStart = 2
  val RangeEnd = 3
}
case class BoundedIndex( index: Long, boundsStatus: Int ) {
  def isBelowRange: Boolean = boundsStatus ==  BoundedIndex.BelowRange
  def isAboveRange: Boolean = boundsStatus ==  BoundedIndex.AboveRange
}

case class TimeRange( firstValue: Long, lastValue: Long, firstRow: Int, nRows: Int, boundsStatus: Int ) {
  import BoundedIndex._
  val time_duration = lastValue - firstValue
  def dt = time_duration / nRows.toFloat

  def toRowIndex( time_value: Long, range_position: Int ): BoundedIndex = boundsStatus match {
    case BoundedIndex.InRange =>
      val r0 = (time_value - firstValue)/dt
      val rval = if( range_position == RangeStart ) { r0 + 0.5 } else { r0 - 0.5 }
      BoundedIndex( firstRow + rval.toLong, boundsStatus )
    case BoundedIndex.AboveRange =>
      BoundedIndex( firstRow, boundsStatus)
    case BoundedIndex.BelowRange =>
      BoundedIndex( 0, boundsStatus )
  }

//  def toRowIndex( time_value: Long ): BoundedIndex = boundsStatus match {
//    case BoundedIndex.InRange =>
//      val tv = time_value - firstValue
//      val frow = tv/dt
//      val rowv = Math.floor(frow).toInt
//      val ( tm, t0, tp ) = (  (rowv-0.5)*dt,  (rowv)*dt,  (rowv+0.5)*dt )
//      BoundedIndex( (firstRow+rowv).toLong, boundsStatus)
//    case BoundedIndex.AboveRange =>
//      BoundedIndex( firstRow, boundsStatus)
//    case BoundedIndex.BelowRange =>
//      BoundedIndex( 0, boundsStatus )
//  }

  def toTimeValue( row_index: Int ): BoundedIndex = boundsStatus match {
    case BoundedIndex.InRange => BoundedIndex( (firstValue + (row_index - firstRow + 0.5) * dt).toLong, boundsStatus)
    case BoundedIndex.AboveRange => BoundedIndex( lastValue, boundsStatus)
    case BoundedIndex.BelowRange => BoundedIndex( firstValue, boundsStatus )
  }

}

case class Aggregation( dataPath: String, files: Array[FileInput], variables: List[Variable], coordinates: List[Coordinate], axes: List[Axis], parms: Map[String,String] ) extends Loggable {
  val time_start: Long = EDTime.toMillis( parms.getOrElse("time.start", throw new Exception("Aggregation file format error; missing 'time.start' parameter")).toDouble )
  val time_end: Long = EDTime.toMillis( parms.getOrElse("time.end", throw new Exception("Aggregation file format error; missing 'time.end' parameter")).toDouble )
  val time_nrows: Int = parms.getOrElse("time.nrows", throw new Exception("Aggregation file format error; missing 'time.nrows' parameter")).toInt
  val time_duration = time_end - time_start
  val dt: Long = time_duration/time_nrows
  val ave_file_dt: Long = time_duration/files.length
  val ave_file_nrows: Long = time_nrows/files.length
  def findVariable( varName: String ): Option[Variable] = variables.find( _.name.equals(varName) )
  def id: String = { new File(dataPath).getName }
  def gridFilePath: String = getRelatedFile( "nc")
  def ncmlFilePath: String = getRelatedFile( "ncml")
  def getFilebase: FileBase = new FileBase( files, parms.get("base.path") )
  def toXml: xml.Elem = { <aggregation id={id}> { variables.map( _.toXml ) } </aggregation> }

  private def _estimate_file_index_from_time_value( time_value: Long ): Int = ( ( time_value - time_start ) / ave_file_dt ).toInt
  private def _estimate_file_index_from_row_index( row_index: Int ): Int = ( row_index / ave_file_nrows ).toInt

  def getRelatedFile( extension: String ): String = {
    val toks: Array[String] = dataPath.split('.').dropRight(1)
    val toks_ext: Array[String] = if( toks.last.equals( extension ) ) { toks } else { toks :+ extension }
    toks_ext.mkString(".")
  }

  private def _fileInputsFromTimeValue( time_value: Long, estimated_file_index: Int ): TimeRange = {
    if( time_value < time_start ) { return  TimeRange( time_start, time_start, 0, 0, BoundedIndex.BelowRange )  }
    if( time_value >= time_end ) { return TimeRange( time_end, time_end, time_nrows-1, 0, BoundedIndex.AboveRange ) }
    val file0 = files( estimated_file_index )
    if (time_value < file0.startTime) { return _fileInputsFromTimeValue(time_value, estimated_file_index - 1) }
    if( estimated_file_index == ( files.length - 1 ) ) {
      TimeRange(file0.startTime, time_end, file0.firstRowIndex, file0.nRows, BoundedIndex.InRange )
    } else {
      val file1 = files(estimated_file_index + 1)
      if (time_value >= file1.startTime) { return _fileInputsFromTimeValue(time_value, estimated_file_index + 1) }
//      logger.info( s" MappingTimeValue: estimated_file_index=${estimated_file_index} startTime=${file0.startTime} timeRange=[${time_start},${time_end}], row=${file0.firstRowIndex} date=${CalendarDate.of(file1.startTime).toString}")
      TimeRange(file0.startTime, file1.startTime, file0.firstRowIndex, file0.nRows, BoundedIndex.InRange)
    }
  }

  private def _fileInputsFromRowIndex( row_index: Int, estimated_file_index: Int ): TimeRange = {
    if( row_index < 0 ) { return  TimeRange( time_start, time_start, 0, 0, BoundedIndex.BelowRange )  }
    if( row_index >= time_nrows ) { return TimeRange( time_end, time_end, time_nrows-1, 0, BoundedIndex.AboveRange  ) }
    val file0 = files( estimated_file_index )
    if (row_index < file0.firstRowIndex) { return _fileInputsFromRowIndex(row_index, estimated_file_index - 1) }
    if( estimated_file_index == ( files.length - 1 ) ) {
      TimeRange(file0.startTime, time_end, file0.firstRowIndex, file0.nRows, BoundedIndex.InRange)
    } else {
      val file1 = files(estimated_file_index + 1)
      if (row_index >= file1.firstRowIndex ) { return _fileInputsFromRowIndex(row_index, estimated_file_index + 1) }
      TimeRange(file0.startTime, file1.startTime, file0.firstRowIndex, file0.nRows, BoundedIndex.InRange)
    }
  }

  def fileInputsFromTimeValue( time_value: Long ): TimeRange = _fileInputsFromTimeValue( time_value, _estimate_file_index_from_time_value(time_value) )
  def fileInputsFromRowIndex( row_index: Int ): TimeRange = _fileInputsFromRowIndex( row_index, _estimate_file_index_from_row_index(row_index) )
  def toTimeValue( row_index: Int ): BoundedIndex = fileInputsFromRowIndex( row_index ).toTimeValue( row_index )

  def findRowIndicesFromCalendarDates( start_date: CalendarDate, end_date: CalendarDate): Option[ ( Int, Int ) ] = {
    val ( t0, t1 ) = ( start_date.getMillis, end_date.getMillis )
    val startIndex: BoundedIndex = fileInputsFromTimeValue( t0 ).toRowIndex( t0, BoundedIndex.RangeStart )
    val endIndex: BoundedIndex = fileInputsFromTimeValue( t1 ).toRowIndex( t1, BoundedIndex.RangeEnd )
    if( endIndex.isBelowRange || startIndex.isAboveRange ) { None }
    else {
//      logger.info( s" @@@ FindRowIndicesFromCalendarDates: startRow=${startIndex.index.toInt} endRow=${endIndex.index.toInt}")
      Some((startIndex.index.toInt, endIndex.index.toInt ))
    }
  }

  def getBasePath: Option[String] = parms.get("base.path")

  def getFilePath( fileInput: FileInput ): String = getBasePath.fold( fileInput.path )( basePath => Paths.get( basePath, fileInput.path ).toString )

  def getRangeMap(time_index: Int = 0, fileInputs: Array[FileInput] = files, rangeMap: List[(ma2.Range,FileInput)] = List.empty[(ma2.Range,FileInput)]  ): List[(ma2.Range,FileInput)] = {
    if( fileInputs.isEmpty ) { rangeMap } else { getRangeMap(time_index + fileInputs.head.nRows + 1, fileInputs.tail,  rangeMap ++ List( new ma2.Range( time_index, time_index + fileInputs.head.nRows ) -> fileInputs.head ) ) }
  }

  def getIntersectingFiles( sectionString: String ): Array[FileInput] = CDSection.fromString(sectionString).map( _.toSection.getRange(0) ).fold( files )( getIntersectingFiles )
  def getIntersectingFiles( timeRange: ma2.Range ): Array[FileInput] =  files.filter( _.intersects(timeRange) )

}

class FileBase( val files: Array[FileInput], val optBasePath: Option[String] ) extends Loggable with Serializable {
  val nFiles = files.length
  val dt: Float = ( files.last.startTime - files.head.startTime ) / ( nFiles - 1 ).toFloat
  val startTime = files.head.startTime
  def getIndexEstimate( timestamp: Long ): Int = Math.round( ( timestamp - startTime ) / dt )
  def getFileInput( timestamp: Long ): FileInput = _getFileInput( timestamp, getIndexEstimate(timestamp) )
  def getFilePath( fileInput: FileInput ): String = optBasePath.fold( fileInput.path )( basePath => Paths.get( basePath, fileInput.path ).toString )

  private def _getFileInput( timestamp: Long, indexEstimate: Int ): FileInput = {
    if( indexEstimate < 0 ) { return files(0) }
    if( indexEstimate >= files.length ) { return files.last }
    val fileStartTime = files( indexEstimate ).startTime
    val fileStartDate = new Date( fileStartTime ).toString
    val tsDate = new Date( timestamp ).toString
    if( timestamp < fileStartTime ) { return _getFileInput(timestamp,indexEstimate-1) }
    if( indexEstimate >= nFiles-1) { return files.last }
    val fileEndTime = files( indexEstimate+1 ).startTime
    if( timestamp < fileEndTime ) { return  files( indexEstimate ) }
    return _getFileInput( timestamp, indexEstimate + 1)
  }
}

object Aggregation extends Loggable {

  def read(aggFile: String): Aggregation = {
    val source = Source.fromFile(aggFile)
    var files: mutable.ArrayBuffer[FileInput] = null
    val variables = mutable.ListBuffer.empty[Variable]
    val coordinates = mutable.ListBuffer.empty[Coordinate]
    val axes = mutable.ListBuffer.empty[Axis]
    val parameters = mutable.HashMap.empty[String,String]
    var timeIndex = 0
    try {
      for (line <- source.getLines; toks = line.split(';').map(_.trim) ) try{ toks(0) match {
        case "F" =>
          assert( files != null, s"Missing or misordered 'num.files' parameter in Aggregation file: ${aggFile}")
          val nTS = toks(2).toInt
          files += FileInput(files.length, EDTime.toMillis(toks(1).toDouble), timeIndex, nTS, toks(3) )
          timeIndex += nTS
        case "P" =>
          parameters += toks(1) -> toks(2)
          if( toks(1).equals("num.files") ) { files = new mutable.ArrayBuffer[FileInput]( toks(2).toInt ) }
        case "V" => variables += Variable( toks(1), toks(2).split(",").map( toInt ), toks(3), toks(4) )
        case "C" => coordinates += Coordinate( toks(1), toks(2).split(",").map( toInt ) )
        case "A" => axes += Axis( toks(1), toks(2), toks(3).split(",").map( toInt ), toks(4), toFloat(toks(5)), toFloat(toks(6)) )
        case _ => Unit
      } } catch {
        case err: Exception =>
          logger.error( s"Error '${err.toString}' processing line in Aggregation file => ${line} " )
          logger.error( err.getStackTrace.map( _.toString ).mkString("\n\t") )
      }
    } finally { source.close() }
    Aggregation( aggFile, files.toArray, variables.toList, coordinates.toList, axes.toList, parameters.toMap )
  }

  def toInt( tok: String ): Int = if( tok.isEmpty ) { 0 } else { tok.toInt }
  def toFloat( tok: String ): Float = if( tok.isEmpty ) { 0 } else { tok.toFloat }

  def write( aggregationId: String, files: IndexedSeq[String], format: String = "ag1" ): IndexedSeq[FileHeader] = {
    try {
      val fileHeaders = FileHeader.getFileHeaders( files, false )
      if( !format.isEmpty ) { writeAggregation(  Collections.getAggregationPath.resolve(aggregationId + "." + format).toFile, fileHeaders, format ) }
      fileHeaders
    } catch {
      case err: Exception =>
        logger.error( s"Error writing aggregation ${aggregationId}: ${err.getMessage}")
        IndexedSeq.empty[FileHeader]
    }
  }

  def writeAggregation( aggFile: File,  fileHeaders: IndexedSeq[FileHeader], format: String, maxCores: Int = 8 ): Unit = {
    logger.info(s"Writing Aggregation[$format] File: " + aggFile.toString)
    val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors, maxCores )
    logger.info("Processing %d files with %d workers".format(fileHeaders.length, nReadProcessors))
    val bw = new BufferedWriter(new FileWriter(aggFile))
    val fileMetadata = FileMetadata( fileHeaders.head.filePath )
    val ( basePath, reducedFileheaders ) = FileHeader.extractSubpath( fileHeaders )
    try {
      bw.write( s"P; time.nrows; ${fileHeaders.length}\n")
      bw.write( s"P; time.start; ${fileHeaders.head.startValue}\n")
      bw.write( s"P; time.end; ${fileHeaders.last.endValue}\n")
      bw.write( s"P; base.path; $basePath\n")
      bw.write( s"P; num.files; ${reducedFileheaders.length}\n")
      for (attr <- fileMetadata.attributes ) { bw.write( s"P; ${attr.getFullName}; ${attr.getStringValue} \n") }
      for (coordAxis <- fileMetadata.coordinateAxes; ctype = coordAxis.getAxisType.getCFAxisName ) {
        if(ctype.equals("Z") ) {  bw.write( s"A; ${coordAxis.getShortName}; ${ctype}; ${coordAxis.getShape.mkString(",")}; ${coordAxis.getUnitsString};  ${coordAxis.getMinValue}; ${coordAxis.getMaxValue}\n") }
        else {                    bw.write( s"A; ${coordAxis.getShortName}; ${ctype}; ${coordAxis.getShape.mkString(",")}; ${coordAxis.getUnitsString};  ${coordAxis.getMinValue}; ${coordAxis.getMaxValue}\n" ) }
      }
      for (cVar <- fileMetadata.coordVars) { bw.write( s"C; ${cVar.getShortName};  ${cVar.getShape.mkString(",")} \n" ) }
      for (variable <- fileMetadata.variables) { bw.write( s"V; ${variable.getShortName};  ${variable.getShape.mkString(",")};  ${variable.getDimensionsString};  ${variable.getUnitsString} \n" ) }
      for (fileHeader <- reducedFileheaders) {
        bw.write( s"F; ${EDTime.toString(fileHeader.startValue)}; ${fileHeader.nElem.toString}; ${fileHeader.filePath}\n" )
      }
    } finally {
      fileMetadata.close
    }
    bw.close()
  }
}



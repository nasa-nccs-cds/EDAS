package nasa.nccs.edas.sources

import java.io.{BufferedWriter, File, FileWriter}
import nasa.nccs.edas.sources.netcdf.NCMLWriter
import nasa.nccs.utilities.Loggable
import scala.collection.mutable
import scala.io.Source

case class FileInput( index: Int, startTime: Long, nRows: Int, path: String )
case class Variable( name: String, shape: Array[Int], dims: String, units: String )
case class Coordinate( name: String, shape: Array[Int], dims: String, units: String )
case class Axis( name: String, ctype: String, shape: Array[Int], units: String, minval: Float, maxval: Float )
case class Aggregation(  files: List[FileInput], variables: List[Variable], coordinates: List[Coordinate], aces: List[Axis], parms: Map[String,String] )

object Aggregation extends Loggable {

  def read(aggFile: String): Aggregation = {
    val source = Source.fromFile(aggFile)
    val files = mutable.ListBuffer.empty[FileInput]
    val variables = mutable.ListBuffer.empty[Variable]
    val coordinates = mutable.ListBuffer.empty[Coordinate]
    val axes = mutable.ListBuffer.empty[Axis]
    val parameters = mutable.HashMap.empty[String,String]
    try {
      for (line <- source.getLines; toks = line.split(',').map(_.trim) ) toks(0) match {
        case "F" =>  files += FileInput(files.length, toks(1).toLong, toks(2).toInt, toks(3))
        case "P" =>  parameters += toks(1) -> toks(2)
        case "V" => variables += Variable( toks(1), toks(2).split(",").map( _.toInt ), toks(3), toks(4) )
        case "C" => coordinates += Coordinate( toks(1), toks(2).split(",").map( _.toInt ), toks(3), toks(4) )
        case "A" => axes += Axis( toks(1), toks(2), toks(3).split(",").map( _.toInt ), toks(3), toks(4).toFloat, toks(5).toFloat )
      }
    } finally { source.close() }
    Aggregation( files.toList, variables.toList, coordinates.toList, axes.toList, parameters.toMap )
  }


  def write( aggregationId: String, files: IndexedSeq[String], format: String = "ag1" ): List[String] = {
    try {
      val cacheDir = Collections.getCachePath("NCML")
      val fileHeaders = FileHeader.getFileHeaders( files, false )
      if( !format.isEmpty ) { writeAggregation( cacheDir.resolve(aggregationId + "." + format).toFile, fileHeaders, format ) }
      val writer = new NCMLWriter( fileHeaders )
      writer.writeNCML( cacheDir.resolve(aggregationId + ".ncml").toFile )
    } catch {
      case err: Exception =>
        logger.error( s"Error writing aggregation ${aggregationId}: ${err.getMessage}")
        List.empty[String]
    }
  }

  def writeAggregation( aggFile: File,  fileHeaders: IndexedSeq[FileHeader], format: String, maxCores: Int = 8 ): Unit = {
    logger.info(s"Writing Aggregation[$format] File: " + aggFile.toString)
    val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors, maxCores )
    logger.info("Processing %d files with %d workers".format(fileHeaders.length, nReadProcessors))
    val bw = new BufferedWriter(new FileWriter(aggFile))
    val fileMetadata = FileMetadata( fileHeaders.head.filePath )
    val dt: Int = Math.round( ( fileHeaders.last.startValue - fileHeaders.head.startValue ) / ( fileHeaders.length - 1 ).toFloat )
    val ( basePath, reducedFileheaders ) = FileHeader.extractSubpath( fileHeaders )
    try {
      bw.write( s"P; time.step; $dt\n")
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
        bw.write( s"F; ${fileHeader.startValue}; ${fileHeader.nElem.toString}; ${fileHeader.filePath}\n" )
      }
    } finally {
      fileMetadata.close
    }
    bw.close()
  }
}



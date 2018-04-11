package nasa.nccs.edas.sources.netcdf

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.net.URI

import nasa.nccs.cdapi.tensors.CDDoubleArray
import ucar.{ma2, nc2}
import nasa.nccs.edas.sources.{FileHeader, FileMetadata}
import nasa.nccs.utilities.{EDTime, Loggable, XMLParser, cdsutils}
import ucar.nc2.Group
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1D}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._





class NCMLWriter( val aggregationId: String, fileHeaders: IndexedSeq[FileHeader], val maxCores: Int = 8)  extends Loggable {
  private val nReadProcessors = Math.min( Runtime.getRuntime.availableProcessors, maxCores )
  private val nFiles = fileHeaders.length
  val outerDimensionSize: Int = fileHeaders.foldLeft(0)(_ + _.nElem)
  val ignored_attributes = List("comments","time_increment")
  val overwriteTime = fileHeaders.length > 1

  def uriToString( uri: URI ): String = {
    if( uri.getScheme.equals("file") || uri.getScheme.isEmpty ) { uri.getPath }
    else { uri.toString }
  }
  def getName(node: nc2.CDMNode): String = node.getFullName
  def isIgnored(attribute: nc2.Attribute): Boolean = {
    ignored_attributes.contains(getName(attribute))
  }
  def getDimCoordRef(fileMetadata: FileMetadata, dim: nc2.Dimension): String = {
    val dimname = getName(dim)
    fileMetadata.coordVars.map(getName(_)).find(vname => (vname equals dimname) || (vname.split(':')(0) == dimname.split(':')(0))) match {
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
      if (dim.isShared) { dim.getShortName }
      else if (dim.isVariableLength) "*"
      else dim.getLength.toString
    ).toArray.mkString(" ")

//  def getDimensionFromAxis(axis: CoordinateAxis): Option[xml.Node] = {
//    axis match {
//      case coordAxis: CoordinateAxis1D =>
//        val nElems =
//          if (coordAxis.getAxisType == AxisType.Time) outerDimensionSize
//          else coordAxis.getSize
//        val dimension = coordAxis.getDimension(0)
//        val node =  <dimension name={getName(dimension)} length={nElems.toString} isUnlimited={dimension.isUnlimited.toString} isVariableLength={dimension.isVariableLength.toString} isShared={dimension.isShared.toString}/>
//        Some(node)
//      case x =>
//        logger.warn( "This Coord axis type not currently supported: " + axis.getClass.getName + " for axis " + axis.getNameAndDimensions(true) )
//        None
//    }
//  }

  def getDimension(dim: nc2.Dimension): xml.Node = {
      <dimension name={dim.getShortName} length={dim.getLength.toString} isUnlimited={dim.isUnlimited.toString} isVariableLength={dim.isVariableLength.toString} isShared={dim.isShared.toString}/>
  }



  def getAggDatasetTUC(fileHeader: FileHeader,
                       timeRegular: Boolean = false): xml.Node =
      <netcdf location={fileHeader.toPath.toString} ncoords={fileHeader.nElem.toString} timeUnitsChange="true"/>

  def getAggDataset(fileHeader: FileHeader, timeRegular: Boolean = false): xml.Node =
    if (timeRegular || !overwriteTime)
        <netcdf location={fileHeader.toPath.toString} ncoords={fileHeader.nElem.toString}/>
    else
        <netcdf location={fileHeader.toPath.toString} ncoords={fileHeader.nElem.toString} coordValue={fileHeader.axisValues.map( EDTime.toString ).mkString(", ")}/>
  def getDataType( axisType: AxisType ): String = if( axisType == AxisType.Time ) { EDTime.datatype } else { "float" }

  def getVariable(fileMetadata: FileMetadata, variable: nc2.Variable,  timeRegularSpecs: Option[(Double, Double)]): xml.Node = {
    val axisType = fileMetadata.getAxisType(variable)
    <variable name={getName(variable)} shape={getDims(fileMetadata,variable)} type={getDataType(axisType)}> {
        if( axisType == AxisType.Time )  <attribute name="_CoordinateAxisType" value="Time"/>  <attribute name="units" value={if(overwriteTime) EDTime.units else variable.getUnitsString}/>
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
    if( fileHeaders.isEmpty ) {
      logger.warn( s"No files found for agg ${aggregationId}")
      val result = <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" />
      val varNames: List[String] = List.empty
      (varNames, result)
    } else {
      val fileMetadata = FileMetadata( fileHeaders.head.toPath.toString, outerDimensionSize )
      try {
        logger.info(s"\n\n -----> FileMetadata: variables = ${fileMetadata.variables.map(_.getShortName).mkString(", ")}\n\n")
        val timeRegularSpecs = None //  getTimeSpecs( fileMetadata )
        logger.info("Processing %d files with %d workers".format(nFiles, nReadProcessors))
        val result = <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2">
          <explicit/>
          <attribute name="title" type="string" value="NetCDF aggregated dataset"/>
          {for (attribute <- fileMetadata.attributes) yield getAttribute(attribute)}
          {for (dimension <- fileMetadata.dimensions) yield getDimension(dimension)}
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
  }

//  def defineNewTimeVariable: xml.Node =
//    <variable name={getTimeVarName}>
//      <attribute name="units" value={EDTime.units}/>
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
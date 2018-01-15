package nasa.nccs.edas.sources.netcdf

import java.util.{Formatter, Locale}

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap
import nasa.nccs.cdapi.cdm.{CDSVariable, MetaCollectionFile, VariableMetadata, VariableRecord}
import nasa.nccs.cdapi.data.HeapFltArray
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.esgf.process.ContainerOps
import nasa.nccs.utilities.Loggable
import ucar.nc2.constants.AxisType
import ucar.nc2.dataset.{CoordinateAxis1DTime, NetcdfDataset}
import ucar.{ma2, nc2}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
/**
  * Created by tpmaxwel on 1/14/18.
  */
object NetcdfDatasetMgr extends Loggable with ContainerOps  {
    import CDSVariable._
//  NetcdfDataset.initNetcdfFileCache(10,1000,3600)   // Bugs in Netcdf file caching cause NullPointerExceptions on MERRA2 npana datasets (var T): ( 3/3/2017 )
  val datasetCache = new ConcurrentLinkedHashMap.Builder[String, NetcdfDataset].initialCapacity(64).maximumWeightedCapacity(1000).build()
  val MB = 1024*1024
  val formatter = new Formatter(Locale.US)
  def findAttributeValue( attributes: Map[String, nc2.Attribute], keyRegExp: String, default_value: String ): String = filterAttrMap( attributes, keyRegExp.r, default_value )

  def getTimeAxis( dataPath: String ): CoordinateAxis1DTime = {
    val ncDataset: NetcdfDataset = aquireFile( dataPath, 11.toString )
    try {
      getTimeAxis( ncDataset ) getOrElse { throw new Exception( "Can't find time axis in dataset: " + dataPath ) }
    } finally { ncDataset.close() }
  }

  def getTimeAxis( ncDataset: NetcdfDataset ): Option[CoordinateAxis1DTime] = {
    val axes = ncDataset.getCoordinateAxes.toList
    axes.find( _.getAxisType == AxisType.Time ) map { time_axis => CoordinateAxis1DTime.factory(ncDataset, time_axis, formatter ) }
  }

  def getMissingValue(attributes: Map[String, nc2.Attribute]): Float = findAttributeValue(  attributes, "^.*missing.*$", "" ) match {
    case "" =>
      logger.warn( "Can't find missing value, attributes = " + attributes.keys.mkString(", ") )
      Float.MaxValue;
    case s =>
      logger.info( "Found missing attribute value: " + s )
      s.toFloat
  }


  def readVariableData(varShortName: String, dataPath: String, section: ma2.Section): ma2.Array = {
    val ncDataset: NetcdfDataset = openCollection(varShortName, dataPath)
    try {
      readVariableData( varShortName, ncDataset, section ) getOrElse { throw new Exception(s"Can't find variable $varShortName in dataset $dataPath ") }
    } catch {
      case err: Exception =>
        logger.error("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
        logger.error(err.getStackTrace.map(_.toString).mkString("\n"))
        throw err
    } finally {
      ncDataset.close()
    }
  }

  def readVariableData(varShortName: String, ncDataset: NetcdfDataset, section: ma2.Section): Option[ma2.Array] =
    ncDataset.getVariables.toList.find(v => v.getShortName equals varShortName) map { variable =>
//        runtime.printMemoryUsage(logger)
        val ma2array = variable.read(section)
        logger.error("Reading Variable %s, shape: (%s),  section: { o:(%s) s:(%s) }".format( varShortName, variable.getShape.mkString(","), section.getOrigin.mkString(","), section.getShape.mkString(",")) )
        ma2array
    }

  def createVariableMetadataRecord( varShortName: String, dataPath: String ): VariableMetadata = {
    val ncDataset: NetcdfDataset = openCollection( varShortName, dataPath )
    try {
      ncDataset.getVariables.toList.find(v => v.getShortName equals varShortName) match {
        case Some(variable) =>
          try {
            val attributes = Map(variable.getAttributes.toList.map(attr => attr.getShortName -> attr): _*)
            val missing = getMissingValue(attributes)
            val metadata = attributes.mapValues(_.getStringValue).mkString(";")
            new VariableMetadata(variable.getNameAndDimensions, variable.getUnitsString, missing, metadata, variable.getShape)
          } catch {
            case err: Exception =>
              logger.error("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
              logger.error(err.getStackTrace.map(_.toString).mkString("\n"))
              throw err
          }
        case None => throw new Exception(s"Can't find variable $varShortName in dataset $dataPath ")
      }
    } finally { ncDataset.close() }
  }

  def createVariableDataRecord(varShortName: String, dataPath: String, section: ma2.Section): VariableRecord = {
    val ncDataset: NetcdfDataset = openCollection( varShortName, dataPath )
    try {
      ncDataset.getVariables.toList.find(v => v.getShortName equals varShortName) match {
        case Some(variable) =>
          try {
            val t0 = System.nanoTime()
            val ma2array = variable.read(section)
            val axes = ncDataset.getCoordinateAxes
            val coordAxis = Option(ncDataset.findCoordinateAxis(AxisType.Time)).getOrElse(throw new Exception("Can't find time axis in dataset: " + dataPath))
            val timeAxis = CoordinateAxis1DTime.factory(ncDataset, coordAxis, new Formatter())
            val timeIndex = section.getRange(0).first
            val date = timeAxis.getCalendarDate(timeIndex)
            val sample_data = (0 until Math.min(16, ma2array.getSize).toInt) map ma2array.getFloat
            val attributes = Map(variable.getAttributes.toList.map(attr => attr.getShortName -> attr): _*)
            val missing = getMissingValue(attributes)
            val fltData: CDFloatArray = CDFloatArray.factory(ma2array, missing)
            val dataArray = HeapFltArray(fltData, section.getOrigin, attributes.mapValues(_.toString), None)
            logger.info("[T%d] Reading variable %s from path %s, section shape: (%s), section origin: (%s), variable shape: (%s), size = %.2f M, read time = %.4f sec, sample data = [ %s ]".format(
              Thread.currentThread().getId(), varShortName, dataPath, section.getShape.mkString(","), section.getOrigin.mkString(","), variable.getShape.mkString(","), (section.computeSize * 4.0) / MB, (System.nanoTime() - t0) / 1.0E9, sample_data.mkString(", ")))
            new VariableRecord(date.toString, dataArray.missing.getOrElse(Float.NaN), dataArray.data)
          } catch {
            case err: Exception =>
              logger.error("Can't read data for variable %s in dataset %s due to error: %s".format(varShortName, ncDataset.getLocation, err.toString));
              logger.error("Variable shape: (%s),  section: { o:(%s) s:(%s) }".format(variable.getShape.mkString(","), section.getOrigin.mkString(","), section.getShape.mkString(",")));
              logger.error(err.getStackTrace.map(_.toString).mkString("\n"))
              throw err
          }
        case None => throw new Exception(s"Can't find variable $varShortName in dataset $dataPath ")
      }
    } finally { ncDataset.close() }
  }

  def keys: Set[String] = datasetCache.keySet().toSet
  def values: Iterable[NetcdfDataset] = datasetCache.values()
  def getKey( path: String ): String =  path + ":" + Thread.currentThread().getId()

  def aquireFile(dpath: String, context: String, cache: Boolean = false ): NetcdfDataset =
    if( cache ) { datasetCache.getOrElseUpdate( dpath, openFile(dpath,context) ) } else { openFile(dpath,context) }

  def openFile( dpath: String, context: String ): NetcdfDataset = try {
    val result = NetcdfDataset.openDataset(dpath)
//    logger.info(s"   *%* --> Opened Dataset from path: $dpath, context: $context ")
    result
  } catch {
    case err: Throwable => throw new Exception( s"Error opening netcdf file $dpath: ${err.toString} ")
  }

  def openCollection(varName: String, path: String ): NetcdfDataset = {
    val cpath = cleanPath(path)
    val key = getKey(path)
    val result = acquireCollection(cpath,varName)
    //    logger.info(s"   Accessed Dataset using key: $key, path: $cpath")
    result
  }

  def cleanPath( path: String ): String =
    if( path.startsWith("file:///") ) path.substring(7)
    else if( path.startsWith("file://") ) path.substring(6)
    else if( path.startsWith("file:/") ) path.substring(5)
    else path

  def closeAll: Iterable[NetcdfDataset]  = keys flatMap _close
  private def _close( key: String ): Option[NetcdfDataset] = Option( datasetCache.remove( key ) ).map ( dataset => { dataset.close(); dataset } )
  def close( path: String ): Option[NetcdfDataset] = _close( getKey(cleanPath(path)) )

  private def acquireCollection( dpath: String, varName: String ): NetcdfDataset = {
    val collectionPath: String = getCollectionPath( dpath, varName )
    logger.info(s" *%* --> Opening Dataset from path: $collectionPath   ")
    NetcdfDataset.openDataset(collectionPath)
  }

  def getCollectionPath( path: String, varName: String ): String = {
    if( path.endsWith("csv") ) {
      val metaFile = new MetaCollectionFile(path)
      metaFile.getPath( varName ) match {
        case Some(path) => path
        case None => throw new Exception( s"Can't locate variable ${varName} in Collection Directory ${path}")
      }
    } else { path }
  }

}

package nasa.nccs.edas.engine.spark.api

import nasa.nccs.cdapi.data.{RDDRecord, RDDataManager}
import nasa.nccs.edas.engine.EDASExecutionManager
import nasa.nccs.edas.sources.Collections
import org.apache.spark.rdd.RDD
import ucar.nc2
import ucar.nc2.constants.AxisType

abstract class EDASparkDataManager( val executionMgr: EDASExecutionManager ) extends RDDataManager {

  def getDatasets(): Set[String] = Collections.idSet

  def getDatasetMetadata( dsid: String ): Map[String,String] =
    Map( Collections.getCollectionMetadata( dsid  ).map( attr => ( attr.getShortName -> attrValue(attr) ) ):_*)

  def getVariables( dsid: String ): Set[String]
  def getVariableMetadata( vid: String ): Map[String,String]

  override def getDataProducts(): Set[String] = Set.empty
  override def getDataProductMetadata( pid: String ): Map[String,String] = Map.empty

  def getDataRDD( id: String, domain: Map[AxisType,(Int,Int)] ): RDD[RDDRecord]

  def attrValue( attribute: nc2.Attribute  ) =  attribute.toString.split('=').last.trim

}
package nasa.nccs.edas.sources

import nasa.nccs.cdapi.cdm.{CDGrid, CDSVariable}
import nasa.nccs.edas.rdd.VariableRecord
import nasa.nccs.esgf.process.{GenericOperationData, GridContext, ResultCacheElement}
import nasa.nccs.utilities.Loggable
import ucar.nc2

import scala.collection.immutable.Map


abstract class DataSource( val ctype: String, val id: String, val metadata: Map[String,String], val vars: Seq[String] = Seq()) extends Serializable with Loggable  {
  val title = metadata.getOrElse("title","Aggregated Collection")
  val scope = metadata.getOrElse("scope","local")

  def toXml: xml.Elem
  def getVariable(varName: String): CDSVariable
  def isEmpty: Boolean
  def getGrid( varName: String ) = CDGrid( varName + "-" + id, metadata.getOrElse("gridfile.path",throw new Exception( s"Can't find grid file for variable ${varName}")) )
}

object CachedResult {
  def apply( id: String, cachedResult: ResultCacheElement ): CachedResult = {
    new CachedResult( id, cachedResult.result.getMetadata, cachedResult.result.variableRecords )
  }
}

class CachedResult( id: String, metadata: Map[String,String], val variableRecords: Map[String,VariableRecord] ) extends DataSource( "cache", id, metadata, variableRecords.keys.toSeq ) {

  def toXml: xml.Elem = {
    <cache id={id} title={title}> </cache>
  }
  def gridFilePath(varName: String) = variableRecords.getOrElse( varName, missingVar(varName) ).gridFilePath
  def isEmpty: Boolean = id.isEmpty
  def getVariable(varName: String): CDSVariable = new CDSVariable( varName, getGrid( varName ), getAttributes(varName) )
  def getBaseAttributes: Map[String,nc2.Attribute] = metadata.map { case (key, sval) => key -> new nc2.Attribute( key, sval) }
  def getVariableAttributes(varName: String): Map[String,nc2.Attribute] = variableRecords.get(varName).fold( Map.empty[String,nc2.Attribute] )( _.getAttributes )
  def getAttributes(varName: String): Map[String,nc2.Attribute] = getBaseAttributes ++ getVariableAttributes(varName)
  override def getGrid(varName: String): CDGrid = CDGrid.create( id, gridFilePath(varName) )
  def missingVar(varName: String) = throw new Exception( s"Unrecognized Variable: ${varName}, current vars: { ${variableRecords.keys.mkString(", ")} }")
}

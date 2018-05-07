package nasa.nccs.edas.sources

import nasa.nccs.cdapi.cdm.{CDGrid, CDSVariable}
import nasa.nccs.esgf.process.GenericOperationData
import nasa.nccs.utilities.Loggable

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
  def apply( id: String, genericOpData: GenericOperationData ): CachedResult = {
    new CachedResult( id, genericOpData.getMetadata, genericOpData.getVars )
  }
}

class CachedResult( id: String, metadata: Map[String,String], vars: Seq[String] ) extends DataSource( "cache", id, metadata, vars ) {

  def toXml: xml.Elem = {
    <cache id={id} title={title}> </cache>
  }
  def isEmpty: Boolean = id.isEmpty
  def getVariable(varName: String): CDSVariable = null
}

package nasa.nccs.edas.sources

import nasa.nccs.cdapi.cdm.CDSVariable
import nasa.nccs.esgf.process.GenericOperationData
import nasa.nccs.utilities.Loggable

import scala.collection.immutable.Map


abstract class DataSource( val ctype: String, val id: String, val metadata: Map[String,String], val vars: Seq[String] = Seq()) extends Serializable with Loggable  {
  val title = metadata.getOrElse("title","Aggregated Collection")
  val scope = metadata.getOrElse("scope","local")

  def toXml: xml.Elem
  def getVariable(varName: String): CDSVariable
}

object CachedResult {
  def apply( id: String, genericOpData: GenericOperationData ): CachedResult = {
    new CachedResult( "cache", id, genericOpData.getMetadata, genericOpData.getVars )
  }
}

class CachedResult( ctype: String, id: String, metadata: Map[String,String], vars: Seq[String] ) extends DataSource( ctype, id, metadata, vars ) {

  def toXml: xml.Elem = {
    <cache id={id} title={title}> </cache>
  }
}

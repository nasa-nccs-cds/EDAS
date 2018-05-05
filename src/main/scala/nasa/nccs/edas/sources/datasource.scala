package nasa.nccs.edas.sources

import nasa.nccs.utilities.Loggable

import scala.collection.immutable.Map


abstract class DataSource( val ctype: String, val id: String, val dataPath: String, val metadata: Map[String,String], val vars: List[String] = List()) extends Serializable with Loggable  {
  val title = metadata.getOrElse("title","Aggregated Collection")
  val scope = metadata.getOrElse("scope","local")

  def toXml: xml.Elem
  def isEmpty: Boolean
}

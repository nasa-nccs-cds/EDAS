package nasa.nccs.edas.utilities
import java.nio.file.{Files, Paths}

import nasa.nccs.utilities.Loggable

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object appParameters extends Serializable with Loggable {

  private var customCacheDir: Option[String] = None
  private var configParams: Map[String, String] = Map.empty[String, String]
  private lazy val parmFile = Paths.get(cacheDir, "edas.properties").toString
  private lazy val map = buildParameterMap

  lazy val cacheDir = getCacheDirectory
  def addConfigParams(configuration: Map[String, String]): Unit = { configParams = configuration }
  def setCustomCacheDir(cache_dir: String): Unit = if(!cache_dir.isEmpty) { customCacheDir = Some(cache_dir) } else { logger.warn("Attempt to set empty cache dir ignored.") }

  def apply(key: String, default: String): String = map.getOrElse(key, default)
  def getParameterMap: Map[String, String] = map
  def apply(key: String): Option[String] = map.get(key);

  def bool(key: String, default: Boolean): Boolean = map.get(key) match {
    case Some(value) => value.toLowerCase.trim.startsWith("t")
    case None => default
  }

  def keySet: Set[String] = map.keySet

  def getCacheDirectory: String = customCacheDir match {
    case None =>
      sys.env.get("EDAS_CACHE_DIR") match {
        case Some(cache_path) => cache_path
        case None =>
          val home = System.getProperty("user.home")
          Paths.get(home, ".edas", "cache").toString
      }
    case Some(cache_dir) => cache_dir
  }

  private def buildParameterMap: Map[String, String] = {
    var _map: Map[String, String] = configParams
    if (Files.exists(Paths.get(parmFile))) {
      val params: Iterator[Array[String]] = for (line <- Source.fromFile(parmFile).getLines()) yield { line.split('=') }
      _map = _map ++ Map(params.filter(_.length > 1).map(a => a.head.trim -> a.last.trim).toSeq: _*)
    }
    else { logger.warn("Can't find default parameter file: " + parmFile); }
    map;
  }
}




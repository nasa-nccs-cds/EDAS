import java.io.{FileWriter, FilenameFilter, PrintWriter}
import java.nio.file.Files.copy
import java.nio.file.Files.deleteIfExists
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption

import sbt.{SettingKey, _}


val kernelPackages = settingKey[ Seq[String] ]("A list of user-defined Kernel packages")
val EDAS_VERSION = sys.env.getOrElse("EDAS_VERSION","{UNDEFINED}")

name := "EDAS"
version := EDAS_VERSION + "-SNAPSHOT"
scalaVersion := "2.11.8"
organization := "nasa.nccs"

lazy val root = project in file(".")
val sbtcp = taskKey[Unit]("sbt-classpath")
val upscr = taskKey[Unit]("update-edas-scripts")

resolvers += "Unidata maven repository" at "https://artifacts.unidata.ucar.edu/repository/unidata-all"
resolvers += "Java.net repository" at "http://download.java.net/maven/2"
resolvers += "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools"
resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
resolvers += "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main"
resolvers += "spray repo" at "http://repo.spray.io"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Geotoolkit" at "http://maven.geotoolkit.org/"
resolvers += "Maven Central" at "http://central.maven.org/maven2/"
resolvers += "JBoss Repo" at "https://repository.jboss.org/nexus/content/repositories/thirdparty-releases"

enablePlugins(JavaAppPackaging)

mainClass in (Compile, run) := Some("nasa.nccs.edas.portal.EDASApplication")
mainClass in (Compile, packageBin) := Some("nasa.nccs.edas.portal.EDASApplication")

libraryDependencies ++= ( Dependencies.cache  ++ Dependencies.geo ++ Dependencies.netcdf ++ Dependencies.socket ++ Dependencies.utils ++ Dependencies.test ) // ++ Dependencies.jackson

libraryDependencies ++= {
  sys.env.get("YARN_CONF_DIR") match {
    case Some(yarn_config) => Seq.empty
    case None => Dependencies.spark ++ Dependencies.scala                     // ++ Dependencies.xml     : For 2.11 or later!
  }
}

// dependencyOverrides ++= Set( "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4" )

dependencyOverrides += Library.jacksonCore
dependencyOverrides += Library.jacksonDatabind
dependencyOverrides += Library.jacksonModule

sbtcp := {
  val files: Seq[String] = (fullClasspath in Compile).value.files.map(x => x.getAbsolutePath)
  val libFiles: Seq[String] = ( baseDirectory.value / "lib" ).list()
  val sbtClasspath : String = ( files ++ libFiles ).mkString(":")
  println("Set SBT classpath to 'sbt-classpath' environment variable")
  System.setProperty("sbt-classpath", sbtClasspath)
}

compile  <<= (compile in Compile).dependsOn(sbtcp)

fork := true

logBuffered in Test := false

javaOptions in run ++= Seq( "-Xmx8000M", "-Xms512M", "-Xss1M", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC")
javaOptions in test ++= Seq( "-Xmx8000M", "-Xms512M", "-Xss1M", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC", "-XX:+PrintFlagsFinal")
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

import java.util.Properties
lazy val edasPropertiesFile = settingKey[File]("The edas properties file")
lazy val edasDefaultPropertiesFile = settingKey[File]("The edas defaultproperties file")
lazy val edasPythonRunScript = settingKey[File]("The edas python worker startup script")
lazy val edasDefaultPythonRunScript = settingKey[File]("The default edas python worker startup script")
lazy val edasStandaloneRunScript = settingKey[File]("The edas spark-cluster startup script")
lazy val edasDefaultStandaloneRunScript = settingKey[File]("The default edas spark-cluster startup script")
lazy val edasPythonShutdownScript = settingKey[File]("The edas python worker shutdown script")
lazy val edasDefaultPythonShutdownScript = settingKey[File]("The default edas python worker shutdown script")
lazy val edasSparkCleanupScript = settingKey[File]("The edas spark worker cleanup script")
lazy val edasDefaultSparkCleanupScript = settingKey[File]("The default edas spark worker cleanup script")
lazy val edasSetupScript = settingKey[File]("The edas setup runtime script")
lazy val edasDefaultSetupScript = settingKey[File]("The default edas setup runtime script")
lazy val edasLocalCollectionsFile = settingKey[File]("The edas local Collections file")
lazy val edas_cache_dir = settingKey[File]("The EDAS cache directory.")
lazy val edas_conf_dir = settingKey[File]("The EDAS conf directory.")
lazy val edas_sbin_dir = settingKey[File]("The EDAS sbin directory.")
lazy val edas_logs_dir = settingKey[File]("The EDAS logs directory.")
lazy val conda_lib_dir = settingKey[Option[File]]("The Conda lib directory.")
val edasProperties = settingKey[Properties]("The edas properties map")

edas_conf_dir := baseDirectory.value / "src" / "universal" / "conf"
edas_sbin_dir := getEDASbinDir
edas_logs_dir := getEDASlogsDir
conda_lib_dir := getCondaLibDir

//lazy val installNetcdfTask = taskKey[Unit]("Install Netcdf jar")
//
//installNetcdfTask := {
//  if( """netcdfAll.*""".r.findFirstIn( (baseDirectory.value / "lib").list.mkString(";") ).isEmpty ) {
//    (baseDirectory.value / "bin" / "install_netcdf_jar.sh").toString !
//  }
//}


unmanagedJars in Compile ++= {
  val jars_dir: String = sys.env.getOrElse( "EDAS_UNMANAGED_JARS", (baseDirectory.value / "lib").toString )
    val customJars: PathFinder =  file(jars_dir) ** (("*.jar" -- "*concurrentlinkedhashmap*") -- "*netcdf*")
    val classpath_file = edas_cache_dir.value / "classpath.txt"
    val pw = new PrintWriter( classpath_file )
    val jars_list = customJars.getPaths.mkString("\n")
    println("Custom jars: " + jars_list + ", dir: " + jars_dir )
    pw.write( jars_list )
    customJars.classpath
}

//unmanagedJars in Compile ++= {
//  sys.env.get("SPARK_HOME") match {
//    case Some(spark_dir) =>  ( file(spark_dir) ** "*.jar" ).classpath
//    case None => PathFinder.empty.classpath
//  }
//}

unmanagedClasspath in Test ++= conda_lib_dir.value.toSeq
unmanagedClasspath in (Compile, runMain) ++= conda_lib_dir.value.toSeq
classpathTypes += "dylib"
classpathTypes += "so"

stage ~= { (file: File) => edasPatch( file / "bin" / "edas" ); file }
// lazy val edasGlobalCollectionsFile = settingKey[File]("The edas global Collections file")

edas_cache_dir := getCacheDir
edasPropertiesFile := edas_cache_dir.value / "edas.properties"
edasDefaultPropertiesFile := baseDirectory.value / "project" / "edas.properties"
edasPythonRunScript := edas_sbin_dir.value / "startup_python_worker.sh"
edasDefaultPythonRunScript := baseDirectory.value / "bin" / "startup_python_worker.sh"
edasStandaloneRunScript := edas_sbin_dir.value / "startup_edas_standalone.sh"
edasDefaultStandaloneRunScript := baseDirectory.value / "bin" / "startup_edas_standalone.sh"
edasPythonShutdownScript := edas_sbin_dir.value / "shutdown_python_worker.sh"
edasDefaultPythonShutdownScript := baseDirectory.value / "bin" / "shutdown_python_worker.sh"
edasSparkCleanupScript := edas_sbin_dir.value / "cleanup_spark_workers.sh"
edasDefaultSparkCleanupScript := baseDirectory.value / "bin" / "cleanup_spark_workers.sh"
edasSetupScript := edas_sbin_dir.value / "setup_runtime.sh"
edasDefaultSetupScript := baseDirectory.value / "bin" / "setup_runtime.sh"

edasProperties := {
  val prop = new Properties()
  try{
     println("Loading property file: " + edasPropertiesFile.value.toString )
    IO.load( prop, edasPropertiesFile.value )
  } catch {
    case err: Exception => println("No property file found: " + edasPropertiesFile.value.toString )
  }
  prop
}

upscr := {
  if( !edasPropertiesFile.value.exists() ) {
    println("Copying default property file: " + edasDefaultPropertiesFile.value.toString )
    copy( edasDefaultPropertiesFile.value.toPath, edasPropertiesFile.value.toPath )
  }
  println("Copying default python run script: " + edasDefaultPythonRunScript.value.toString)
  copy( edasDefaultPythonRunScript.value.toPath, edasPythonRunScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default python shutdown script: " + edasDefaultPythonShutdownScript.value.toString )
  copy( edasDefaultPythonShutdownScript.value.toPath, edasPythonShutdownScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default spark cleanup script: " + edasDefaultSparkCleanupScript.value.toString )
  copy( edasDefaultSparkCleanupScript.value.toPath, edasSparkCleanupScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default edas spark-cluster startup script: " + edasDefaultStandaloneRunScript.value.toString  + " to " + edasStandaloneRunScript.value.toString )
  copy( edasDefaultStandaloneRunScript.value.toPath, edasStandaloneRunScript.value.toPath, StandardCopyOption.REPLACE_EXISTING )
  println("Copying default setup script: " + edasDefaultSetupScript.value.toString )
  deleteIfExists( edasSetupScript.value.toPath )
  val lines = scala.io.Source.fromFile( edasDefaultSetupScript.value ).getLines
  val out_lines = Seq( "#!/usr/bin/env bash", "export EDAS_HOME_DIR=" + baseDirectory.value ) ++ lines
  val fw = new PrintWriter( edasSetupScript.value.toPath.toFile )
  try { fw.write( out_lines.mkString("\n")) }
  finally fw.close()
}

compile  <<= (compile in Compile).dependsOn( upscr )

def getCondaLibDir: Option[File] = sys.env.get("CONDA_PREFIX") match {
  case Some(ldir) => Some(file(ldir) / "lib")
  case None => println( " ******* Warning: Must activate the edas environment in Anaconda to run the EDAS server: '>> source activate edas'  ******* " ); None
}

def getCacheDir: File = {
  val cache_dir = sys.env.get("EDAS_CACHE_DIR") match {
    case Some(cache_dir) => file(cache_dir)
    case None => file(System.getProperty("user.home")) / ".edas" / "cache";
  }
  val ncml_dir = cache_dir / "collections" / "NCML";
  ncml_dir.mkdirs();
  cache_dir
}

def getEDASbinDir: File = {
  val bin_dir =  file(System.getProperty("user.home")) / ".edas" / "sbin";
  bin_dir.mkdirs();
  bin_dir
}

def getEDASlogsDir: File = {
  val log_dir =  file("/tmp") / System.getProperty("user.name") / "logs";
  log_dir.mkdirs();
  log_dir
}

edasLocalCollectionsFile :=  {
  val collections_file = edas_cache_dir.value / "collections" / "local_collections.xml"
  if( !collections_file.exists ) { xml.XML.save( collections_file.getAbsolutePath, <collections></collections> ) }
  collections_file
}

//edasGlobalCollectionsFile := {
//  val collections_file = baseDirectory.value / "src" / "main" / "resources" / "global_collections.xml"
//  val collections_install_path = edas_cache_dir.value / "global_collections.xml"
//  if( !collections_install_path.exists() ) { copy( collections_file.toPath, collections_install_path.toPath ) }
//  collections_install_path
//}

publishTo := Some(Resolver.file( "file",  sys.env.get("SBT_PUBLISH_DIR") match {
  case Some(pub_dir) => { val pdir = file(pub_dir); pdir.mkdirs(); pdir }
  case None =>  { val pdir = getCacheDir / "publish"; pdir.mkdirs(); pdir }
} ) )

//
//md := {
//  import nasa.nccs.cds2.engine.MetadataPrinter
//}







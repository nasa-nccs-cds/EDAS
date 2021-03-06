import sbt._

object Versions {
  val ucar = "4.6.8"
  val spark = "2.2.1"
  val jackson = "2.6.5"
  val breeze = "0.13.2"
}

object Library {
  val logback        = "ch.qos.logback"     %  "logback-core"   % "1.1.3"
  val mockitoAll     = "org.mockito"       %  "mockito-all"     % "1.10.19"
  val scalaTest      = "org.scalatest"     %% "scalatest"       % "2.2.4"
  val sparkMLLib     = "org.apache.spark"  %% "spark-mllib"     % Versions.spark
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Versions.spark
  val sparkCore      = "org.apache.spark"  %% "spark-core"      % Versions.spark
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Versions.spark
  val commonsIO      = "commons-io"         % "commons-io"      % "2.5"
  val zeromq         = "org.zeromq"         % "jeromq"          % "0.4.3"
  val breezeNlp      = "org.scalanlp"       %% "breeze"         % Versions.breeze
  val breezeNative   = "org.scalanlp"      %% "breeze-natives"  % Versions.breeze
  val breezeViz      = "org.scalanlp"      %% "breeze-viz"      % Versions.breeze
  val cdm            = "edu.ucar"           % "cdm"             % Versions.ucar
  val clcommon       = "edu.ucar"           % "clcommon"        % Versions.ucar
  val netcdf4        = "edu.ucar"           % "netcdf4"         % Versions.ucar
  val opendap        = "edu.ucar"           % "opendap"         % Versions.ucar
  val netlib         = "com.github.fommil.netlib"  % "all"      % "1.1.2"
  val nd4s           = "org.nd4j"           %% "nd4s"       % "0.4-rc3.8"
  val nd4j           =  "org.nd4j"          % "nd4j-x86"        % "0.4-rc3.8"
  val httpservices   = "edu.ucar"           %  "httpservices"   % Versions.ucar
  val httpclient     = "org.apache.httpcomponents" % "httpclient" % "4.5.2"
  val udunits        = "edu.ucar"           %  "udunits"        % Versions.ucar
  val joda           = "joda-time"          % "joda-time"       % "2.8.1"
  val natty          = "com.joestelmach"    % "natty"           % "0.12"
  val py4j           = "net.sf.py4j"        % "py4j"            % "0.10.4"
  val geotools       = "org.geotools"      %  "gt-shapefile"    % "13.2"
  val scalactic      = "org.scalactic" %% "scalactic"          % "3.0.0"
  val scalatest      = "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  val concurrentlinkedhashmap = "com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.4.2"
  val reflections    = "org.reflections" % "reflections"       % "0.9.10"
  val scalaxml       = "org.scala-lang.modules" %% "scala-xml"  % "1.0.3"
  val scalaparser    = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3"
  val jacksonCore = "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson
  val jacksonDatabind = "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson
  val jacksonModule = "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson

}

object Dependencies {
  import Library._

  val scala = Seq( joda, scalactic, commonsIO, httpclient )

  val test = Seq( scalatest, logback )

  val xml = Seq( scalaxml, scalaparser )

  val breeze = Seq( breezeNlp, breezeNative, breezeViz )

  val jackson = Seq( jacksonCore, jacksonDatabind, jacksonModule )

  val spark = Seq( sparkCore, sparkStreaming, sparkMLLib, sparkSQL ) // , netlib )

  val cache = Seq( concurrentlinkedhashmap )

  val ndarray = Seq( nd4s, nd4j )

  val geo  = Seq( geotools )

  val netcdf = Seq( cdm, clcommon, netcdf4, opendap )

  val socket = Seq(  zeromq )

  val utils = Seq( natty )
}


object edasPatch {
  def apply( filePath: sbt.File ) = {
    import scala.io.Source
    import java.io
    val old_lines = Source.fromFile(filePath).getLines.toList
    val new_lines = old_lines map { line =>
      val pos = line.indexOfSlice("app_classpath=")
      if (pos == -1) line else { line.slice(0, pos + 15) + "${CONDA_PREFIX}/lib:" + line.slice(pos + 15, line.length) }
    }
    val pw = new io.PrintWriter( new File(filePath.toString) )
    pw.write(new_lines.mkString("\n"))
    pw.close
    println( "Patched executable: " + filePath )
  }
}












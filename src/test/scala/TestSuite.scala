import java.io.File
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.Formatter
import java.util.regex.Pattern
import scala.xml
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.edas.engine.ExecutionCallback
import nasa.nccs.edas.sources.{CollectionLoadServices, Collections}
import nasa.nccs.edas.utilities.runtime
import nasa.nccs.esgf.wps.Job
import nasa.nccs.utilities.{EDASLogManager, Loggable}
import org.apache.commons.lang.RandomStringUtils
// import co.theasi.plotly
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}
import ucar.nc2.dataset.{CoordinateAxis, CoordinateAxis1DTime}
import ucar.nc2.{NetcdfFileWriter, Variable}

import scala.collection.mutable.ListBuffer

//class DASSTestSuite extends EDASTestSuite {
//
//  test("SpaceAve-weighted") {
//    val datainputs = s"""[domain=[{"name":"d0","time":{"start":"1960-01-01T00:00:00:00Z","end":"1960-01-05T00:00:00:00Z","system":"timestamps"}}],variable=[{"uri":"collection://iap-ua_eraint_tas1hr","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"cosine","axes":"t"}]]"""
//    val result_node = executeTest( datainputs )
//    val result_data = getResultData( result_node ).sample(35)
//    println( "Op Result:       " + result_data.mkBoundedDataString(", ", 35) )
//  }
//}

class DefaultTestSuite extends EDASTestSuite {

  val nExp = 3
  val use_6hr_data = false
  val use_npana_data = false
  val use_local_data = false
  val test_cache = false
  val test_python = true
  val test_binning = true
  val test_regrid = true
  val test_collection_output = true
  val reanalysis_ensemble = false
  val mod_collections = for (model <- List( "GISS", "GISS-E2-R" ); iExp <- (1 to nExp)) yield (model -> s"${model}_r${iExp}i1p1")
  val cip_collections = for ( model <- List( "CIP_CFSR_6hr", "CIP_MERRA2_mon" ) ) yield (model -> s"${model}_ta")

  test("getCollections") {
    CollectionLoadServices.startService()
    Thread.sleep(20000)
    val response = getCapabilities("coll")
    print( response.toString )
  }

  test("ReanalysisEnsemble") { if(test_regrid && reanalysis_ensemble ) {
    print( s"Running test ReanalysisEnsemble" )
    val JRA_input   =  s"""{"uri":"collection:/cip_ecmwf_mon_1980-2015","name":"ta:v0","domain":"d0"}"""
    val MERRA2_input = s"""{"uri":"collection:/cip_merra2_mon_1980-2015","name":"ta:v1","domain":"d0"}"""
    val datainputs =
      s"""[   variable=[$JRA_input,$MERRA2_input],
              domain=[ {"name":"d0","time":{"start":"1990-01-01T00:00:00Z","end":"1990-03-01T00:00:00Z","system":"values"},"lat":{"start":20,"end":30,"system":"values"},"lon":{"start":30,"end":40,"system":"values"}} ],
              operation=[{"name":"CDSpark.filter","input":"v0","plev":"97500,87500,77500","result":"cv0"},{"name":"CDSpark.filter","input":"v1","plev":"97500,87500,77500","result":"cv1"},{"name":"CDSpark.eAve","input":"cv0,cv1","crs":"~v0"}]]""".stripMargin.replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ).slice(0,0,10) )
    println( " ** Op Result:       " + result_data.mkBoundedDataString( ", ", 200 ) )
  }}

  test("RegridToTargetInput") { if( test_regrid ) {
    print( s"Running test ReanalysisEnsemble" )
    val JRA_input   =  s"""{"uri":"collection:/cip_ecmwf_mon_1980-2015","name":"tas:v0","domain":"d0"}"""
    val MERRA2_input = s"""{"uri":"collection:/cip_merra2_mon_1980-2015","name":"tas:v1","domain":"d0"}"""
    val datainputs =
      s"""[   variable=[$JRA_input,$MERRA2_input],
              domain=[ {"name":"d0","time":{"start":"1990-01-01T00:00:00Z","end":"1990-03-01T00:00:00Z","system":"values"},"lat":{"start":20,"end":30,"system":"values"},"lon":{"start":30,"end":40,"system":"values"}} ],
              operation=[{"name":"CDSpark.eAve","input":"v0,v1","crs":"~v0"}]]""".stripMargin.replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ).slice(0,0,10) )
    println( " ** Op Result:       " + result_data.mkBoundedDataString( ", ", 200 ) )
  }}

//  test("DiffWithRegrid")  { if(test_regrid)  {
//    print( s"Running test DiffWithRegrid" )
//    val MERRA_mon_variable = s"""{"uri":"collection:/cip_merra2_mon_1980-2015","name":"tas:v0","domain":"d0"}"""
//    val CFSR_mon_variable   = s"""{"uri":"collection:/cip_cfsr_mon_1980-1995","name":"tas:v1","domain":"d0"}"""
//    val ECMWF_mon_variable = s"""{"uri":"collection:/cip_ecmwf_mon_1980-2015","name":"tas:v2","domain":"d0"}"""
//    val datainputs =
//      s"""[   variable=[$MERRA_mon_variable,$CFSR_mon_variable],
//              domain=[  {"name":"d0","time":{"start":"1990-01-01T00:00:00Z","end":"1991-01-01T00:00:00Z","system":"values"}},
//                        {"name":"d1","time":{"start":"1990-01-01T00:00:00Z","end":"1991-01-01T00:00:00Z","system":"values"},"lat":{"start":20,"end":50,"system":"values"},"lon":{"start":30,"end":40,"system":"values"}} ],
//              operation=[{"name":"CDSpark.eDiff","input":"v0,v1","domain":"d0","crs":"~cip_merra2_mon_1980-2015"}]]""".stripMargin.replaceAll("\\s", "")
//    val result_node = executeTest(datainputs)
//    val result_data = CDFloatArray( getResultData( result_node ).slice(0,0,10) )
//    println( " ** Op Result:       " + result_data.mkBoundedDataString( ", ", 200 ) )
//  } }

  test("TimeConvertedDiff")  { if( use_6hr_data ) {
    print( s"Running test TimeConvertedDiff" )
    val CFSR_6hr_variable = s"""{"uri":"collection:/CIP_CFSR_6hr_ta","name":"ta:v0","domain":"d0"}"""
    val MERRA2_mon_variable = s"""{"uri":"collection:/CIP_MERRA2_mon_ta","name":"ta:v1","domain":"d0"}"""
    val datainputs = s"""[variable=[$CFSR_6hr_variable,$MERRA2_mon_variable],domain=[{"name":"d0","lat":{"start":0,"end":30,"system":"values"},"time":{"start":"2000-01-01T00:00:00Z","end":"2009-12-31T00:00:00Z","system":"values"},"lon":{"start":0,"end":30,"system":"values"}},{"name":"d1","crs":"~v1","trs":"~v0"}],operation=[{"name":"CDSpark.eDiff","input":"v0,v1","domain":"d1"}]]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ).slice(0,0,10) )
    println( " ** Op Result:       " + result_data.mkDataString(", ") )
  }}

  test("subsetTestXY") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 241.2655, 245.2, 244.904, 244.6914, 244.5297, 244.2834, 244.0234, 245.4426, 245.1731, 244.9478, 244.6251, 244.2375, 244.0953, 248.4837, 247.4268, 246.4957, 245.586, 245.4244, 244.8213, 249.7772, 248.7458, 247.5331, 246.8871, 246.0183, 245.8848, 248.257, 247.3562, 246.3798, 245.3962, 244.6091, 243.6039 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":0,"end":5,"system":"indices"},"lon":{"start":0,"end":5,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
    println( " ** NCO Result:       " + nco_verified_result.mkDataString(", ") )
    assert( getResultData( result_node ).maxScaledDiff( nco_verified_result ) < eps, s" Incorrect value computed for Sum")
  }

  test("getCapabilities") {
    val response = getCapabilities("op")
    print( response.toString )
  }

  //  test("pyZADemo") {
  //    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"file:///Users/tpmaxwel/Dropbox/Tom/Data/MERRA/atmos_ua.nc","name":"ua:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.avew","input":"v1","axes":"xt","filter":"DJF"}]]"""
  //    val result_node = executeTest(datainputs)
  //    val result_data = CDFloatArray( getResultData( result_node ) )
  //    val array_data = result_data.getArrayData(50)
  //    assert( array_data.length > 0 )
  //    println( " ** CDMS Result:       "  + array_data.mkString(", ") )
  //  }

  test("pyWeightedAveTest") { if(test_python) {
    val unverified_result: CDFloatArray = CDFloatArray( Array( 276.80597, 276.60977, 276.65247, 278.10095, 279.9955, 281.20566, 281.34833, 281.0004, 279.65433, 278.43326, 277.53558 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.avew","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
    println( " ** unverified Result:       " + unverified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }}


  test("NCML-timeBinAveTestLocal")  { if(test_binning) {
    val data_file = "collection:/giss_r1i1p1"
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":10,"end":10,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"%s","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","groupBy":"monthofyear","axes":"t"}]]""".format( data_file )
    val result_node = executeTest( datainputs )
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )

 //   val result_data = getResultDataArraySeq( result_node )
 //   println( " ** CDMS Results:       \n\t" + result_data.map( tup => tup._1.toString + " ---> " + tup._2.mkBoundedDataString(", ",16) ).mkString("\n\t") )
  }}

  test("pyMaxTestLocal")  { if(test_python) {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"tyx"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node ) )
    println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
    //    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
    //    assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  }}

//  test("pyTimeAveTest")  { if(test_python) {
//    val nco_result: CDFloatArray = CDFloatArray( Array( 286.2326, 286.5537, 287.2408, 288.1576, 288.9455, 289.5202, 289.6924, 289.5549, 288.8497, 287.8196, 286.8923 ).map(_.toFloat), Float.MaxValue )
//    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","domain":"d0","axes":"t"}]]"""
//    val result_node = executeTest(datainputs)
//    val result_data = CDFloatArray( getResultData( result_node ) )
//    println( " ** CDMS Result:       " + result_data.mkBoundedDataString(", ",10) )
//    //    println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
//    //    assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
//  }}

  //  test("pyWeightedAveTestExt") {
  //      val nco_result: CDFloatArray = CDFloatArray( Array( 286.2326, 286.5537, 287.2408, 288.1576, 288.9455, 289.5202, 289.6924, 289.5549, 288.8497, 287.8196, 286.8923 ).map(_.toFloat), Float.MaxValue )
  //      val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.cdmsExt.ave","input":"v1","domain":"d0","axes":"xy"}]]"""
  //      val result_node = executeTest(datainputs)
  //      val result_data = CDFloatArray( getResultData( result_node, true ) )
  //      println( " ** CDMS Result:       " + result_data.mkDataString(", ") )
  //      println( " ** NCO Result:       " + nco_result.mkDataString(", ") )
  //      assert( result_data.maxScaledDiff( nco_result )  < eps, s" UVCDAT result (with generated weights) does not match NCO result (with cosine weighting)")
  //    }

  test("pyRegridTest")  { if(test_regrid) {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.noOp","input":"v1","domain":"d0","grid":"uniform","shape":"32,64"}]]"""
    val result_node = executeTest(datainputs )
    val result_array = CDFloatArray( getResultData( result_node ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("pyRegridTest_MERRA")  { if(test_regrid) {
    val datainputs =
      s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],
         | variable=[{"uri":"collection:/cip_merra2_mon_1980-2015","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.noOp","input":"v1","domain":"d0","grid":"uniform","shape":"32,64"}]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
    val result_array = CDFloatArray( getResultData( result_node ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("pyRegridTest_MERRA_collection")  { if(test_collection_output) {
    val datainputs =
      s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],
         | variable=[{"uri":"collection:/cip_merra2_mon_1980-2015","name":"tas:v0","domain":"d0"}],
         | operation=[{"name":"CDSpark.noOp","input":"v0","domain":"d0","grid":"uniform","shape":"32,64","responseform":"collection:merra2_tas_regrid_32x64"}]]""".stripMargin
    val result_node0 = executeTest( datainputs )  // , runArgs = Map( "response" -> "collection","cid" -> "merra2_tas_regrid_32x64" )
    println( " ** Result: " + result_node0.toString() )
    val datainputs1 =
      s"""[domain=[{"name":"d1"}],
         | variable=[{"uri":"collection:/merra2_tas_regrid_32x64","name":"tas:v1","domain":"d1"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d1","axes":"xy"}]]""".stripMargin
    val result_node1 = executeTest( datainputs1 )
    val result_array = CDFloatArray( getResultData( result_node1 ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("preprocess_MERRA_collection")  { if(test_collection_output) {
    val datainputs =
      s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],
         | variable=[{"uri":"collection:/cip_merra2_mon_1980-2015","name":"tas:v0","domain":"d0"}],
         | operation=[{"name":"SparkML.rescale","input":"v0","domain":"d0","grid":"uniform","shape":"32,64","responseform":"collection:merra2_tas_regrid_32x64"}]]""".stripMargin
    val result_node0 = executeTest( datainputs )  // , runArgs = Map( "response" -> "collection","cid" -> "merra2_tas_regrid_32x64" )
    println( " ** Result: " + result_node0.toString() )
    val datainputs1 =
      s"""[domain=[{"name":"d1"}],
         | variable=[{"uri":"collection:/merra2_tas_regrid_32x64","name":"tas:v1","domain":"d1"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d1","axes":"xy"}]]""".stripMargin
    val result_node1 = executeTest( datainputs1 )
    val result_array = CDFloatArray( getResultData( result_node1 ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("AveTest")  { if(test_regrid) {
    val datainputs =
      s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"xy"}]]""".stripMargin
    val result_node = executeTest(datainputs )
    val result_array = CDFloatArray( getResultData( result_node ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("pyRegrid2Test")  { if(test_regrid) {
    val t0 = System.nanoTime()
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.noOp","input":"v1","domain":"d0","grid":"uniform","shape":"32,64"}]]"""
    val result_node = executeTest(datainputs )
    val result_array = CDFloatArray( getResultData( result_node ) )
    println(" ### First Execution, time: %.2f".format( (System.nanoTime-t0)/1.0E9 ))
    val t1 = System.nanoTime()
    val datainputs1 = s"""[domain=[{"name":"d0","time":{"start":20,"end":30,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.noOp","input":"v1","domain":"d0","grid":"uniform","shape":"32,64"}]]"""
    val result_node1 = executeTest(datainputs1 )
    val result_array1 = CDFloatArray( getResultData( result_node1 ) )
    println(" ### Second Execution, time: %.2f".format( (System.nanoTime-t1)/1.0E9 ))
  }}

  test("pyRegridTestFile")  { if(test_regrid) {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":0,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.noOp","input":"v1","domain":"d0","grid":"uniform","shape":"32,64"}]]"""
    val result_node = executeTest(datainputs, Map( "saveLocalFile" -> "true" ) )
    val result_array = CDFloatArray( getResultData( result_node ) )
    println( " ** Result Sample:       " + result_array.sample( 35 ).mkDataString( ", " ) )
    println( " ** Result Shape:       " + result_array.getShape.mkString(",") )
  }}

  test("ensemble_time_ave0") {
    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
    val datainputs = s"""[
             variable=[$GISS_H_variables],
             domain=[       {"name":"d0","lat":{"start":10,"end":20,"system":"values"},"lon":{"start":10,"end":20,"system":"values"}}],
             operation=[    {"name":"CDSpark.eAve","input":"${GISS_H_vids.mkString(",")}","domain":"d0"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ) )
    println( " ** Op Result:         " + result_data.mkBoundedDataString( ", ", 100 ) )
  }

  test("ensemble_time_ave1") {
    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
    val datainputs = s"""[
             variable=[$GISS_H_variables],
             domain=[       {"name":"d0","lat":{"start":10,"end":20,"system":"values"},"lon":{"start":10,"end":20,"system":"values"},"time":{"start":"1985-01-01T00:00:00Z","end":"1990-04-04T00:00:00Z"}}],
             operation=[    {"name":"CDSpark.eAve","input":"${GISS_H_vids.mkString(",")}","domain":"d0"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ) )
    println( " ** Op Result:         " + result_data.mkBoundedDataString(", ", 100) )
  }

  test("time_bounds_test") {
    val datainputs = s"""[
             variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
             domain=[       {"name":"d0","lat":{"start":10,"end":20,"system":"indices"},"lon":{"start":10,"end":20,"system":"indices"},"time":{"start":"1985-01-01T00:00:00Z","end":"1985-12-31T23:00:00Z"}}],
             operation=[    {"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ) )
    assert( result_data.getSize == 12, s" Incorrect number of time values in output" )
    println( " ** Op Result:         " + result_data.mkBoundedDataString(", ", 100) )
  }

  test("ensemble_time_ave2") {
//    val unverified_result: CDFloatArray = CDFloatArray(  Array( 246.78374, 246.78374, 246.78374, 246.78374, 246.78374, 246.78374, 246.78374, 246.78374, 246.78374, 246.78374 ).map(_.toFloat), Float.MaxValue )
    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
    val datainputs = s"""[
             variable=[$GISS_H_variables],
             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}}],
             operation=[    {"name":"CDSpark.eAve","input":"${GISS_H_vids.mkString(",")}","domain":"d0","id":"eaGISS-H"} ]
            ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
    println( " ** Op Result:         " + result_data.mkDataString(", ") )
//    println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
//    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  }

  //  test("ESGF_subDemo1") {
  //    val unverified_result: CDFloatArray = CDFloatArray(  Array( 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841 ).map(_.toFloat), Float.MaxValue )
  //    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
  //    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
  //    val datainputs = s"""[
  //             variable=[$GISS_H_variables],
  //             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}},{"name":"d1","crs":"uniform~128"}],
  //             operation=[    {"name":"CDSpark.multiAverage","input":"${GISS_H_vids.mkString(",")}","domain":"d0","id":"eaGISS-H"} ]
  //            ]""".replaceAll("\\s", "")
  //    val result_node = executeTest(datainputs)
  //    val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
  //    println( " ** Op Result:         " + result_data.mkDataString(", ") )
  //    println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
  //    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  //  }
  //
  //  test("ESGF_subDemo2") {
  //    val unverified_result: CDFloatArray = CDFloatArray(  Array( 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841, 243.85841 ).map(_.toFloat), Float.MaxValue )
  ////    val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
  ////    val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
  ////    val CIP_vids = ( 1 to cip_collections.length ) map { index => s"vC$index" }
  ////    val CIP_variables     = ( ( 0 until cip_collections.length ) map { index =>  s"""{"uri":"collection:/${cip_collections(index)}","name":"tas:${CIP_vids(index)}","domain":"d0"}""" } ).mkString(",")
  //    val variable1 = """{"uri":"collection:/cip_cfsr_6hr_ta","name":"ta:v0","domain":"d0"}"""
  //    val variable2 = """{"uri":"collection:/merra2-6hr-ana_np.200001","name":"T:v1","domain":"d0"}"""
  //    val variable3 = """{"uri":"collection:/cip_merra2_mon_ta","name":"ta:v2","domain":"d0"}"""
  //    val datainputs = s"""[
  //             variable=[$variable1,$variable2,$variable3],
  //             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"2015-04-04T00:00:00Z","system":"values"}}],
  //             operation=[    {"name":"CDSpark.multiAverage","input":"v0","domain":"d0","id":"eaCFSR"},{"name":"CDSpark.multiAverage","input":"v1,v2","domain":"d0","id":"eaMERRA"}]
  //            ]""".replaceAll("\\s", "")
  //    val result_node = executeTest(datainputs)
  //    val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
  //    println( " ** Op Result:         " + result_data.mkDataString(", ") )
  //    println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
  //    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  //  }
  //
  //  test("ESGF_Demo") {
  //      val unverified_result: CDFloatArray = CDFloatArray(  Array( 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908, 242.11908 ).map(_.toFloat), Float.MaxValue )
  //      val GISS_H_vids = ( 1 to nExp ) map { index => s"vH$index" }
  //      val GISS_E2R_vids = ( 1 to nExp ) map { index => s"vR$index" }
  //      val GISS_H_variables     = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss_r${index}i1p1","name":"tas:${GISS_H_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
  //      val GISS_E2R_variables = ( ( 1 to nExp ) map { index =>  s"""{"uri":"collection:/giss-e2-r_r${index}i1p1","name":"tas:${GISS_E2R_vids(index-1)}","domain":"d0"}""" } ).mkString(",")
  //      val datainputs = s"""[
  //             variable=[$GISS_H_variables,$GISS_E2R_variables],
  //             domain=[       {"name":"d0","time":{"start":"1985-01-01T00:00:00Z","end":"1985-04-04T00:00:00Z","system":"values"}},{"name":"d1","crs":"uniform~128"}],
  //             operation=[    {"name":"CDSpark.multiAverage","input":"${GISS_H_vids.mkString(",")}","domain":"d0","id":"eaGISS-H"},
  //                            {"name":"CDSpark.multiAverage","input":"${GISS_E2R_vids.mkString(",")}","domain":"d0","id":"eaGISS-E2R"},
  //                            {"name":"CDSpark.multiAverage","input":"eaGISS-E2R,eaGISS-H","domain":"d1","result":"esgfDemo"} ]
  //            ]""".replaceAll("\\s", "")
  //      val result_node = executeTest(datainputs)
  //      val result_data = CDFloatArray( getResultData( result_node, false ).slice(0,0,10) )
  //      println( " ** Op Result:         " + result_data.mkDataString(", ") )
  //      println( " ** Unverified Result: " + unverified_result.mkDataString(", ") )
  //      assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Max")
  //    }

  test("TimeSum-dap") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 140615.5f, 139952f, 139100.6f, 138552.2f, 137481.9f, 137100.5f ), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":0,"end":611,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("giss-ave-dap") {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0","cache":"false"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"tyx"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
  }

//  test("giss-ss-ave-dap") {
//    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"SparkSQL.ave","input":"v1","domain":"d0","axes":"tyx"}]]"""
//    val result_node = executeTest(datainputs)
//    val result_data = getResultData( result_node )
//    println( "Op Result:       " + result_data.mkDataString(", ") )
//  }

  test("anomaly-cycle")  { if(test_binning) {
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":40,"end":40,"system":"values"},"lon":{"start":40,"end":40,"system":"values"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","groupBy":"monthofyear","axes":"t"}]]""".stripMargin
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }}

  test("anomaly")  { if(test_binning) {
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":40,"end":40,"system":"values"},"lon":{"start":40,"end":40,"system":"values"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","groupBy":"monthofyear","axes":"t","id":"v1ave"},{"name":"CDSpark.eDiff","input":"v1,v1ave","domain":"d0"}]]""".stripMargin
    val result_node = executeTest( datainputs )
    println( "Result Node:       " + result_node.toString )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }}

  test("anomaly-time") {
    val datainputs = s"""[domain=[{"name":"d0", "lat":{"start":30,"end":50,"system":"values"}, "lon":{"start":0,"end":20,"system":"values"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","axes":"t","id":"v1ave"},{"name":"CDSpark.eDiff","input":"v1,v1ave"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("anomaly-time-multiK") {
    val datainputs = s"""[domain=[{"name":"d0", "lat":{"start":30,"end":50,"system":"values"}, "lon":{"start":0,"end":20,"system":"values"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.anomaly","input":"v1","axes":"t"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("anomaly-seasonal") {
    val datainputs = s"""[domain=[{"name":"d0", "lat":{"start":40,"end":40,"system":"values"}, "lon":{"start":260,"end":260,"system":"values"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","axes":"t","id":"v1ave","groupBy":"seasonOfYear"},{"name":"CDSpark.eDiff","input":"v1,v1ave"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("anomaly-seasonal-write-output") {
    val datainputs = s"""[domain=[{"name":"d0", "lat":{"start":0,"end":90,"system":"values"}, "lon":{"start":0,"end":90,"system":"values"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","axes":"t","id":"v1ave","groupBy":"seasonOfYear"},{"name":"CDSpark.write","input":"v1ave"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("seasonal-cycle") {
    val datainputs =
      s"""[domain=[{"name":"d0", "lat":{"start":40,"end":40,"system":"values"}, "lon":{"start":260,"end":260,"system":"values"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         |operation=[{"name":"CDSpark.ave","input":"v1","axes":"t","id":"v1ave","groupBy":"season"}]]""".stripMargin
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Shape: " + result_data.getShape.mkString(",") )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("anomaly-time-2") {
    val datainputs =
      s"""[domain=[   {"name":"d0", "lat":{"start":30,"end":50,"system":"values"}, "lon":{"start":0,"end":20,"system":"values"}},
         |            {"name":"d1", "lat":{"start":30,"end":50,"system":"values"}, "lon":{"start":0,"end":20,"system":"values"}, "time": {"start": 0, "end": 100, "crs": "indices"}}],
         |  variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v0","domain":"d0"},{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d1"}],
         |  operation=[{"name":"CDSpark.ave","input":"v0","axes":"t","id":"v0ave"},{"name":"CDSpark.eDiff","input":"v1,v0ave"}]]""".stripMargin
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }

  test("anomaly-spatial") { if(test_binning) {
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":0,"end":60,"system":"values"},"lon":{"start":0,"end":60,"system":"values"},"time": {"start": 0, "end": 100, "crs": "indices"}},{"name":"d1","lat":{"start":30,"end":30,"system":"values"},"lon":{"start":30,"end":30,"system":"values"}, "time": {"start": 0, "end": 100, "crs": "indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v0","domain":"d0"},{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d1"}],operation=[{"name":"CDSpark.ave","input":"v0","axes":"xy","id":"v1ave"},{"name":"CDSpark.eDiff","input":"v1,v1ave"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
  }}

  test("time-ave-giss") {
    val datainputs =
      s"""[domain=[ {"name":"d0", "time": {"start":"1980-01-01T00:00:00Z", "end":"1980-01-31T23:00:00Z", "crs": "timestamps"}} ],
          variable=[ {"uri":"collection:/giss_r1i1p1","name":"tas:v0","domain":"d0"}],
          operation=[ {"name":"CDSpark.ave","input":"v0","axes":"t"} ] ]""".stripMargin
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ",300) )
  }


//  test("time-ave-domains-diff")  {  if( test_binning ) {
//    val datainputs =
//      s"""[domain=[
//              {"name":"d0", "time": {"start":"1980-01-01T00:00:00", "end":"1981-01-01T00:00:00", "crs": "timestamps"}},
//              {"name":"d1", "time": {"start":"2000-01-01T00:00:00", "end":"2001-01-01T00:00:00", "crs": "timestamps"}}],
//          variable=[
//              {"uri":"collection:/giss_r1i1p1","name":"tas:v0","domain":"d0"},
//              {"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d1"}],
//          operation=[
//              {"name":"CDSpark.ave","input":"v0","axes":"t","id":"v0ave"},
//              {"name":"CDSpark.ave","input":"v1","axes":"t","id":"v1ave"},
//              {"name":"CDSpark.eDiff","input":"v0ave,v1ave"}]
//          ]""".stripMargin
//    val result_node = executeTest( datainputs )
//    val result_data = getResultData( result_node )
//    println( "Op Result:       " + result_data.mkBoundedDataString(", ",100) )
//  }}

  test("pyMaximum-cache")  { if(test_python ) {
    val nco_verified_result = 309.7112
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
    println( "Verified Result: " + nco_verified_result )
    assert(Math.abs( results(0) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }}

  test("Maximum-cache")  {
    val nco_verified_result = 309.7112
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
    println("Verified Result: " + nco_verified_result)
    assert(Math.abs(results(0) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }

  test("Maximum-local") { if(use_local_data) {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/merra_daily","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs, Map("response"->"xml"))
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
  }}

  test("Maximum-local-collection") { if(use_local_data) {
    val datainputs = s"""[domain=[{"name":"d0","level":{"start":3,"end":3,"system":"indices"}}],variable=[{"uri":"collection:/merra_daily","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs, Map("response"->"xml"))
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
  }}

  test("Maximum-dap") {
    val nco_verified_result = 309.7112
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
    println( "Verified Result: " + nco_verified_result )
    assert(Math.abs( results(0) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }

  //  test("Seasons-filter") {
  //    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":30,"end":50,"system":"indices"},"time":{"start":0,"end":200,"system":"indices"}}],variable=[{"uri":"file:///Users/tpmaxwel/.edas/cache/collections/NCML/giss_r1i1p1.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"xt","filter":"DJF"}]]"""
  //    val result_node = executeTest(datainputs)
  //    val result_data = getResultData( result_node, true )
  //    println( "Op Result:       " + result_data.toDataString )
  //  }

  test("pyTimeSum-dap") { if( test_python) {
//    ncwa -O -v tas -d lat,5,5 -d lon,5,10 -a time -y total ${datafile_agg} ~/test/out/minval.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 431859.2, 429501.2, 426810.8, 424555.2, 422398.5, 420567.7 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.sum","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",")  )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }}

  test("TimeAve-r1i1p1") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 229.7638, 228.6798, 227.2885, 226.3925, 224.6436, 224.0204 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":0,"end":611,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("TimeAve-r1i1p1-agg") {
    // ncwa -O -v tas -d lat,20,23 -d lon,30,33 -a time ${datafile_agg} ~/test/out/time_ave_agg.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 281.1322, 281.946, 282.5854, 282.8054, 282.4883, 282.9572, 283.7418, 284.1825, 284.4378, 284.7263, 285.1034, 285.3722, 286.3785, 286.4522, 286.4391, 286.4304 ).map(_.toFloat), Float.MaxValue )
    val edas_agg_result: CDFloatArray = CDFloatArray( Array( 281.13208, 281.94595, 282.58557, 282.80496, 282.48840, 282.95740, 283.74164, 284.18268, 284.43793, 284.72614, 285.10373, 285.37222, 286.37885, 286.45245, 286.43930, 286.43048 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":20,"end":23,"system":"indices"},"lon":{"start":30,"end":33,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Verified Result:     " + nco_verified_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Agg Result:     " + edas_agg_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Reduced Result: " + result_data.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("SpaceTimeAve-r1i1p1-agg") {
    // ncwa -O -v tas -a time,lat,lon ${datafile_agg} ~/test/out/spacetime_ave_agg.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  279.1232 ).map(_.toFloat), Float.MaxValue )
    val edas_agg_result: CDFloatArray = CDFloatArray( Array( 279.12314 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"tyx"}]]"""
    val result_node = executeTest(datainputs, Map( "profile" -> "true" ) )
    val result_data = getResultData( result_node )
    println( "Verified Result:     " + nco_verified_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Agg Result:     " + edas_agg_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Reduced Result: " + result_data.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

  test("SpaceTimeAve-r1i1p1-agg-weighted") {
    // ncwa -O -v tas -a time,lat,lon ${datafile_agg} ~/test/out/spacetime_ave_agg.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  288.2343 ).map(_.toFloat), Float.MaxValue )
    val edas_agg_result: CDFloatArray = CDFloatArray( Array( 288.22720 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"cosine","axes":"tyx"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Verified Result:     " + nco_verified_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Agg Result:     " + edas_agg_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "EDAS Reduced Result: " + result_data.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Max")
  }

//  test("Subset-Space-GISS-R1i1p1") {
//    //  ncks -O -v tas -d lat,25,25 -d lon,20,25 -d time,45,50 ${datafile} ~/test/out/subset.nc
//    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  295.16614, 296.34174, 297.53488, 297.4988, 294.6441, 293.97302, 293.59158, 294.0262, 291.21802, 293.24033, 296.05637, 295.29703, 293.64047, 293.54874, 293.35687, 293.26257, 289.24893, 289.4723, 290.362, 290.29272, 292.577, 292.59106, 292.4626, 292.46747, 292.91727, 293.47934, 293.3845, 290.05426, 291.79367, 291.86508, 291.78754, 291.81616, 291.47205, 291.9417, 291.8509, 291.57675, 291.33777, 291.24738, 291.3855, 291.25272, 290.93738, 290.94537, 290.81366, 288.7501, 290.57834, 287.44073, 287.43607, 290.55014, 283.41797, 289.5257, 290.5159, 289.8508, 286.65228, 290.10086, 289.72946, 289.0534, 286.04187, 282.6272, 278.97275, 284.91632, 285.72458, 288.6694, 283.28638, 284.18774, 285.35696, 284.68204, 282.79947, 278.32, 274.6654, 278.60153, 284.12213, 285.06912, 284.2621, 284.2931, 283.17007, 284.0533, 283.45163, 282.38916, 282.06033, 280.44333, 295.16614, 296.34174, 297.53488, 297.4988, 294.6441, 293.97302, 293.59158, 294.0262, 291.21802, 293.24033, 296.05637, 295.29703, 293.64047, 293.54874, 293.35687, 293.26257, 289.24893, 289.4723, 290.362, 290.29272 ).map(_.toFloat).sortWith(_ > _ ), Float.MaxValue )
//    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":25,"end":25,"system":"indices"},"lon":{"start":20,"end":25,"system":"indices"},"time":{"start":45,"end":50,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
//    val result_node = executeTest( datainputs )
//    val result_data: CDFloatArray = getResultData( result_node ).sortWith(_ > _ )
//    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
//    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
//    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
//  }

  test("Lowpass-GISS-R1i1p1") {
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":25,"end":25,"system":"indices"},"lon":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.lowpass","input":"v1","domain":"d0","groupBy":"5-year"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Size:         " + result_data.getStorageArray.length )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
  }

  test("Highpass-GISS-R1i1p1") {
    val datainputs =
      s"""  [     domain=[{"name":"d0","lat":{"start":-75,"end":75,"system":"values"},"time":{"start":"1850-01-30T00:00:00Z","end":"1856-07-28T00:00:00Z","system":"timestamps"}}],
         |        variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         |        operation=[{"name":"CDSpark.highpass","input":"v1","domain":"d0","grid":"uniform","shape":"15,36","res":"10,10","groupBy":"6-month"}]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
  }

  test("Highpass-GISS-R1i1p1-1") {
    val datainputs =
      s"""  [     domain=[{"name":"d0","lat":{"start":-30,"end":30,"system":"values"},"lon":{"start":0,"end":60,"system":"values"},"time":{"start":"1850-01-30T00:00:00Z","end":"1856-07-28T00:00:00Z","system":"timestamps"}}],
         |        variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         |        operation=[{"name":"CDSpark.highpass","input":"v1","domain":"d0","groupBy":"year"}]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
  }

  def find_spark_home: String = {
    import sys.process._
    val path = "which spark-submit" !!;
    new File(path).getParentFile.getParent
  }

  test("find_path") {
    print( find_spark_home );
  }


  test("Ave-1-Space-GISS-R1i1p1") {
    //  ncks -O -v tas -d lat,25,30 -d lon,20,25 -d time,45,50 ${datafile} ~/test/out/subset.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  289.0866, 290.5467, 292.9329, 294.6103, 295.5956, 294.7446   ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":25,"end":30,"system":"indices"},"lon":{"start":20,"end":25,"system":"indices"},"time":{"start":45,"end":50,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("Ave-Full-Space-GISS-R1i1p1") {
    // ncwa -O -d time,75,80 -a lat,lon  ${datafile} ~/test/out/spatial_average2.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  277.941, 279.8109, 280.9602, 281.2116, 280.7651, 279.4151   ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":75,"end":80,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.map(v=>f"$v%.5f").mkString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("SpaceAve-GISS-R1i1p1-weighted") {
    //  ncap2 -O -S cosine_weights.nco ${datafile} /tmp/data_with_weights.nc
    //  ncwa -O -w gw -d lat,5,25 -d lon,5,25 -d time,50,75 -a lat,lon /tmp/data_with_weights.nc ~/test/out/spatial_average_wts.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 276.6266, 274.5546, 272.6416, 270.9347, 270.3316, 270.1695, 270.4735, 271.177, 273.5099, 276.5793, 278.5962, 278.5879, 277.1897, 274.816, 272.8245, 271.4627, 270.4306, 270.0331, 270.1859, 271.4998, 273.4527, 276.2516, 278.1563, 277.8333, 276.4765, 274.7603 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":25,"system":"indices"},"lon":{"start":5,"end":25,"system":"indices"},"time":{"start":50,"end":75,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"cosine","axes":"xy"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("Subset-GISS-R1i1p1-window") {
    // ncks -O -v tas -d lat,5,7 -d lon,25,25 -d time,75,75 ${datafile} ~/test/out/subset.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 223.8638, 230.5135, 238.1273 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":7,"system":"indices"},"lon":{"start":25,"end":25,"system":"indices"},"time":{"start":75,"end":75,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("SpaceAve-GISS-R1i1p1-window-weighted") {
    // ncwa -O -d lat,5,7 -d lon,25,25 -d time,75,75 -a lat,lon  ${datafile} ~/test/out/spatial_average1.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 231.5538 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":7,"system":"indices"},"lon":{"start":25,"end":25,"system":"indices"},"time":{"start":75,"end":75,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"cosine","axes":"xy"}]]"""
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("SpaceAve-GISS-R1i1p1") {
    //  ncwa -O -d lat,5,25 -d lon,5,25 -d time,50,75 -a lat,lon ${datafile} ~/test/out/spatial_average.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(  270.0048, 267.3162, 264.9052, 263.4048, 262.913, 262.7695, 263.3018, 264.4724, 267.8822, 271.8264, 273.7054, 272.8606, 270.3697, 267.4805, 265.3143, 263.836, 262.907, 262.3552, 263.0375, 264.8206, 267.8294, 271.3149, 273.1132, 271.8285, 269.5949, 267.6493 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":5,"end":25,"system":"indices"},"lon":{"start":5,"end":25,"system":"indices"},"time":{"start":50,"end":75,"system":"indices"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"xy"}]]""".stripMargin
    val result_node = executeTest( datainputs )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.getStorageArray.mkString(",") )
    println( "Verified Result: " + nco_verified_result.getStorageArray.mkString(",") )
    assert( result_data.maxScaledDiff( nco_verified_result  )  < eps, s" Incorrect value computed for Ave")
  }

  test("ML-svd-GISS") {
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":-75,"end":75,"system":"values"},"time":{"start":"1990-01-01T00:00:00Z","end":"1994-12-31T23:59:00Z","system":"timestamps"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"},{"uri":"collection:/giss_r2i1p1","name":"tas:v2","domain":"d0"}],
         | operation=[
         |      { "name": "CDSpark.highpass", "input":"v1,v2", "grid":"uniform", "shape": "15,36", "res":"10,10", "id":"highpass", "groupBy":"6-month" },
         |      { "name":"SparkML.svd", "input":"highpass", "modes":"4" }]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
  }

  test("filter-GISS") {
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":-75,"end":75,"system":"values"},"time":{"start":"1990-01-01T00:00:00Z","end":"1999-12-31T23:59:00Z","system":"timestamps","filter":"DJF"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[
         |      { "name": "CDSpark.highpass", "input":"v1", "grid":"uniform", "shape": "15,36", "res":"10,10", "id":"highpass", "groupBy":"6-month" },
         |      { "name":"SparkML.svd", "input":"highpass", "modes":"4" }]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
  }

  test("svd-GISS") {
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":-75,"end":75,"system":"values"},"time":{"start":"1990-01-01T00:00:00Z","end":"1994-12-31T23:59:00Z","system":"timestamps"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{ "name":"SparkML.svd", "input":"v1", "modes":"4", "compu":"true", "grid":"uniform", "shape": "15,36", "res":"10,10" }]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
  }

  test("binning-yearlyAve-GISS") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(
      230.1202, 224.2958, 228.4658, 228.0224, 226.1936, 225.7275, 222.0484, 223.9207, 223.3873, 222.8198, 225.1187, 224.4428, 229.7138, 229.7007, 229.8149, 229.4356, 227.8259, 228.9415,
      231.1172, 229.9618, 228.9264, 228.0932, 227.1155, 226.0691, 227.9059, 227.116, 226.5818, 225.653, 224.769, 223.5814, 229.1446, 229.0689, 229.1144, 229.0135, 227.8756, 228.0897,
      223.9355, 228.5432, 228.4396, 227.1773, 223.7382, 225.177, 228.7011, 226.9322, 218.759, 218.5423, 225.1021, 224.3837, 229.1338, 227.1501, 229.3834, 228.9121, 228.2971, 228.9964,
      231.3791, 230.4205, 225.0711, 228.0521, 227.1422, 225.7934, 228.5478, 227.8226, 223.3534, 224.6234, 226.147, 225.1512, 228.3303, 228.9947, 228.601, 229.2599, 228.1443, 228.9194 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":5,"end":7,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":"1850-01-01T00:00:00Z","end":"1854-01-01T00:00:00Z","system":"timestamps"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{ "name":"CDSpark.ave", "axes":"t", "input":"v1", "groupBy":"year" }]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
  }

  test("1yearAve-GISS") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 230.1202, 224.2958, 228.4658, 228.0224, 226.1936, 225.7275, 222.0484, 223.9207, 223.3873, 222.8198, 225.1187, 224.4428, 229.7138, 229.7007, 229.8149, 229.4356, 227.8259, 228.9415 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":5,"end":7,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":"1850-01-01T00:00:00Z","end":"1851-01-01T00:00:00Z","system":"timestamps"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{ "name":"CDSpark.ave", "axes":"t", "input":"v1" }]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "saveLocalFile" -> "true" ) )
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for 1yearAve")
  }

  test("SpaceAve-GISS-R1i1p1-dates") {
    val result_vals = CDFloatArray( Array(  1.05339E7, 1.05777E7, 1.06215E7, 1.06653E7, 1.07091E7, 1.07529E7, 1.07967E7, 1.08405E7, 1.08843E7, 1.09281E7, 1.09719E7, 1.10157E7, 1.10595E7, 1.11033E7, 1.11471E7, 1.11909E7, 1.12347E7, 1.12785E7, 1.13223E7, 1.13661E7, 1.14099E7, 1.14537E7, 1.14975E7, 1.15413E7, 1.15851E7, 1.16289E7, 1.16727E7, 1.17165E7, 1.17603E7, 1.18041E7, 1.18479E7, 1.18917E7, 1.19355E7, 1.19793E7, 1.20231E7, 1.20669E7 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":5,"end":25,"system":"indices"},"lon":{"start":5,"end":25,"system":"indices"},"time":{"start":"1990-01-01T00:00:00Z","end":"1992-12-31T23:59:00Z","system":"timestamps"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         |  operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"xy"}]]""".stripMargin
    val result_node = executeTest( datainputs, Map( "response" -> "file" ) )
    val dataset = getResultDatasets( result_node ).headOption.getOrElse( throw new Exception( "Missing result data file, result node: " + result_node.toString() ) )
    dataset.getCoordinateAxes.find( _.getAxisType.getCFAxisName == "T" ) match {
      case Some(coordAxis) =>
        val timeAxis: CoordinateAxis1DTime = CoordinateAxis1DTime.factory(dataset, coordAxis, new Formatter())
        println( "Op Result time axis units:       " + timeAxis.getUnitsString )
        val nDates = timeAxis.getSize.toInt
        val time_values = (0 until nDates) map timeAxis.getCoordValue
        println( "Op Result times: " + (0 until nDates).map( iTime => timeAxis.getCalendarDate(iTime).toString ).mkString(", ") )
        println( "Op Result values: " + time_values.mkString(", ") )
        assert( result_vals.maxScaledDiff( CDFloatArray( time_values.map(_.toFloat).toArray, Float.MaxValue )  )  < eps, s" Incorrect value computed for Ave")
      case None => throw new Exception( "Missing Time Axis in data file: " + dataset.getLocation )
    }
  }

  test("StdDev-GISS") {
    // # NCO Verification script:
    //  datafile="collection:/giss_r1i1p1"
    //  ncks -O -v tas  -d lat,5,5 -d lon,5,10  -d time,0,500 ${datafile} ~/test/out/sample_data.nc
    //  ncwa -O -v tas -a time ~/test/out/sample_data.nc ~/test/out/time_ave.nc
    //  ncbo -O -v tas ~/test/out/sample_data.nc ~/test/out/time_ave.nc ~/test/out/dev.nc
    //  ncra -O -y rmssdn  ~/test/out/dev.nc ~/test/out/stdev.nc
    //  ncdump ~/test/out/stdev.nc

    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 8.977108, 9.206723, 9.441524, 9.263811, 9.883913, 10.13755 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":0,"end":500,"system":"indices"}}],
            variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
            operation=[ {"name":"CDSpark.stdDev","input":"v1","domain":"d0","axes":"t"} ]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for StdDev")
  }

  test("Anomaly-GISS-1") {

    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":5,"system":"indices"},"time":{"start":0,"end":50,"system":"indices"}}],
            variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
            operation=[ {"name":"CDSpark.anomaly","input":"v1","domain":"d0","axes":"t"} ]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
  }


  test("TimeAve-GISS-1") {
    // # NCO Verification script:
    //  datafile="collection:/giss_r1i1p1"
    //  ncks -O -v tas  -d lat,5,5 -d lon,5,10  -d time,0,500 ${datafile} ~/test/out/sample_data.nc
    //  ncwa -O -v tas -a time ~/test/out/sample_data.nc ~/test/out/time_ave.nc

    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 229.6116, 228.4243, 227.2998, 226.567, 224.8429, 224.1108 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":0,"end":500,"system":"indices"}}],
            variable=[ {"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"} ],
            operation=[ {"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t","id":"v1m"} ]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for StdDev")
  }

  test("TimeAve-GISS-roi-values") {
    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":-25,"end":25,"system":"values"},"lon":{"start":-120,"end":-60,"system":"values"},"time":{"start":0,"end":500,"system":"indices"}}],
            variable=[ {"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"} ],
            operation=[ {"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t","id":"v1m"} ]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op  Shape:       " + result_data.getShape.mkString(", ")  )
    println( "Op Result:       " + result_data.mkDataString(", ")  )
  }

  test("TimeDiff-GISS") {
    // # NCO Verification script:
    //  datafile="collection:/giss_r1i1p1"
    //  ncks -O -v tas  -d lat,5,5 -d lon,5,10  -d time,0,500 ${datafile} ~/test/out/sample_data.nc
    //  ncwa -O -v tas -a time ~/test/out/sample_data.nc ~/test/out/time_ave.nc
    //  ncbo -O -v tas ~/test/out/sample_data.nc ~/test/out/time_ave.nc ~/test/out/dev.nc
    //  ncdump ~/test/out/dev.nc

    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 13.99229, 14.34569, 14.92871, 14.9615, 15.70262, 15.52544, 12.26497, 12.68567, 12.18654, 11.67958, 11.54929, 10.82594, -1.039551, -0.6583405, 1.8862, 5.342392, 5.140457, 7.04039, -4.342102, -10.55997, -3.561218, -1.931107, -3.857086, -0.3062286 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[
            domain=[{"name":"d0","lat":{"start":5,"end":5,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"},"time":{"start":0,"end":500,"system":"indices"}}],
            variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
            operation=[       {"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t","id":"v1m"},
                              {"name":"CDSpark.eDiff","input":"v1,v1m","domain":"d0","id":"v1ss"} ]
          ]""".replaceAll("\\s", "")
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkBoundedDataString(", ", 16 )  )
    println( "Verified Result: " + nco_verified_result.mkBoundedDataString(", ", 16 ) )
    assert( result_data.sample(nco_verified_result.getShape(0)).maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for StdDev")
  }

  test("TimeAve-npana") { if(use_npana_data) {
    val datainputs = """[domain=[{"name":"d0","lat":{"start":10,"end":20,"system":"indices"},"lon":{"start":10,"end":20,"system":"indices"}},{"name":"d1","lev":{"start":5,"end":5,"system":"indices"}}],variable=[{"uri":"collection:/npana","name":"T:v1","domain":"d1"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result Data:       " + result_data.mkBoundedDataString(", ", 64) )
  }}

  test("TimeAve-GISS-2") {
    val datainputs = """[domain=[{"name":"d0","lat":{"start":10,"end":20,"system":"indices"},"lon":{"start":10,"end":20,"system":"indices"}}],variable=[{"uri":"collection:/GISS_r3i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result Data:       " + result_data.mkBoundedDataString(", ", 64) )
  }

  test("pyMaximum-dap") { if( test_python) {
    val nco_verified_result = 309.7112
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
    println( "Verified Result: " + nco_verified_result )
    assert(Math.abs( results(0) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }}


  test("CherryPick") { if(use_local_data) {
    val unverified_result: CDFloatArray = CDFloatArray( Array(267.78323,260.57275,257.5716,249.33249,242.7927 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":1,"end":1,"system":"indices"},"lat":{"start":10,"end":10,"system":"indices"},"lon":{"start":10,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/merra_daily","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.filter","input":"v1","plev":"975,875,775,650,550"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData(result_node)
    println( "Op Result:       " + result_data.mkDataString(", ") )
    assert( result_data.maxScaledDiff( unverified_result )  < eps, s" Incorrect value computed for Subset")
  }}

  test("Maximum1") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
  }

  test("Maximum2") {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Completed first execution, Result:       " + result_data.mkDataString(", ") )

    val result_node1 = executeTest(datainputs)
    val result_data1 = getResultData( result_node1 )
    println( "Completed second execution, Op Result:       " + result_data1.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data1.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")

  }

  //  test("Spatial Average Constant") {
  //    val nco_verified_result = 1.0
  //    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"","axes":"xy"}]]"""
  //    val result_node = executeTest(datainputs)
  //    logger.info( "Test Result: " + printer.format(result_node) )
  //    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "Data" \\ "LiteralData"
  //    val result_value = data_nodes.head.text.toFloat
  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  //  }
  //
  //  test("Weighted Spatial Average Constant") {
  //    val nco_verified_result = 1.0
  //    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","weights":"cosine","axes":"xy"}]]"""
  //    val result_node = executeTest(datainputs)
  //    logger.info( "Test Result: " + printer.format(result_node) )
  //    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "Data" \\ "LiteralData"
  //    val result_value = data_nodes.head.text.toFloat
  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  //  }
  //
  //  test("Spatial Average") {
  //    val nco_verified_result = 270.092
  //    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"","axes":"xy"}]]"""
  //    val result_node = executeTest(datainputs)
  //    logger.info( "Test Result: " + printer.format(result_node) )
  //    val data_nodes: xml.NodeSeq =  result_node \\ "Output" \\ "Data" \\ "LiteralData"
  //    val result_value = data_nodes.head.text.toFloat
  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  //  }
  //
  //  test("Weighted Spatial Average") {
  //    val nco_verified_result = 275.4043
  //    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.ave","input":"v1","domain":"d0","weights":"cosine","axes":"xy"}]]"""
  //    val result_node = executeTest(datainputs)
  //    logger.info( "Test Result: " + printer.format(result_node) )
  //    val data_nodes: xml.NodeSeq =  result_node \\ "Output" \\ "Data" \\ "LiteralData"
  //    val result_value = data_nodes.head.text.toFloat
  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  //  }

  test("Maximum-values") {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":50,"system":"indices"},"lon":{"start":180,"end":360,"system":"values"},"lat":{"start":0,"end":90,"system":"values"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs, Map( "response"->"file" ) )
    println( "Result: " + result_node.toString )
  }

  test("pyMaxT") { if( test_python) {
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.max","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
  }}

  //    test("pyMaxTCustom") {
  //      val nco_verified_result: CDFloatArray = CDFloatArray( Array( 275.95224, 277.0977, 277.9525, 278.9344, 280.25458, 282.28925, 283.88788, 285.12033, 285.94675, 286.6788, 287.6439 ).map(_.toFloat), Float.MaxValue )
  //      val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.maxCustRed","input":"v1","domain":"d0","axes":"t"}]]"""
  //      val result_node = executeTest(datainputs)
  //      val result_data = getResultData( result_node )
  //      println( "Op Result:       " + result_data.mkDataString(", ") )
  //      println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
  //      assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
  //    }

//  test("pyMaxTSerial") { if( test_python) {
//    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 277.8863, 279.0432, 280.0728, 280.9739, 282.2123, 283.7078, 284.6707, 285.4793, 286.259, 286.9836, 287.6983 ).map(_.toFloat), Float.MaxValue )
//    val datainputs = s"""[domain=[{"name":"d0","time":{"start":50,"end":150,"system":"indices"},"lon":{"start":100,"end":100,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"} }],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.maxSer","input":"v1","domain":"d0","axes":"t"}]]"""
//    val result_node = executeTest(datainputs)
//    val result_data = getResultData( result_node )
//    println( "Op Result:       " + result_data.mkDataString(", ") )
//    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
//    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Subset")
//  }}

  test("Minimum") {
    // ncwa -O -v tas -d time,50,150 -d lat,5,8 -d lon,5,8 -a time -y min ${datafile} ~/test/out/maxval.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 214.3339, 215.8409, 205.9775, 208.0006, 206.4181, 202.4724, 202.9022, 206.9719, 217.8426, 215.4173, 216.0199, 217.2311, 231.4988, 231.5838, 232.7329, 232.5641 ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":5,"end":8,"system":"indices"},"lon":{"start":5,"end":8,"system":"indices"},"time":{"start":50,"end":150,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
  }

  test("subsetTestXY1") {
    // ncks -O -v tas -d lat,10,15 -d lon,5,10 -d time,10,10  ${datafile} ~/test/out/subset.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array(   271.8525, 271.9948, 271.9691, 271.9805, 272.0052, 272.2418, 272.7861, 272.9485, 273.25, 273.4908, 273.5451, 273.45, 272.7733, 273.0835, 273.3886, 273.6199, 273.7051, 273.7632, 272.2565, 272.7566, 273.1762, 273.5975, 273.8943, 274.075, 272.4098, 272.8103, 273.2189, 273.6471, 273.8576, 274.0239, 273.3904, 273.5003, 273.667, 273.8236, 273.9353, 274.1161  ).map(_.toFloat), Float.MaxValue )
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"},"lat":{"start":10,"end":15,"system":"indices"},"lon":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d0"}]]"""
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result ) < eps, s" Incorrect value computed for Sum")
  }

  test("Max") {
    val nco_verified_result = 284.8936
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":10,"end":10,"system":"indices"},"lat":{"start":10,"end":20,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    val node_str = result_node.toString
    val results = getResults(result_node)
    println( "Op Result:       " + results.mkString(",") )
    println( "Verified Result: " + nco_verified_result )
    assert(Math.abs( results(0) - nco_verified_result) / nco_verified_result < eps, s" Incorrect value computed for Max")
  }

  test("Max3") {
    // ncwa -O -v tas -d time,10,10 -d lat,30.0,40.0  -a lon -y max ${datafile} ~/test/out/maxval1.nc
    val nco_verified_result: CDFloatArray = CDFloatArray( Array( 296.312, 294.3597, 293.7058, 292.8994, 291.9226 ).map(_.toFloat), Float.MaxValue )
    val datainputs =
      s"""[domain=[{"name":"d0","lat":{"start":30,"end":40,"system":"values"},"time":{"start":10,"end":10,"system":"indices"}}],
         | variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],
         | operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"x"}]]""".stripMargin
    val result_node = executeTest(datainputs)
    val result_data = getResultData( result_node )
    println( "Op Result:       " + result_data.mkDataString(", ") )
    println( "Verified Result: " + nco_verified_result.mkDataString(", ") )
    assert( result_data.maxScaledDiff( nco_verified_result )  < eps, s" Incorrect value computed for Min")
  }
}
class EDASTestSuite extends FunSuite with Loggable with BeforeAndAfter {
  EDASLogManager.testing
  import nasa.nccs.cdapi.tensors.CDFloatArray
  import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
  import ucar.nc2.dataset.NetcdfDataset
  val serverConfiguration = Map[String,String]()
  val webProcessManager = new ProcessManager( serverConfiguration )
  val shutdown_after = false
  val eps = 0.0001
  val service = "cds2"
  val run_args = Map("status" -> "false")
  val printer = new scala.xml.PrettyPrinter(200, 3)
  val test_data_dir = sys.env.get("EDAS_HOME_DIR") match {
    case Some(edas_home) => Paths.get( edas_home, "src", "test", "resources", "data" )
    case None => Paths.get("")
  }
  after {
    if(shutdown_after) { cleanup }
  }

  def readVerificationData( fileResourcePath: String, varName: String ): Option[CDFloatArray] = {
    try {
      val url = getClass.getResource( fileResourcePath ).toString
      logger.info( "Opening NetCDF dataset at url: " + url )
      val ncDataset: NetcdfDataset = NetcdfDataset.openDataset(url)
      val ncVariable = ncDataset.findVariable(varName)
      Some( CDFloatArray.factory(ncVariable.read(), Float.NaN) )
    } catch {
      case err: Exception =>
        println( "Error Reading VerificationData: " + err.getMessage )
        None
    }
  }

  def computeCycle( tsdata: CDFloatArray, cycle_period: Int ): CDFloatArray = {
    val values: CDFloatArray = CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    val counts: CDFloatArray = CDFloatArray( Array(cycle_period), Array.fill[Float](cycle_period)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize.toInt); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      values.augment( Array(index % cycle_period), val0 )
      counts.augment( Array(index % cycle_period),  1f )
    }
    values / counts
  }

  def computeSeriesAverage( tsdata: CDFloatArray, ave_period: Int, offset: Int = 0, mod: Int = Int.MaxValue ): CDFloatArray = {
    val npts = (tsdata.getSize / ave_period + 1).toInt
    val values: CDFloatArray = CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    val counts: CDFloatArray = CDFloatArray( Array(npts), Array.fill[Float](npts)(0f), Float.NaN )
    for (index <- (0 until tsdata.getSize.toInt); val0 = tsdata.getFlatValue(index); if tsdata.valid(val0) ) {
      val op_offset = (ave_period-offset) % ave_period
      val bin_index = ( ( index + op_offset ) / ave_period ) % mod
      values.augment( Array(bin_index), val0 )
      counts.augment( Array(bin_index), 1f )
    }
    values / counts
  }

  def   getDataNodes( result_node: xml.Elem, print_result: Boolean = false  ): xml.NodeSeq = {
    if(print_result) { println( s"Result Node:\n${result_node.toString}\n" ) }
    result_node.label match {
      case "response" =>
        result_node \\ "outputs" \\ "data"
      case _ =>
        result_node \\ "Output" \\ "LiteralData"
    }
  }

  def getResultData( result_node: xml.Elem, print_result: Boolean = false ): CDFloatArray = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    try{  CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue ) } catch { case err: Exception => CDFloatArray.empty }
  }

  def getResultDataArraySeq( result_node: xml.Elem, print_result: Boolean = false ): Seq[(Int,CDFloatArray)] = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
    data_nodes.map ( node => getNodeIndex(node) -> CDFloatArray( node.text.split(',').map(_.toFloat), Float.MaxValue ) ).sortBy( _._1 )
  }

  def getNodeIndex( node: xml.Node ): Int = node.attribute("id") match {
    case Some( idnode ) => idnode.text.split('.').last.toInt
    case None => -1
  }

  def getResults( result_node: xml.Elem ): Array[Float] = {
    val data_nodes: xml.NodeSeq = getDataNodes( result_node )
    val nnodes = data_nodes.length
    data_nodes.head.text.split(",").map(_.toFloat)
  }

  def getResultVariables( result_node: xml.Elem ): List[Variable] = {
    val variables = ListBuffer.empty[Variable]
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, false )
    for (data_node <- data_nodes; if data_node.label.startsWith("data")) yield data_node.attribute("file") foreach {
      filePath => variables += NetcdfDataset.openDataset(filePath.toString).findVariable("Nd4jMaskedTensor")
    }
    variables.toList
  }

  def getResultDatasets( result_node: xml.Elem ): List[NetcdfDataset] = {
    val datasets = ListBuffer.empty[NetcdfDataset]
    val data_nodes: xml.NodeSeq = getDataNodes( result_node, false )
    for (data_node <- data_nodes; if data_node.label.startsWith("data")) yield data_node.attribute("files") foreach {
      filePath => datasets += NetcdfDataset.openDataset(filePath.toString)
    }
    datasets.toList
  }

  def executeTest( datainputs: String, runArgs: Map[String,String]=Map.empty, async: Boolean = false, _processName: String = "" ): xml.Elem = {
    val t0 = System.nanoTime()
    val runargs = runArgs ++ Map( "responseform" -> "generic", "storeexecuteresponse" -> "true", "unitTest" -> "true", "status" -> async.toString )
    val rId: String = RandomStringUtils.random( 6, true, true )
    val process_name = if( _processName.isEmpty ) { "Workflow-" + rId } else _processName
    val executionCallback: ExecutionCallback = new ExecutionCallback {
      override def success( results: xml.Node ): Unit = {
        val responseType = runArgs.getOrElse("response","xml")
        logger.info(s" *** ExecutionCallback: rId = ${rId}, responseType = ${responseType} *** ")
        if (responseType == "file") { ; }
      }
      override def failure( msg: String ): Unit = { logger.error( s"ERROR CALLBACK ($rId): " + msg ) }
    }
    val ( rid, response ) = webProcessManager.executeProcess( service, Job( rId, process_name, datainputs, runargs, 1f ) )
    for( child_node <- response.child ) if ( child_node.label.startsWith("exception")) {
      throw new Exception( child_node.toString )
    }
    println("Completed test '%s' in %.4f sec".format(process_name, (System.nanoTime() - t0) / 1.0E9))
    response
  }

  def cleanup = webProcessManager.term

  def getCapabilities( identifier: String="", runArgs: Map[String,String]=Map.empty[String,String] ): xml.Elem = {
    val t0 = System.nanoTime()
    val response: xml.Elem = webProcessManager.getCapabilities(service, identifier, runArgs )
    webProcessManager.logger.info("Completed GetCapabilities '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    webProcessManager.logger.info( printer.format(response) )
    response
  }

  def describeProcess( identifier: String, runArgs: Map[String,String]=Map.empty[String,String] ): xml.Elem = {
    val t0 = System.nanoTime()
    val response: xml.Elem = webProcessManager.describeProcess(service, identifier, runArgs )
    webProcessManager.logger.info("Completed DescribeProcess '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
    webProcessManager.logger.info( printer.format(response) )
    response
  }
}

//class EDASDemoTestSuite extends FunSuite with Loggable with BeforeAndAfter {
//  EDASLogManager.testing
//  import nasa.nccs.cdapi.tensors.CDFloatArray
//  import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
//  import ucar.nc2.dataset.NetcdfDataset
//  val serverConfiguration = Map[String,String]()
//  val webProcessManager = new ProcessManager( serverConfiguration )
//
//  def executeTest( datainputs: String, status: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
//    val t0 = System.nanoTime()
//    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "status" -> status.toString, "unitTest" -> "true" )
//    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
//    val response: xml.Elem = webProcessManager.executeProcess("cds2", identifier, datainputs, parsed_data_inputs, runargs)
//    for( child_node <- response.child ) if ( child_node.label.startsWith("exception")) { throw new Exception( child_node.toString ) }
//    println("Completed test '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
//    response
//  }
//
//  def getResultData( result_node: xml.Elem, print_result: Boolean = false ): CDFloatArray = {
//    val data_nodes: xml.NodeSeq = getDataNodes( result_node, print_result )
//    try{  CDFloatArray( data_nodes.head.text.split(',').map(_.toFloat), Float.MaxValue ) } catch { case err: Exception => CDFloatArray.empty }
//  }
//
//  def getDataNodes( result_node: xml.Elem, print_result: Boolean = false  ): xml.NodeSeq = {
//    if(print_result) { println( s"Result Node:\n${result_node.toString}\n" ) }
//    result_node.label match {
//      case "response" => result_node \\ "outputs" \\ "data"
//      case _ => result_node \\ "Output" \\ "LiteralData"
//    }
//  }
//}
/*

@Ignore class EDASMainTestSuite extends TestSuite(0, 0, 0f, 0f ) with Loggable {
//  Collections.addCollection( "merra.test", merra_data, "MERRA data", List("ta") )
//  Collections.addCollection( "const.test", const_data, "Constant data", List("ta") )

  test("GetCapabilities") {
    val result_node = getCapabilities("collections")
  }

  test("DescribeProcess") {
    val result_node = describeProcess( "CDSpark.min" )
  }

  test("Aggregate") {
    val collection = "GISS_r1i1p1"
    val url=getClass.getResource(s"/collections/GISS/$collection.csv")
    val GISS_path = url.getFile
    val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$GISS_path"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
  }

  test("AggregateFiles") {
    val collection = "merra_daily"
    val path = "/Users/tpmaxwel/Data/MERRA/DAILY"
    val datainputs = s"""[variable=[{"uri":"collection:/$collection","path":"$path"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
  }

  test("Cache") {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/GISS_r2i1p1","name":"tas:v1","domain":"d0"}]]"""
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("CacheLocal") {
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}]]"""
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("Aggregate&Cache") {
    val index = 6
    val collection = s"GISS_r${index}i1p1"
    val GISS_path = s"/Users/tpmaxwel/Dropbox/Tom/Data/ESGF-CWT/GISS/$collection.csv"
    val datainputs = s"""[domain=[{"name":"d0"}],variable=[{"uri":"collection:/$collection","path":"${GISS_path}","name":"tas:v1","domain":"d0"}]]"""
    val agg_result_node = executeTest(datainputs,false,"util.agg")
    logger.info( "Agg Result: " + printer.format(agg_result_node) )
    val cache_result_node = executeTest(datainputs,false,"util.cache")
    logger.info( "Cache Result: " + printer.format(cache_result_node) )
  }

  test("EnsembleAve") {
    val variables = ( 1 to 6 ) map { index =>
      val collection = s"GISS_r${index}i1p1"
      val GISS_path = s"/Users/tpmaxwel/Dropbox/Tom/Data/ESGF-CWT/GISS/$collection.csv"
      s"""{"uri":"collection:/$collection","path":"${GISS_path}","name":"tas:v$index","domain":"d0"}"""
    }
    val vids = ( 1 to 6 ) map { index => s"v$index" }
    val datainputs = """[domain=[{"name":"d0"}],variable=[%s],operation=[{"name":"CDSpark.multiAverage","input":"%s","domain":"d0"}]]""".format( variables.mkString(","), vids.mkString(",") )
    logger.info( "Request datainputs: " + datainputs )
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    logger.info( "Sum1 Result: " + result_value.toString )
  }

  test("Sum") {
    val nco_verified_result = 4.886666e+07
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":0,"end":0,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/merra_daily","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Sum1") {
    val datainputs = s"""[domain=[{"name":"d0","time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    logger.info( "Sum1 Result: " + result_value.toString )
  }

  test("Sum Constant") {
    val nco_verified_result = 180749.0
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/const.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.sum","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("Maximum twice") {
    val nco_verified_result = 291.1066
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","result":"test_result","axes":"xy"}]]"""
    val result_node0 = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node0) )
    val result_node1 = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node1) )
    val data_nodes: xml.NodeSeq = result_node1 \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }
  test("SerializeTest") {
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.serializeTest","input":"v1","domain":"d0"}]]"""
    executeTest(datainputs) \\ "data"
  }
  test("Minimum") {
    val nco_verified_result = 239.4816
    val datainputs = s"""[domain=[{"name":"d0","lev":{"start":$level_index,"end":$level_index,"system":"indices"},"time":{"start":$time_index,"end":$time_index,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p2","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"xy"}]]"""
    val result_node = executeTest(datainputs)
//    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text
//    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
  }

  test("MinimumFragment") {
    val lat_index = 50
    val lon_index = 100
    val datainputs = s"""[domain=[{"name":"d1","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}}],variable=[{"uri":"fragment:/t|merra___daily|0,0,0,0|248,1,144,288","name":"t:v1","domain":"d1"}],operation=[{"name":"CDSpark.min","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_value = data_nodes.head.text.toFloat
  }

  test("OutOfBounds") {
    val lat_index = 50
    val lon_index = 100
    val lev_value = 75000
    val nco_verified_result = 239.4816
    val datainputs = s"""[domain=[{"name":"d1","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_value,"end":$lev_value,"system":"values"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
  }

  def getTimeseriesData( collId: String, varName: String, lon_index: Int, lat_index: Int, lev_index: Int): CDFloatArray = {
    val collection = new Collection( "aggregation", collId.replace('/','_'), "" )
    val cdvar = collection.getVariable(varName)
    val nTimesteps = cdvar.shape(0)
    val section: ma2.Section = new ma2.Section( Array(0,lev_index,lat_index,lon_index), Array(nTimesteps,1,1,1) )
    CDFloatArray( Array(nTimesteps), CDFloatArray.toFloatArray( collection.readVariableData( varName, section )), cdvar.missing )
  }

  test("Subset_Indexed_TS") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d0","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"},"lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values = data_nodes.head.text.split(",").map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val max_scaled_diff = result_array.maxScaledDiff( direct_result_array )
    printf( " \n\n        result, shape: " + result_array.getShape.mkString(",") + ", values: " + result_array.mkDataString(",") )
    printf( " \n\n direct result, shape: " + direct_result_array.getShape.mkString(",") + ", values: " + direct_result_array.mkDataString(",") )
    printf( "\n\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
  }

  test("Yearly Cycle") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d2","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.timeBin","input":"v1","result":"cycle","domain":"d2","axes":"t","bins":"t|month|ave|year"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values: Array[Float] = data_nodes.head.text.trim.split(' ').head.split(',').map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val computed_result = computeCycle( direct_result_array, 12 )
    val max_scaled_diff = result_array.maxScaledDiff( computed_result)
    printf( "    edas result: " + result_array.mkDataString(",") + "\n" )
    printf( "computed result: " + computed_result.mkDataString(",") + "\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect series computed for Yearly Cycle")
  }

  test("Workflow: Yearly Cycle Anomaly") {
    val lat_index = 50
    val lon_index = 100
    val lev_index = 0
    val direct_result_array = getTimeseriesData( "merra.test", "ta", lon_index, lat_index, lev_index )
    val datainputs = s"""[domain=[{"name":"d2","lat":{"start":$lat_index,"end":$lat_index,"system":"indices"},"lon":{"start":$lon_index,"end":$lon_index,"system":"indices"}},{"name":"d0","lev":{"start":$lev_index,"end":$lev_index,"system":"indices"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.timeBin","input":"v1","result":"cycle","domain":"d2","axes":"t","bins":"t|month|ave|year"},{"name":"CDSpark.eDiff","input":["v1","cycle"],"domain":"d2","axes":"t"}]]"""
    val result_node = executeTest(datainputs)
    logger.info( "Test Result: " + printer.format(result_node) )
    val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
    val result_values: Array[Float] = data_nodes.head.text.trim.split(' ').head.split(',').map( _.toFloat )
    val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
    val computed_result = computeCycle( direct_result_array, 12 )
    val max_scaled_diff = result_array.maxScaledDiff( computed_result )
    printf( "    edas result: " + result_array.mkDataString(",") + "\n" )
    printf( "computed result: " + computed_result.mkDataString(",") + "\n *** max_scaled_diff = " + max_scaled_diff )
    assert(max_scaled_diff < eps, s" Incorrect series computed for Yearly Cycle")
  }

  //  test("Subset(d0)") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some(nco_verified_result) =>
//        val datainputs = s"""[domain=[{"name":"d0","lat":{"start":$lat_value,"end":$lat_value,"system":"values"},"lon":{"start":$lon_value,"end":$lon_value,"system":"values"}}],variable=[{"uri":"collection:/merra.test","name":"ta:v1","domain":"d0"}],operation=[{"name":"CDSpark.subset","input":"v1","axes":"t"}]]"""
//        val result_node = executeTest(datainputs) \\ "data"
//        val result_values = result_node.text.split(",").map( _.toFloat )
//        val result_array = CDFloatArray( Array( result_values.length ), result_values, Float.MaxValue )
//        printf( "nco_verified_result: " + nco_verified_result.mkDataString(",") )
//        val max_scaled_diff = maxScaledDiff(result_array, nco_verified_result)
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
//      case None => throw new Exception( "Can't read verification data")
//    }
//  }


  //  test("Seasonal Cycle") {
  //    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
  //      case Some( nco_subsetted_timeseries ) =>
  //        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"3"), ( "mod"->"4"), ( "offset"->"2") )
  //        val result_values = computeArray("CDSpark.timeBin", dataInputs)
  //        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 3, 2, 4 )
  //        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
  //        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
  //        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Cycle")
  //      case None => fail( "Error reading verification data")
  //    }
  //  }


  //  test("Subset(d0)") {
  //    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
  //      case Some( nco_verified_result ) =>
  //        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
  //        val result_values = computeArray("CDSpark.subset", dataInputs)
  //        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
  //        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
  //        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
  //      case None => fail( "Error reading verification data")
  //    }
  //  }


//  test("Persistence") {
//    val dataInputs = getSubsetDataInputs( merra_data )
//    val request_context: RequestContext = getRequestContext( "CDSpark.metadata", dataInputs )
//    for( ospec <- request_context.inputs.values.flatten ) {
//      FragmentPersistence.deleteEnclosing(ospec)
//    }
//    val result_array1: CDFloatArray = computeArray("CDSpark.subset", dataInputs)
//    collectionDataCache.clearFragmentCache
//    val result_array2: CDFloatArray = computeArray("CDSpark.subset", dataInputs)
//    val max_diff = maxDiff( result_array1, result_array2 )
//    println(s"Test Result: %.4f".format( max_diff ) )
//    assert(max_diff == 0.0, " Persisted data differs from original data" )
//  }
//
//  test("Anomaly") {
//    readVerificationData( "/data/ta__0_0.nc", "ta" ) match {
//      case Some( nco_verified_result ) =>
//        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "axes"->"t") )
//        val result_values = computeArray("CDSpark.anomaly", dataInputs)
//        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Anomaly")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

//
//  test("Subset(d0) with secondary domain (d1)") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some( nco_verified_result ) =>
//        val time_index = 3
//        val verified_result_array = nco_verified_result.section( Array(time_index,0,0,0), Array(1,1,1,1) )
//        val dataInputs = getTemporalDataInputs(merra_data, time_index, ( "domain"->"d1") )
//        val result_values = computeArray("CDSpark.subset", dataInputs)
//        val max_scaled_diff = maxScaledDiff(result_values,verified_result_array)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), verified_result_array.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Subset")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

//
////  test("Variable Metadata") {
////    val dataInputs = getMetaDataInputs( "collection://MERRA/mon/atmos", "ta" )
////    val result_node = computeXmlNode("CDSpark.metadata", dataInputs)
////    result_node.attribute("shape") match {
////      case Some( shape_attr ) => assert( shape_attr.text == "[432 42 361 540]", " Incorrect shape attribute, should be [432 42 361 540]: " + shape_attr.text )
////      case None => fail( " Missing 'shape' attribute in result: " + result_node.toString )
////    }
////  }
//

//
//  test("Weighted Masked Spatial Average") {
//    val nco_verified_result = 275.4317
//    val dataInputs = getMaskedSpatialDataInputs(merra_data, ( "axes"->"xy"), ( "weights"->"cosine") )
//    val result_value: Float = computeValue("CDSpark.ave", dataInputs)
//    println(s"Test Result:  $result_value, NCO Result: $nco_verified_result")
//    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Weighted Masked Spatial Average")
//  }
//
//
//  test("Yearly Means") {
//    readVerificationData( "/data/ta_subset_0_0.nc", "ta" ) match {
//      case Some( nco_subsetted_timeseries ) =>
//        val dataInputs = getTemporalDataInputs(merra_data, 0, ( "unit"->"month"), ( "period"->"12") )
//        val result_values = computeArray("CDSpark.timeBin", dataInputs)
//        val nco_verified_result = computeSeriesAverage( nco_subsetted_timeseries, 12 )
//        val max_scaled_diff = maxScaledDiff(result_values, nco_verified_result)
//        println("Test Result: (%s)\n NCO Result: (%s)\n Max_scaled_diff: %f".format(result_values.toString(), nco_verified_result.toString(), max_scaled_diff))
//        assert(max_scaled_diff < eps, s" Incorrect timeseries computed for Yearly Ave")
//        assert( result_values.getSize == 11, "Wrong size result in Yearly Means")
//      case None => fail( "Error reading verification data")
//    }
//  }
//

}

//object MinimumTest extends App {
//  val nco_verified_result = 239.4816
//  val datainputs = s"""[domain=[{"name":"d0","lev":{"start":0,"end":0,"system":"indices"},"time":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"collection:/giss_r1i1p2","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.min","input":"v1","domain":"d0","axes":"xy"}]]"""
//  val result_node = executeTest(datainputs)
//  //    logger.info( "Test Result: " + printer.format(result_node) )
//  val data_nodes: xml.NodeSeq = result_node \\ "Output" \\ "LiteralData"
//  val result_value = data_nodes.head.text
//  //    assert(Math.abs(result_value - nco_verified_result) / nco_verified_result < eps, s" Incorrect value ($result_value vs $nco_verified_result) computed for Sum")
//
//  def executeTest( datainputs: String, async: Boolean = false, identifier: String = "CDSpark.workflow" ): xml.Elem = {
//    val t0 = System.nanoTime()
//    val runargs = Map("responseform" -> "", "storeexecuteresponse" -> "true", "async" -> async.toString )
//    val parsed_data_inputs = wpsObjectParser.parseDataInputs(datainputs)
//    val response: xml.Elem = webProcessManager.executeProcess(service, identifier, parsed_data_inputs, runargs)
//    webProcessManager.logger.info("Completed request '%s' in %.4f sec".format(identifier, (System.nanoTime() - t0) / 1.0E9))
//    response
//  }
//}*/

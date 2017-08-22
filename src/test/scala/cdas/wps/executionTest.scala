package edas.wps

import java.nio.file.Paths

import nasa.nccs.caching.{FragmentPersistence, collectionDataCache}
import nasa.nccs.cdapi.cdm.{CDSVariable, Collection}
import nasa.nccs.cdapi.tensors.CDFloatArray
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.esgf.wps.{ProcessManager, wpsObjectParser}
import org.scalatest._
import ucar.ma2

import scala.io.Source
import org.scalatest.Tag

// Execution Example (tag = yearly_cycle):
// >> sbt test-only *wpsSuite -J-Xmx16000M -- -n subset+cache
// Define frag id in ~/.edas/test_config.txt
// sample test_config.txt:
// fragment=t|merra/daily|0,0,0,0|248,42,144,288
/*
@Ignore     // casues this suite to be ignored by "sbt test".
class wpsSuite extends LocalExecutionTestSuite {
  val fragment = appParameters("sample.local.input")
  val frag_varname = fragment.split('|').head
  val frag_collection =  fragment.split('|')(1)
  val opendap_url = appParameters("sample.opendap.url")
  val collection_id = appParameters("sample.collection.id")
  val collection_varname = appParameters("sample.collection.variable")
  val collection_path = appParameters("sample.collection.path")
  val tstart = 0
  val tend = 420
  val level = 0
  val lat = -20f
  val lon = 0f

  test("op") {
    val datainputs = "[domain=[{\"name\":\"d1\",\"lev\":{\"start\":%d,\"end\":%d,\"system\":\"indices\"}}],variable=[{\"uri\":\"fragment:/%s\",\"name\":\"%s:v1\",\"domain\":\"d1\"}],operation=[{\"name\":\"%s\",\"input\":\"v1\",\"axes\":\"t\"}]]".format(level, level, operation, fragment, frag_varname)
    executeTest(datainputs)
  }
  test("anomaly_1D", Tag("anomaly")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDSpark.anomaly","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, frag_varname)
    executeTest(datainputs)
  }
  test("subset_1D", Tag("subset")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDSpark.subset","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, frag_varname)
    executeTest(datainputs)
  }
  test("subset_1D_cache", Tag("subset+cache")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d2","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("anomaly_1D_cache", Tag("subset+cache")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.anomaly","input":"v1","domain":"d2","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("binnedArray_1D_cache", Tag("subset+cache")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.timeBin","input":"v1","domain":"d2","axes":"t","bins":"t|month|ave|year"}]]""".format(lat, lat, lon, lon, level, level, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("average_1D", Tag("average")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDSpark.average","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, frag_varname)
    executeTest(datainputs)
  }
  test("subset_0D") {
    val datainputs = """[domain=[{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}},{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d3","time":{"start":"2006-06-18T10:00:00","end":"2006-06-18T10:00:00","system":"values"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d2,d3","axes":"t"}]]""".format(level, level, lat, lat, lon, lon, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("subset_empty") {
    val datainputs = """[domain=[{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}},{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d3","time":{"start":"2026-06-18T10:00:00","end":"2026-06-18T10:00:00","system":"values"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d2,d3","axes":"t"}]]""".format(level, level, lat, lat, lon, lon, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("subset_1Dts") {
    val datainputs = """[domain=[{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}},{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.subset","input":"v1","domain":"d2","axes":"t"}]]""".format(level, level, lat, lat, lon, lon, frag_collection, frag_varname)
    executeTest(datainputs)
  }
  test("yearly_cycle_1D", Tag("yearly_cycle")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDSpark.timeBin","input":"v1","axes":"t","unit":"month","period":"1","mod":"12"}]]""".format(lat, lat, lon, lon, level, level, frag_collection, frag_varname)
    val response = executeTest(datainputs)
  }
  test("timeseries_ave", Tag("tsave")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"},"lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"fragment:/%s","name":"%s:v1","domain":"d2"}],operation=[{"name":"CDSpark.average","input":"v1","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, fragment, frag_varname)
    val response = executeTest(datainputs)
  }
  test("createV", Tag("createV")) {
    val datainputs = """[domain=[{"name":"d2","lat":{"start":%.1f,"end":%.1f,"system":"values"},"lon":{"start":%.1f,"end":%.1f,"system":"values"}},{"name":"d1","lev":{"start":%d,"end":%d,"system":"indices"}}],variable=[{"uri":"collection:/%s","name":"%s:v1","domain":"d1"}],operation=[{"name":"CDSpark.timeBin","input":"v1","result":"cycle","domain":"d2","axes":"t","bins":"t|month|ave|year"},{"name":"CDSpark.diff2","input":["v1","cycle"],"domain":"d2","axes":"t"}]]""".format(lat, lat, lon, lon, level, level, collection_id, collection_varname)
    executeTest(datainputs)
  }
  test("MERRA_Collection", Tag("aggM")) {
    val datainputs = """[variable=[{"frag_collection":"%s","name":"%s","path":"%s"}]]""".format( frag_collection, frag_varname, collection_path )
    executeTest(datainputs,false,"util.agg")
  }
}
*/
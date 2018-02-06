package nasa.nccs.utilities

import java.lang.management.ManagementFactory

import org.apache.spark._
import org.apache.spark.util.CollectionAccumulator
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object TimeStamp {

  def getWorkerSignature: String = {
    val thread: Thread = Thread.currentThread()
    val node_name = ManagementFactory.getRuntimeMXBean.getName.split("[@]").last.split("[.]").head
    val worker_name = thread.getName.split("[-]").last
    s"${node_name}-E${SparkEnv.get.executorId}-W${worker_name}"
  }
}

case class TimeStamp( elapasedJobTime: Float, duration: Float, label: String ) extends Serializable with Ordered [TimeStamp] with Loggable {
  import TimeStamp._
  val tid = s"TimeStamp[${getWorkerSignature}]"
  val sval = s"TIME[${getWorkerSignature}] { ${elapasedJobTime.toString} ( ${duration.toString} )  => $label }"
  override def toString(): String = { sval }
  def compare (that: TimeStamp) = { elapasedJobTime.compareTo( that.elapasedJobTime ) }
}

object ProfilingTool extends Loggable {

  def apply( sparkContext: SparkContext ): ProfilingTool = {
    val startTimeMS: Long = System.currentTimeMillis()
    val starting_timestamp = new TimeStamp( 0f, 0f, "Job Start")
    val timestamps: CollectionAccumulator[TimeStamp] = new CollectionAccumulator[TimeStamp]()
    val profiler = new ProfilingTool( startTimeMS, timestamps )
    logger.info( s"Starting profiler in sparkContext '${sparkContext.applicationId}' with master '${sparkContext.master}' ")
    profiler.timestamp("Startup")
    profiler
  }
}

class ProfilingTool( val startTime: Long, timestamps: CollectionAccumulator[TimeStamp] ) extends Serializable with Loggable {
  private var _lastTime = startTime

  def timestamp( label: String, log: Boolean = false ): Unit = {
    val current = System.currentTimeMillis()
    val elapasedJobTime = (current-startTime)/1.0E3f
    val duration = (current-_lastTime)/1.0E3f
    timestamps.add( TimeStamp( startTime, duration, label ) )
    _lastTime = current
    if( log ) { logger.info(label) }
  }


  def getTimestamps: List[TimeStamp] = timestamps.value.toList.sortBy( _.elapasedJobTime )
  override def toString = " *** TIMESTAMPS: ***\n\n\t" + getTimestamps.map( _.toString() ).mkString("\n\t") + "\n\n"
}

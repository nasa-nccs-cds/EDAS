package nasa.nccs.edas.rdd
import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

class SlidingRDDPartition[T](val idx: Int, val prev: Partition, val tail: Seq[T], val offset: Int)
  extends Partition with Serializable {
  override val index: Int = idx
}

/**
  Spark SlidingRDD modified to truncate instead of drop the last window if it does not completely fit into the data range.
  */

class SlidingRDD[T: ClassTag](@transient val parent: RDD[T], val windowSize: Int, val step: Int) extends RDD[Array[T]](parent) {

  require(windowSize > 0 && step > 0 && !(windowSize == 1 && step == 1),
    "Window size and step must be greater than 0, " +
      s"and they cannot be both 1, but got windowSize = $windowSize and step = $step.")

  override def compute(split: Partition, context: TaskContext): Iterator[Array[T]] = {
    val part = split.asInstanceOf[SlidingRDDPartition[T]]
    (firstParent[T].iterator(part.prev, context) ++ part.tail)
      .drop(part.offset)
      .sliding(windowSize, step)
      .withPartial(true)
      .map(_.toArray)
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SlidingRDDPartition[T]].prev)

  override def getPartitions: Array[Partition] = {
    val parentPartitions = parent.partitions
    val n = parentPartitions.length
    if (n == 0) {
      Array.empty
    } else if (n == 1) {
      Array(new SlidingRDDPartition[T](0, parentPartitions(0), Seq.empty, 0))
    } else {
      val w1 = windowSize - 1
      // Get partition sizes and first w1 elements.
      val (sizes, heads) = parent.mapPartitions { iter =>
        val w1Array = iter.take(w1).toArray
        Iterator.single((w1Array.length + iter.length, w1Array))
      }.collect().unzip
      val partitions = mutable.ArrayBuffer.empty[SlidingRDDPartition[T]]
      var i = 0
      var cumSize = 0
      var partitionIndex = 0
      while (i < n) {
        val mod = cumSize % step
        val offset = if (mod == 0) 0 else step - mod
        val size = sizes(i)
        if (offset < size) {
          val tail = mutable.ListBuffer.empty[T]
          // Keep appending to the current tail until it has w1 elements.
          var j = i + 1
          while (j < n && tail.length < w1) {
            tail ++= heads(j).take(w1 - tail.length)
            j += 1
          }
          if (sizes(i) + tail.length >= offset + windowSize) {
            partitions += new SlidingRDDPartition[T](partitionIndex, parentPartitions(i), tail, offset)
            partitionIndex += 1
          }
        }
        cumSize += size
        i += 1
      }
      partitions.toArray
    }
  }

  // TODO: Override methods such as aggregate, which only requires one Spark job.
}

package nasa.nccs.edas.rdd

case class CDTimeSlice( year: Short, month: Byte, day: Byte, hour: Byte, missing: Float, data: Array[Float] )

case class FileInput( path: String )
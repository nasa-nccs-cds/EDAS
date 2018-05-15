package nasa.nccs.cdapi.cdm

import nasa.nccs.utilities.Loggable
import ucar.nc2.dataset.{CoordinateAxis, NetcdfDataset}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CoordAxis {
  def apply( axis: CoordinateAxis ): CoordAxis = {
     CoordAxis( axis.getShortName, axis.getAxisType.getCFAxisName, axis.getUnitsString, axis.getMaxValue, axis.getMinValue, axis.getShape )
  }
}

case class CoordAxis( name: String, ctype: String, units: String, maxVal: Double, minVal: Double, shape: Array[Int] ) extends Serializable with Loggable {

}

object Grid extends Loggable {
  def apply( fileResourcePath: String ): Grid = {
    try {
      val url = getClass.getResource(fileResourcePath).toString
      val ncDataset: NetcdfDataset = NetcdfDataset.openDataset(url)
      val axes = ncDataset.getCoordinateAxes.map( cAxis => CoordAxis( cAxis ) )
      new Grid( axes.toList )
    }
  }
}


class Grid( val axes: List[CoordAxis] ) extends Serializable with Loggable {

}

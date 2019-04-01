package nasa.nccs.cdapi.cdm

import nasa.nccs.utilities.Loggable
import ucar.nc2.Dimension
import ucar.nc2.dataset.{CoordinateAxis, NetcdfDataset}
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CoordAxis {
  def apply( axis: CoordinateAxis ): CoordAxis = {
     val data = axis.read()
     CoordAxis( axis.getShortName, axis.getAxisType.getCFAxisName, axis.getUnitsString, axis.getMaxValue, axis.getMinValue, axis.getShape, axis.getDimensions.toList.map( d => Dim(d)) )
  }
}

case class CoordAxis( name: String, ctype: String, units: String, maxVal: Double, minVal: Double, shape: Array[Int], dims: List[Dim] ) extends Serializable with Loggable {

}

object Dim {
  def apply( dim: Dimension ): Dim = {
    new Dim( dim.getShortName, dim.getDODSName, dim.getFullName, dim.getLength )
  }
}

case class Dim( name: String, dodsName: String, fullName: String, size: Int ) extends Serializable with Loggable {

}

object Grid extends Loggable {
  def apply( fileResourcePath: String ): Grid = {
      val url = getClass.getResource(fileResourcePath).toString
      val ncDataset: NetcdfDataset = NetcdfDataset.openDataset(url)
      val axes = ncDataset.getCoordinateAxes.map( cAxis => CoordAxis( cAxis ) )
      new Grid( axes.toList )
  }
}


class Grid( val axes: List[CoordAxis] ) extends Serializable with Loggable {
  val axisTypes = List( "T", "Z", "Y", "X" )

  def getDims: Map[String,Dim] = {
    val dimsMap = HashMap.empty[String,Dim]
    for( axis <- axes; dim <- axis.dims ) { dimsMap += dim.name -> dim }
    dimsMap.toMap
  }

  def getAxis( aType: String ): Option[CoordAxis] = axes.find( _.ctype == aType )

  def getAxes: List[(CoordAxis,Int)] = {
    axisTypes.flatMap( atype => getAxis( atype ) ).zipWithIndex
  }
}

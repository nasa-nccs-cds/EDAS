package cdas.wps

import nasa.nccs.edas.sources.netcdf.NetcdfDatasetMgr

/**
  * Created by tpmaxwel on 3/4/18.
  */
class SyntheticDataGenerator {

  def main(args : Array[String]): Unit = {
    val gridFile = args(0)
    val gridDS = NetcdfDatasetMgr.aquireFile( gridFile, 17.toString )
  }

}

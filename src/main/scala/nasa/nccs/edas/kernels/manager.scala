package nasa.nccs.edas.kernels
import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.edas.workers.python.PythonWorkerPortal

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import nasa.nccs.utilities.{Loggable, cdsutils}

import scala.collection.immutable.Map

class KernelMgr(  ) {

  val kernelModules: Map[String,KernelModule] = KernelPackageTools.getKernelModuleMap()

  def getModule( moduleName: String ): Option[KernelModule] = kernelModules.get( moduleName.toLowerCase )

  def getModuleNames: List[String] = kernelModules.keys.toList

  def toXml = <modules>{ kernelModules.values.map( _.toXml ) } </modules>

  def getModulesXml = {
    val elemList: Array[xml.Elem] = kernelModules.values.map( _.toXml ).toArray
    <kernels>{ elemList }</kernels>
  }

  def getKernelMap: Map[String,Kernel] =
    Map( kernelModules.values.flatMap( _.getKernels ).map( k => k.identifier.toLowerCase -> k ).toSeq: _* )

  def getKernelMap( visibility: String = "" ): Map[String,Kernel] = {
    val visLevel = KernelStatus.parse(visibility)
    Map(kernelModules.values.map( _.filter( visLevel ) ).flatMap( _.getKernels ).map(k => k.identifier.toLowerCase -> k).toSeq: _*)
  }

}

object KernelPackageTools extends Loggable {
  import com.google.common.reflect.ClassPath
  val internalKernelsPackage = "nasa.nccs.edas.modules"
  val externalKernelPackages = cdsutils.envList("EDAS_KERNEL_PACKAGES")
  val classpath = ClassPath.from( getClass.getClassLoader )
  val kernelPackagePaths: List[String] = List( internalKernelsPackage ) ++ externalKernelPackages

  def getKernelClasses: List[ClassPath.ClassInfo] = {
    kernelPackagePaths.flatMap( package_path => classpath.getTopLevelClassesRecursive( package_path ).toList )
  }

  def getKernelModuleMap( visibility: String = "" ): Map[String,KernelModule] = {
    val visLevel = KernelStatus.parse( if( visibility.isEmpty ) { appParameters("kernels.visibility","public") } else visibility )
    val internal_kernels: Map[String,KernelModule] = getKernelClasses.map(ClassInfoRec( _ )).groupBy( _.module.toLowerCase ).mapValues( KernelModule(_) filter visLevel )
    val capabilities_data = PythonWorkerPortal.getInstance().getCapabilities()
    val python_kernel_modules: Array[KernelModule] = capabilities_data map ( KernelModule(_)  )
    logger.info( " #PK# Loading python kernels from capabilities data: \n" + python_kernel_modules.map( km => s"** Kernel Module ${km.name}:\n" + km.getKernels.map( _.toString ).mkString( "\n\t") ).mkString("\n") )
    val external_kernel_map: Map[String,KernelModule] = Map( python_kernel_modules.map( km => km.getName -> km.filter(visLevel) ): _* )
    logger.info( " #PK# External Kernels: " + external_kernel_map.keys.mkString( ", "))
    ( internal_kernels ++ external_kernel_map ) filter { case (_,kmod) => kmod.nonEmpty }
  }

}





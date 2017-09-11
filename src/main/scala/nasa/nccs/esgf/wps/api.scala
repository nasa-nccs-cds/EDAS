package nasa.nccs.esgf.wps

import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.utilities.Loggable

class APIManager( serverConfiguration: Map[String,String] ) extends Loggable {

  val providers = Map( ("edas", edasServiceProvider) )
  val default_service = edasServiceProvider
  appParameters.addConfigParams( serverConfiguration )

  def getServiceProvider(service: String = ""): ServiceProvider = {
    providers.getOrElse(service,default_service)
  }
}

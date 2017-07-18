package nasa.nccs.esgf.wps

import nasa.nccs.edas.utilities.appParameters
import nasa.nccs.utilities.Loggable

class APIManager( serverConfiguration: Map[String,String] ) extends Loggable {

  val providers = Map( ("edas", cds2ServiceProvider) )
  val default_service = cds2ServiceProvider
  appParameters.addConfigParams( serverConfiguration )

  def getServiceProvider(service: String = ""): ServiceProvider = {
    providers.getOrElse(service,default_service)
  }
}

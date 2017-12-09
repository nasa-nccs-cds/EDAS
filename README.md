###                                EDAS Project

_Earth Data Analytic Services provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  Prerequisite: Install the Java/Scala develpment tools:

    1) Java SE Platform (JDK) 1.7:   http://www.oracle.com/technetwork/indexes/downloads/index.html
    2) Scala:                        http://www.scala-lang.org/download/install.html
    3) Scala Build Tool (sbt):       http://www.scala-sbt.org/0.13/docs/Setup.html

####  Install and run EDAS:

    1. Checkout the EDAS sources:

        >> cd <prefix>
        >> git clone https://github.com/nasa-nccs-cds/EDAS.git 
        
    2. Configure the EDAS server (please note that the build process copies the edas.properties file to the cache dir, which defaults to ~/.edas/cache.  The EDAS app will access it from there):
    
        >> cd EDAS/project
        >> cp edas.properties.template edas.properties
        >> emacs edas.properties
        
    3. Parameter Descriptions
        
        inputs.methods.allowed:  Comma-separated list of allowed input methods, possible values:  collection file  http
        kernels.visibility:      Kernel visibility level, possible values:  experimental developmental restricted public
        spark.master:            Address of Spark master node, examples:  spark://cldralogin101:7077   local[4]
        num.cluster.nodes:       Number of nodes in your cluster (defaults to 1)
        partition.size           Optimal spark partition size for your cluster, recommended value:  200m
        record.size              Optimal spark record size for your cluster, recommended value:  200m
        parts.per.node           Number of cores to be utilized on each worker node of your cluster   
        wps.shared.data.dir      Denotes the path of a data directory that is shared between the EDAS server and portal.  Should not be included if a shared directory does not exist.
        
    4. Optional EDAS environment variable:
        EDAS_CACHE_DIR:             EDAS Cacahe dir (defaults to ~/.edas/cache).
        EDAS_UNMANAGED_JARS:        Directory containing external jars for kernel development
        YARN_CONF_DIR:              Yarn config directory (Yarn usage discouraged- recommend running Spark in standalone mode).

    5. Build the application (for a clean build one can execute "sbt clean" before "sbt package"):

        >> source <prefix>/EDAS/bin/setup_runtime.sh
        >> install_netcdf_jar.sh
        >> cd EDAS
        >> sbt package

    6. Run unit tests:

        >> sbt test

    8. Startup the EDAS server:
     
        >> cd EDAS
        >> ./bin/startup_edas_local.sh

    9. Access demos:

        Designed to be deployed with the CDWPS framework (https://github.com/nasa-nccs-cds/CDWPS)

####  Python/NetCDF support through Conda::

    1) Install Anaconda: https://github.com/UV-CDAT/uvcdat/wiki/Install-using-Anaconda
    
    2) Create EDAS conda environment:
        
        >> conda create -n edas uvcdat -c conda-forge -c uvcdat -c acme 
        >> conda install pyzmq psutil lxml requests urllib3
        
    3) Initialize shell enviromnment for edas:
    
        >> source <prefix>/EDAS/bin/setup_runtime.sh
        >> source activate edas
        
    4) Build EDAS python pacakges:
    
        >> cd EDAS
        >> python setup.py install

####  Code development:

    1) Install IntelliJ IDEA CE from https://www.jetbrains.com/idea/download/ with Scala plugin enabled.
    
    2) Start IDEA and import the EDAS Project from Version Control (github) using the address https://github.com/nasa-nccs-cds/EDAS.git.
        
    
        

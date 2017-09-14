###                                EDAS Project

_Earth Data Analytic Services provider built on scala, Spark, Akka, Haddop, and python tools such as UVCDAT, etc._

####  Prerequisite: Install the Java/Scala develpment tools:

    1) Java SE Platform (JDK) 1.7:   http://www.oracle.com/technetwork/indexes/downloads/index.html
    2) Scala:                        http://www.scala-lang.org/download/install.html
    3) Scala Build Tool (sbt):       http://www.scala-sbt.org/0.13/docs/Setup.html

####  Install and run EDAS:

    1) Checkout the EDAS sources:

        >> cd <prefix>
        >> git clone https://github.com/nasa-nccs-cds/EDAS.git 
        
    2) Configure the EDAS server:
    
        >> cd EDAS/project
        >> cp edas.properties.template edas.properties
        >> emacs edas.properties

    3) Build the application (for a clean build one can execute "sbt clean" before "sbt package"):

        >> cd EDAS
        >> sbt package

     4) Run unit tests:

        >> sbt test

     5) Source the setup file to configure the runtime environment:

        >> source <prefix>/EDAS/bin/setup_runtime.sh

     6) Startup the EDAS server:
     
        >> cd EDAS
        >> ./bin/startup_edas_local.sh

     7) Access demos:

        Designed to be deployed with the CDWPS framework (https://github.com/nasa-nccs-cds/CDWPS)

####  Python/NetCDF support through Conda::

    1) Install Anaconda: https://github.com/UV-CDAT/uvcdat/wiki/Install-using-Anaconda
    
    2) Create EDAS conda environment:
        
        >> conda create -n edas -c conda-forge -c uvcdat uvcdat pyzmq psutil lxml
        
    3) Initialize shell enviromnment for edas:
    
        >> source <prefix>/EDAS/bin/setup_runtime.sh
        >> source activate edas
        
    4) Build EDAS python pacakges:
    
        >> cd EDAS
        >> python setup.py install

####  Code development:

    1) Install IntelliJ IDEA CE from https://www.jetbrains.com/idea/download/ with Scala plugin enabled.
    
    2) Start IDEA and import the EDAS Project from Version Control (github) using the address https://github.com/nasa-nccs-cds/EDAS.git.
        
    


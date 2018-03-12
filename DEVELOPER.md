##                                EDAS Project Developer Notes

_Earth Data Analytic Service provider built on scala, java, Spark, and python tools such as UVCDAT, etc._

###  Configuration

The EDAS environment is initialized by sourcing the **{EDAS}/bin/setup_runtime.sh** script.

####  Environment variables:
The following environment variables can be set to customize the environment:

    * EDAS_CACHE_DIR: Sets the location of the EDAS cache directory (default: ~/.edas/cache).
    * CDWPS_HOME_DIR: Sets the location of the CDWPS home directory (default: {EDAS}/../CDWPS).
    * CDSHELL_HOME_DIR: Sets the location of the CDSHELL home directory (default: {EDAS}/../EDASClientConsole).

####  Configuration parameters:
During the EDAS build process a copy of the file _edas.properties_ is copied to the EDAS cache directory.
    Edit this file to customize the EDAS installation. 
    
Here are descriptions of the currently active parameters:
     
     * wps.response.syntax: Determines the syntax of the WPS responses.  The possibilities are:
        - wps: Use syntax conforming to the wps schema.
        - generic:  Use a simpler (and easier to parse) generic format.
     * wps.server.proxy.href: Http address used to query the server for results (e.g. http://localhost:9001)
     * procs.maxnum: The maximum number of processers that EDAS ia allowed to utilize.
     * record.maxsize: The maximum memory size of a Spark RDD record (i.e. size of the array data partition).
     * ncml.recreate: When set to true the server will recreate the NCML files associated with all registered collections.  When false (the default) it will use the existing NCML files.
     * spark.log.level: Spark framework log level: "WARN", "ERROR", "INFO", or "DEBuG" (note this is separate from the edas log level).    

###  Kernel Development

The use may contribute new analysis modules (kernels) developed in java, scala, or python.  

#### Python Kernels
Here are some pointers on developing new python kernels. Some example code is displayed below.

    1. Create a new python file called {moduleName}.py under {EDAS}/src/pyedas/kernels/internal. All kernels defined in this file will be automatically registered in WPS under a KernelModule named {moduleName}. In a future version it will be possible to locate this file outside of {EDAS}.
    2. Create a class that extends either Kernel (for numpy operations) of CDMSKernel (for cdms2 operations) from pyedas.kernels.Kernel.  
    3. An example Kernel definition, and a corresponding WPS request, are shown below.  One can also take a look at any of the existing python files in the pyedas/kernels/internal directory.
    4. Configure the Kernel by passing a pyedas.kernels.Kernel.KernelSpec instance to the __init__ method.  
    5. The arguments of the KernelSpec define the kernelId, title, description, and configuration parameters for the Kernel.  The configuration parameters are discussed below in the parallelization section.
    6. Define the kernel's executeOperation method to define an analytic operation with a single input.  Alternately, override the kernel's executeOperations method to define an analytic operation with multiple inputs.
    7. The execute method's 'input' argument provides input data arrays in various formats:
         input.array():        numpy ndarray
         input.getVariable():  cdms2 Variable instance
    8. Parameters from the WPS 'operation' specification (e.g. "axes":"xy") can be accessed using the task.metadata dictionary.
    9. The createResult method (on either CDMSKernel or npArray) creates a properly formatted kernel result.
    10. The Kernel is referenced in the WPS request (see below) using the id "python.{moduleName}.{kernelId}"
    
##### Python kernel example code (from file {EDAS}/src/pyedas/kernels/internal/cdmsModule.py).

```python
from pyedas.kernels.Kernel import CDMSKernel, KernelSpec
import cdutil

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", parallelize=True ) )

    def executeOperation(self, task, input):
        variable = input.getVariable()
        axis = task.metadata.get("axis","xy")
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        return self.createResult( result_var, _input, task )

```
##### Corresponding WPS request
```
...datainputs=[
    domain=[ {"name":"d0","time":{"start":0,"end":10,"system":"indices"}} ],
    variable=[ {"uri":"collection:/giss_r1i1p1","name":"tas:v1","domain":"d0"} ],
    operation=[ {"name":"python.cdmsModule.ave","input":"v1","domain":"d0","axes":"xy"} ]
    ]
    
```

##### Python Kernel Parallelization
The configuration parameters defined in the KernelSpec specify how EDAS will handle the parallelization of the Kernel.   Python Kernels can be 
either parallelizable _(parallelize=True)_ or non-parallelizable _(parallelize=False)_.  If a kernel is non-parallelizable then EDAS assumes that the kernel will either run serially or handle
its own parallelization internally.  If a kernel is parallelizable then EDAS will handle the parallelization.  EDAS parallelization occurs as follows:

    1. EDAS partitions the input into N fragments by splitting the data over time into N non-overlapping continuous time segments of approx equal length.
    2. EDAS create N copies of the kernel and calls the N executeOperation(s) methods in parallel.  Each method call is passed a different input fragment of the same variable(s).
    3. The kernel executions produce N result fragments whcih are then combined to produce the final result.
    4. The combination of fragments proceeds as follows:
        - If the set of axes over which the operation is performed (as determined by the operation's "axes" parameter) does not include 't' then the fragments are simply concatenated (maintaining the time ordering) with no reduction.
        - If the operation's axes set does include 't' then a merge operation is performed that is determined by the kernel's 'reduceOp' configuration parameter.
        - If the 'reduceOp' configuration parameter specifies one of the builtin reduction operators then that operation is used to combine the result fragments.
        - If the 'reduceOp' parameter value is "custom" then the python kernel class must implement the 'reduce' method (from the 'Kernel' base class) and that method is then used to combine the result fragments.

##### Python Kernel Data Input
By default the python kernels are passed data input arrays that are injested and subsetted by EDAS using it's caching framework.
However, using the handlesInput configuration parameter a kernel developer can specify that the python kernel should
perform its own data access  _(handlesInput=True)_.   In this case the EDAS data injest and caching framework will be bypassed
and a data access URI will be passed to the kernel.  Until further notice _(handlesInput=True)_ implies _(parallelize=False)_.
 For an example please see the AverageKernel in the cdmsExt KernelModule.

###  Rebuilding

After modifying the EDAS source code (or pulling a new snapshot from github), the framework can be rebuilt using some or all of the 
commands in the **{EDAS}/bin/update.sh** script.

###  Distribution

####  Updating the python distribution:

    1) Push a github tag for version x.x:
    
        >> git push origin HEAD                   # Push any existing commits
        >> git push origin :refs/tags/x.x         # Delete the remote tag if it already exists
        >> git tag -fa x.x                        # Tag the current HEAD
        >> git push --tags origin master          # Pust the tag to origin
        
    2) Edit <version> and <download_url> in setup.py with new version tag x.x
    
    3) Upload new dist to pypi:
     
        >> python setup.py sdist upload -r pypi
        
    4) Build and upload conda package:
    
        >> conda skeleton pypi pyedas       # Update the conda build skeleton with the new pyedas version number
        >> conda build pyedas               # Builds the conda package and prints the <build-path>
        >> anaconda login
        >> anaconda upload <build-path>
        >> anaconda logout
        
        
#### Workflow Dynamics

    1) A workflow links a set of WorkflowNodes, ead representing a Kernel, into a processing pipeline.
    2) To compute partitioning domains, note that:
        A) Operations that eliminate the time axis do not propagate the partitioning.  Ignoring these links Allows us to split the 
            workflow into a set of subworkflows (SWFs), which can be independently partitioned.
        B) SWFs that shage external inputs should share the same partitioning.  These SWFs are merged.
    3) A list of SWF root nodes is generated and sorted by pipeline dependency, predecessors before antecedents.
    4) For each subworkflow a partitioning is computed as follows:
        A) Gather all external inputs to all nodes of the SWF.
        B) Choose the largest domain of all the inputs and use it to compute a data partitioning (which maps data time 
        spans to partitions).  Apply this partitioning to all inputs.
    5) Compute batches as a function of the input data size and Spark partition size limits.  Batches are processed sequentially
        to prevent memory overflow. 
    6) Each SWF is processed in order.  For each SWF an execution pipeline is constructed by starting from the root node and 
        propagating backward through the dependency tree.   The pipeline is executed using parallel streaming map operations followed by
        a final reduce and cache to generate a product for use in subsequent SWF processing. 
  
 #### Data Collections
 
    1) A collection is defined as a set of files that all have the smae variables and axes.
    2) An aggregation is defined as a set of cleectsion for which all variables are unique (no sets of distinct variables defined with the same name)
    3) The script "mkcol" is used to create aggregations.  
        * Usage:  "mkcol -d <bifurcation_depth> <aggregation_name> <aggregation_root_path>". 
        * The "bifurcation_depth" is the depth in the direcory tree beneath the aggregation_root_dir at which the separate collections split off.
    4) The script "mkcols" is used to create a separate aggregation for each subdirectory of a specified master directory.       
    
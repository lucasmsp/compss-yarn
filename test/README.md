
# Application test

This dummy test application is a produce/consumer case where the consumer must sleep for a value of time passed by a parameter in each *n* task. The producer generates a list of time to wait based on each partition based on a random number between *min_time* and *max_time*, also passed as parameter. Example to run in a default COMPSs environment:

```bash
runcompss --python_interpreter=python3 test.py <num_fragments> <min_time> <max_time>
```


## Project and resources files

In order to submit a COMPSs application in Yarn, first, you need to prepare *XML* project and resources files. 

On the project file, update the `test_p_yarn.xml` by specifying:

* The minimum and maximum containers to be created by COMPSs; 
* The *yarn-docker-network-name* with the Docker network to be used by containers (default is `bridge`); 
* The *image name* to be used by COMPSs, one can use more than one image;
* The contaniner user (*vm-user*) that will execute the application inside the docker image;
* The path to the application package, this package will be sent to all containers based on the source and target folder specifications.

On the resource file, update the `test_r_yarn.xml` by specifying:

* The *ConnectorJar* to `yarn-conn.jar`;
* The *ConnectorClass* to `br.ufmg.dcc.conn.yarn.Yarn`;
* The *Endpoint Server* of the Yarn Cluster, which must have a format of `yarn://<master_compss_node_ip>`;
* All *Image Name* to be used by COMPSs;
* The list of possible instances resources (Processor, Memory, etc). Currently, storage size is not take in count.


## Quickstart

Submit a COMPSs application in a Yarn Cluster is similar to the official Mesos's COMPSs connector:

1. Create a package `tar.gz` with `tar -czvf test.tar.gz test.py`;
2. Copy the package and the application to the folder specified in the COMPSs project *XML* file;
3. To simplify, copy the `yarn-conn.jar` to the cloud connector COMPSs folder, the default location is `/opt/COMPSs/Runtime/cloud-conn/`;
4. Submit with `runcompss` command:

```bash
runcompss --summary\
          --lang=python\
		  --project=./test_p_yarn.xml\
		  --resources=./test_r_yarn.xml\
	      --python_interpreter=python3\
          --pythonpath=/tmp/\
		  test.py 16 5 20
```

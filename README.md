# COMPSs connector to Yarn cluster

A COMPSs connector to submit tasks in a Yarn Cluster with Docker.

## Quickstart

This connector uses the COMPSs's official cloud connector interface. Its usage is similar to the official Mesos COMPSs connector. However, an [example](./test) is available as quickstart.

1. Build the `yarn-conn.jar`. A compiled jar is also [available](./yarn-conn/target/). It's built using Hadoop 3.3.0 and COMPSs 2.6;
2. Copy the *jar* to an HDFS folder and also to the COMPSs cloud connector folder (e.g., /opt/COMPSs/Runtime/cloud-conn/). This step is only needed in the master node;
3. Create a package with the application `tar -czvf test.tar.gz test.py`;
4. Create a *resource.xml* and a *project.xml* similar to the example;
4. Make sure to add HDFS/YARN jars in your classpath or set them at COMPSs runtime;
```bash
export CLASSPATH=$CLASSPATH:`$HADOOP_HOME/bin/hdfs classpath --glob``
```
5. Run a COMPSs application
```bash
runcompss -d --summary --lang=python\
          --project=./test_p_yarn.xml\
          --resources=./test_r_yarn.xml\
          --python_interpreter=python3\
          --pythonpath=/tmp/\
          --classpath=`$HADOOP_HOME/bin/hdfs classpath --glob`\
          test.py 16 3 10
```

### NOTE: 

 * This connector does not support Hadoop 2.X. However, it's possible that supports other Hadoop versions 3.X besides the 3.3.0;

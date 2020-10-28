
# create the package source
tar -czvf test.tar.gz test.py 
cp ./test.py /tmp/
mv ./test.tar.gz /tmp/

# copy `yarn-conn.jar` to HDFS. The absolute path must be specified at project.xml
hdfs dfs -put -f ../yarn-conn/target/yarn-conn.jar /

# copy `yarn-conn.jar` to the COMPSs cloud connector folders (only needed in master node).
cp ../yarn-conn/target/yarn-conn.jar /opt/COMPSs/Runtime/cloud-conn/
chmod 755 /opt/COMPSs/Runtime/cloud-conn/yarn-conn.jar

# add HDFS/YARN jars in your classpath
# export CLASSPATH=$CLASSPATH:`$HADOOP_HOME/bin/hdfs classpath --glob`
# or set at COMPSs runtime

runcompss -d --summary --lang=python\
		 --project=./test_p_yarn.xml\
		 --resources=./test_r_yarn.xml\
	     --python_interpreter=python3\
         --pythonpath=/tmp/\
         --classpath=`$HADOOP_HOME/bin/hdfs classpath --glob`\
		 test.py 4 3 10



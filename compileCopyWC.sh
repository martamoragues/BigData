CWD=`pwd`
javac -cp /Soft/hadoop/0.20.203.0/hadoop-ant-0.20.203.0.jar:/Soft/hadoop/0.20.203.0/hadoop-core-0.20.203.0.jar:/Soft/hadoop/0.20.203.0/hadoop-examples-0.20.203.0.jar:/Soft/hadoop/0.20.203.0/hadoop-test-0.20.203.0.jar:/Soft/hadoop/0.20.203.0/hadoop-tools-0.20.203.0.jar:/Soft/hadoop/0.20.203.0/lib/commons-logging-1.1.1.jar:/Soft/hadoop/0.20.203.0/lib/commons-logging-api-1.0.4.jar:/Soft/hadoop/0.20.203.0/lib/hsqldb-1.8.0.10.jar:/Soft/hadoop/0.20.203.0/lib/commons-cli-1.2.jar /scratch/nas/2/martam/src/examples/org/apache/hadoop/examples/*.java


cd src/examples

jar cvf $CWD/wc.jar  org/apache/hadoop/examples/*.class
rm  examples/org/apache/hadoop/examples/*.class

cd $CWD



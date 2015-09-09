#!/bin/bash 
VERSION=tez-0.8.0-SNAPSHOT
${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal /users/raajay86/code-netopt/tez/input.txt /
${HADOOP_HOME}/bin/hadoop jar /users/raajay86/code-netopt/tez/tez-dist/target/${VERSION}/tez-examples-0.8.0-SNAPSHOT.jar orderedwordcount -Dtez.runtime.io.sort.mb=220 /input.txt /output.txt


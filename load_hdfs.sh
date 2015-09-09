#!/bin/bash 
VERSION=tez-0.8.0-SNAPSHOT

${HADOOP_HOME}/bin/hdfs dfs -mkdir -p /apps/${VERSION}
${HADOOP_HOME}/bin/hdfs dfs -copyFromLocal /users/raajay86/code-netopt/tez/tez-dist/target/${VERSION}.tar.gz /apps/${VERSION}

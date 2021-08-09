#!/bin/bash
cp repFiles/dcos/build.sbt .
cp src/main/scala/tianyu/algorithm/util/hdfs.scala repFiles/local/
cp repFiles/dcos/hdfs.scala src/main/scala/tianyu/algorithm/util/
sbt clean
sbt package

# Please add proxy_scp to your path
expect proxy_scp target/scala-2.11/recommendationengine_2.11-1.0.jar 172.20.4.212 /home/dcos/artifacts
bash scripts/localtest.sh


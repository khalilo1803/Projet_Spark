#!/bin/bash

echo "deploiement cluster Hadoop Unit"

function downloadHadoopUnit() { 
 wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz -p ./
 echo "done"
tar -xzf archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
echo "desarchiver"


}

downloadHadoopUnit


dirname = $(cd $(dirname $ 0) ; pwd )
jobName = "Reading a csv file using a hadoop Unit cluster "
className = org.example.khalil.SparkApp
app_jar =  /spp/target/spp-1.0-SNAPSHOT.jar
echo "Submitting spark job on Hadoop Unit cluster" 

--files $(dir/application.conf) 

mvn install

spark-submit --master yarn 
--deploy-mode cluster 
--name $jobName
--class $className
--files $(dir/application.conf) 
$app_jar




exit

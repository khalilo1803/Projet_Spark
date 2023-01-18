#!bin/bash
basedir = $(cd $(dirname $ 0) ; pwd )
jobName = "Reading a csv file using a hadoop yarn cluster "
className = org.example.khalil.SparkApp
app_jar = /spp/target/spp-1.0-SNAPSHOT.jar
echo "Submitting spark job on Hadoop Yarn cluster" 

--files $(basedir/application.conf) 

mvn install

spark-submit --master yarn 
--deploy-mode cluster 
--name $jobName
--class $className
--files $(basedir/application.conf) 
$app_jar


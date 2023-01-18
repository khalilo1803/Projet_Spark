#!bin/bash
basedir = $(cd $(dirname $ 0) ; pwd )
jobName = "Reading a csv file "
className = org.example.khalil.SparkApp
utility_jar =  $($basedir/utility_jar)
app_jar = /spp/target/spp-1.0-SNAPSHOT.jar
echo "Submitting spark job" 
mvn install 
./spark-submit --class   $className 
--name $jobName
--conf  spar.driver.memory= 2g
--jars $utility_jar
--master local 
--files $(basedir/application.conf) 
$app_jar

docker build -f docker/Dockerfile -t spark-hadoop:3.2.0 ./docker
kubectl create -f ./kubernetes/spark-master-deployment.yaml
kubectl create -f ./kubernetes/spark-master-service.yaml
kubectl create -f ./kubernetes/spark-worker-deployment.yaml

spark-submit 
  --master k8s://https://kubernetes.docker.internal:6443 
  --deploy-mode cluster  
  --class $className
  --conf spark.executor.instances=5 
  --conf spark.kubernetes.container.image=newfrontdocker/spark:v3.0.1-j14 
$app_jar

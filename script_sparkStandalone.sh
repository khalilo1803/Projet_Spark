#!/bin/bash

echo "deploiement spark standalone"

function download() { 
 wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz -p ./
 echo "done"
tar -xzf dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3-scala2.13.tgz
echo "desarchiver"


}

function verifier_variable() {
source $HOME/.bashrc
echo $SPARK_HOME

}

function lancer() {
start-master.sh
echo"master lance"
start-worker.sh spark://ASUSPC.local:7077
echo "worker lancer"
}

function deploiement (){
spark-submit --master spark://172.16.181.40.7077 --conf spark.app.name="spark_standalone" --exexutor-memory 1G C:/Users/ASUSPC/Downloads/spp/target/spp-1.0-SNAPSHOT.jar
echo "done"
}



download
verifier_variable
lancer
deploiement
exit

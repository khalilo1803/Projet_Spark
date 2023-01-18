package org.example;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.example.Function.ActeNbre;
import org.example.Function.ActeStatFunction;
import org.example.Function.Reader.read;
import org.example.Function.Writer.write;
import org.example.naissance.beans.ActeNaissance;
import org.example.naissance.beans.Stat;
import org.example.naissance.function.MapActeDeNaissanceToStat;
import org.example.naissance.function.ReduceFunction;
import org.example.naissance.function.statgroup;
import scala.Tuple2;


@Slf4j
public class Main {
    public static void main(String[] args) throws InterruptedException {

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
        String inputPath = config.getString("app.data.input");
        String outputPath = config.getString("app.data.output");
        //Dataset<Row> ds=spark.read().option("delimiter",";").option("header","true").csv(inputPath);
        //Dataset<Row> statds=ds.groupBy("name").agg(count("product").as("nb"),sum("price").as("expense"));
        read r =new read(inputPath,spark);
        write w=new write(outputPath);
        ActeStatFunction Actfunc=new ActeStatFunction();
        Dataset<Row> ds=r.get();
        ds.printSchema();
        ds.show(5, false);
        ds.createOrReplaceTempView("ma_view");
        Dataset<Row> newdata = spark.sql("select * from ma_view where COD_MOD=2019 ");
        newdata.show(2, false);
        w.accept(Actfunc.apply(r.get()));
        Dataset<ActeNaissance> cleanDS = new ActeDeNaissanceMapper().apply(ds);
        cleanDS.printSchema();
        cleanDS.show(5, false);
        ActeNbre cc=new ActeNbre();
        statgroup p=new statgroup();
        KeyValueGroupedDataset<String, ActeNaissance> bucket = cleanDS.groupByKey(p, Encoders.STRING());
        MapActeDeNaissanceToStat mp=new MapActeDeNaissanceToStat();
        ReduceFunction reduceFunction = new ReduceFunction() ;
       // Dataset<Tuple2<String, Stat>> tuple2Dataset =   bucket.mapValues(mp, Encoders.bean(Stat.class)).reduceGroups(reduceFunction);
       // tuple2Dataset.show(5,true);
        log.info("nb = {}",cc.apply(cleanDS));
        //Dataset<ActeNaissance> ss=cleanDS.groupBy()
       // cleanDS.coalesce(1).write().mode(SaveMode.Overwrite).json(outputPath);
        //Thread.sleep(1000*60*2);
        //statds.write().partitionBy("name").parquet(outputPath);
    }
}
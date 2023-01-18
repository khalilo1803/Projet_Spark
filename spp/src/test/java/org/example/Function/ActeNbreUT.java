package org.example.Function;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.naissance.beans.ActeNaissance;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ActeNbreUT {

    @Test
    public void testApply2() {
       // ActeNaissance ac=new ActeNaissance("a","b","a","b","a","b");
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        List<ActeNaissance> lista = Arrays.asList(ActeNaissance.builder()
                .LONG_VAR("a")
                .TYPE_VAR("b")
                .LIB_MOD("c")
                .LIB_VAR("d")
                .COD_MOD("e")
                .COD_VAR("f")
                .build());
        Dataset<ActeNaissance> dataset = spark.createDataset(lista, Encoders.bean(ActeNaissance.class));
        ActeNbre c=new ActeNbre();
        Long actual=c.apply(dataset);
        assertThat(actual).isEqualTo(1l);

    }
}

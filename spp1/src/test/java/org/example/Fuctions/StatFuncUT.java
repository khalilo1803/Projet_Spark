package org.example.Fuctions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Naissance.beans.ActeDeNaissance;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatFuncUT {

    @Test
    public void teststat() {
        // ActeNaissance ac=new ActeNaissance("a","b","a","b","a","b");
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        List<ActeDeNaissance> lista = Arrays.asList(ActeDeNaissance.builder()
                .LONG_VAR("a")
                .TYPE_VAR("b")
                .LIB_MOD("c")
                .LIB_VAR("d")
                .COD_MOD("e")
                .COD_VAR("f")
                .build());
        Dataset<ActeDeNaissance> dataset = spark.createDataset(lista, Encoders.bean(ActeDeNaissance.class));
        StatFunc c=new StatFunc();
        Long actual=c.apply(dataset);
        assertThat(actual).isEqualTo(1l);

    }

}

package org.example.Function.Reader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.naissance.beans.ActeNaissance;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReadUT {

    String input = ConfigFactory.load().getString("app.data.input");
    @Test
    public void testRead() throws IOException {

        StructType shema =new StructType(
                new StructField[]{
                        new StructField(
                                "LONG_VAR",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "TYPE_VAR",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "LIB_MOD",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "LIB_VAR",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "COD_MOD",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                        new StructField(
                                "COD_VAR",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()
                        ),
                }
        );
        String[] value=new String[]{"a", "b","c", "d","e", "f" };
        Row s = new GenericRowWithSchema(value, shema);
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("master");
        String appName = config.getString("appname");
        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
        read r=new read(input,spark);
        List<String> ss=List.of("kk");
       // List<ActeNaissance> rows =
        //final Dataset<Row> actual = spark().createDataFrame(rows, Apple.class);
      //  List<ActeNaissance> rows = Arrays.asList(s);
       // Dataset<Row> dataset = spark.createDataset(rows,Encoders.bean(ActeNaissance.class));
        Dataset<Row> axtual=r.get();
        assertThat(axtual)
                .isNotNull();

       // assertNotNull(axtual);
       // assertEquals(1, axtual.count());
    }
}

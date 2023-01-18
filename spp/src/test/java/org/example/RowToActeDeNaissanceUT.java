package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.naissance.beans.ActeNaissance;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToActeDeNaissanceUT {


    @Test
    public void testApply() {
        RowToActeDeNaissance f = new RowToActeDeNaissance();
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
        Row r1 = RowFactory.create("a", "b","c", "d","e", "f" );
        ActeNaissance expected= ActeNaissance.builder()
                .LONG_VAR("a")
                .TYPE_VAR("b")
                .LIB_MOD("c")
                .LIB_VAR("d")
                .COD_MOD("e")
                .COD_VAR("f")
                .build();
        ActeNaissance actual = f.apply(s);

        assertThat(actual).isEqualTo(expected);
    }
}

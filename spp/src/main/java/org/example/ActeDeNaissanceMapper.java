package org.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.ActeNaissance;

import java.util.function.Function;

public class ActeDeNaissanceMapper implements Function<Dataset<Row>, Dataset<ActeNaissance>> {
    //private final RowToActeDeNaissance parser = new RowToActeDeNaissance();
    //private final MapFunction<Row, ActeNaissance> task = parser::apply;
    private final MapFunction<Row, ActeNaissance> task = new RowToActeDeNaissanceSpark();

    @Override
    public Dataset<ActeNaissance> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(ActeNaissance.class));
    }
}

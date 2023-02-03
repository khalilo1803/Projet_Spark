package org.example.Naissance.Function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.Naissance.beans.ActeDeNaissance;

import java.util.function.Function;

public class ActeDeNaissanceMapper implements Function<Dataset<Row>, Dataset<ActeDeNaissance>> {
    private final RowToActeDeNaissance parser = new RowToActeDeNaissance();
    private final MapFunction<Row, ActeDeNaissance> task = parser::apply;
    //private final MapFunction<Row, ActeDeNaissance> task = new RowToActeDeNaissanceSpark();

    @Override
    public Dataset<ActeDeNaissance> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(ActeDeNaissance.class));
    }
}

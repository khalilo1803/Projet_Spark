package org.example.Fuctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.Naissance.Function.ActeDeNaissanceMapper;
import org.example.Naissance.beans.ActeDeNaissance;

import java.util.function.Function;

public class Mapper implements Function<Dataset<Row>, Dataset<ActeDeNaissance> > {
    @Override
    public Dataset<ActeDeNaissance> apply(Dataset<Row> rowDataset) {
        return new ActeDeNaissanceMapper().apply(rowDataset);
    }
}

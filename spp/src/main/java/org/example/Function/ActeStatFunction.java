package org.example.Function;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.ActeNaissance;
import org.example.ActeDeNaissanceMapper;

import java.util.function.Function;

@RequiredArgsConstructor
public class ActeStatFunction implements Function<Dataset<Row>, Dataset<ActeNaissance> > {



    @Override
    public Dataset<ActeNaissance> apply(Dataset<Row> rowDataset) {

        return  new ActeDeNaissanceMapper().apply(rowDataset);
    }
}

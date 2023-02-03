package org.example.Fuctions;

import org.apache.spark.sql.Dataset;
import org.example.Naissance.beans.ActeDeNaissance;

import java.util.function.Function;

public class StatFunc implements Function<Dataset<ActeDeNaissance>, Long > {
    @Override
    public Long apply(Dataset<ActeDeNaissance> acteNaissanceDataset) {
        return acteNaissanceDataset.count();
    }
}

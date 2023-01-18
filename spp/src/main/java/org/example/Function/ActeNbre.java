package org.example.Function;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.naissance.beans.ActeNaissance;

import java.util.function.Function;
@RequiredArgsConstructor
public class ActeNbre  implements Function<Dataset<ActeNaissance>, Long > {
    @Override
    public Long apply(Dataset<ActeNaissance> acteNaissanceDataset) {

        return acteNaissanceDataset.count();
    }
}

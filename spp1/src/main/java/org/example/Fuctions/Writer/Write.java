package org.example.Fuctions.Writer;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.example.Naissance.beans.ActeDeNaissance;

import java.util.function.Consumer;
@RequiredArgsConstructor
public class Write implements Consumer<Dataset<ActeDeNaissance>> {
    private final String outputPath;
    @Override
    public void accept(Dataset<ActeDeNaissance> Dataset) {
        Dataset.write().mode(SaveMode.Overwrite).csv(outputPath);
    }
}

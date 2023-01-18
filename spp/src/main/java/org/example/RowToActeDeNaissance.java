package org.example;

import org.apache.spark.sql.Row;
import org.example.naissance.beans.ActeNaissance;

import java.io.Serializable;
import java.util.function.Function;

public class RowToActeDeNaissance implements Function<Row, ActeNaissance> , Serializable {
    @Override
    public ActeNaissance apply(Row row) {
        String COD_VAR = row.getAs("COD_VAR") ;
        String LIB_VAR = row.getAs("LIB_VAR") ;
        String COD_MOD = row.getAs("COD_MOD") ;
        String LIB_MOD = row.getAs("LIB_MOD") ;
        String TYPE_VAR = row.getAs("TYPE_VAR") ;
        String LONG_VAR = row.getAs("LONG_VAR") ;





        return ActeNaissance.builder()
                .LIB_MOD(LIB_MOD)
                .TYPE_VAR(TYPE_VAR)
                .LONG_VAR(LONG_VAR)
                .LIB_VAR(LIB_VAR)
                .COD_VAR(COD_VAR)
                .COD_MOD(COD_MOD).build();   }
}

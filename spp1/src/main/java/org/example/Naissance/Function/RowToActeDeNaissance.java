package org.example.Naissance.Function;

import org.apache.spark.sql.Row;
import org.example.Naissance.beans.ActeDeNaissance;

import java.io.Serializable;
import java.util.function.Function;

public class RowToActeDeNaissance implements Function<Row, ActeDeNaissance>, Serializable {
    @Override
    public ActeDeNaissance apply(Row row) {
        String COD_VAR = row.getAs("COD_VAR") ;
        String LIB_VAR = row.getAs("LIB_VAR") ;
        String COD_MOD = row.getAs("COD_MOD") ;
        String LIB_MOD = row.getAs("LIB_MOD") ;
        String TYPE_VAR = row.getAs("TYPE_VAR") ;
        String LONG_VAR = row.getAs("LONG_VAR") ;





        return ActeDeNaissance.builder()
                .LIB_MOD(LIB_MOD)
                .TYPE_VAR(TYPE_VAR)
                .LONG_VAR(LONG_VAR)
                .LIB_VAR(LIB_VAR)
                .COD_VAR(COD_VAR)
                .COD_MOD(COD_MOD).build();   }

}

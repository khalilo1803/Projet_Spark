package org.example.Fuctions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.Naissance.Function.ActeDeNaissanceMapper;
import org.example.Naissance.beans.ActeDeNaissance;

import java.io.Serializable;


public class StringToActeDeNaissance implements Function<String, ActeDeNaissance> {


    @Override
    public ActeDeNaissance call(String s) throws Exception {
        String[] split = s.split(";");


        return ActeDeNaissance.builder()
                .LIB_MOD(split[0])
                .TYPE_VAR(split[1])
                .LONG_VAR(split[2])
                .LIB_VAR(split[3])
                .COD_VAR(split[4])
                .COD_MOD(split[5]).build();   }
}

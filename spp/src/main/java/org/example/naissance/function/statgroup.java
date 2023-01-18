package org.example.naissance.function;

import org.apache.spark.api.java.function.MapFunction;
import org.example.naissance.beans.ActeNaissance;

public class statgroup implements MapFunction<ActeNaissance,String> {
    @Override
    public String call(ActeNaissance acteNaissance) throws Exception {
        return acteNaissance.getCOD_MOD();
    }
}

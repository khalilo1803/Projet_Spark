package org.example.naissance.function;

import org.apache.spark.api.java.function.MapFunction;
import org.example.naissance.beans.ActeNaissance;
import org.example.naissance.beans.Stat;

public class MapActeDeNaissanceToStat implements MapFunction<ActeNaissance, Stat> {
    @Override
    public Stat call(ActeNaissance acteNaissance) throws Exception {
        return Stat.builder()
                .MOD(acteNaissance.getCOD_MOD())
                .COUNT(1)
                .build();
    }
}

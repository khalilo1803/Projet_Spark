package org.example.naissance.function;

import org.example.naissance.beans.Stat;

public class ReduceFunction  implements org.apache.spark.api.java.function.ReduceFunction<Stat> {
    @Override
    public Stat call(Stat stats, Stat t1) throws Exception {

        return Stat.builder()
                .COUNT(stats.getCOUNT() + stats.getCOUNT())
                .build();
    }
}

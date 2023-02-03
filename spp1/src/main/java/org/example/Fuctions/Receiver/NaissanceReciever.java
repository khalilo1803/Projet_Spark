package org.example.Fuctions.Receiver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Fuctions.StringToActeDeNaissance;
import org.example.Fuctions.Type.ActeDeNaissanceLongWritable;
import org.example.Fuctions.Type.ActeDeNaissanceText;
import org.example.Fuctions.Type.ActeNaissanceFileInputFormat;
import org.example.Naissance.beans.ActeDeNaissance;


import java.io.Serializable;
import java.util.function.Supplier;
@RequiredArgsConstructor
@Builder
public class NaissanceReciever implements Supplier<JavaDStream<ActeDeNaissance>>, Serializable {
    private final String hdfsInputPathStr;
    private final JavaStreamingContext jsc;


    private final Function<Path,Boolean> filter = hdfsPath->{
        return hdfsPath.getName().endsWith(".csv") &&
                !hdfsPath.getName().startsWith("_") &&
                !hdfsPath.getName().startsWith(".tmp") ;
    };

    @Override
    public JavaDStream<ActeDeNaissance> get() {
        JavaPairInputDStream<ActeDeNaissanceLongWritable, ActeDeNaissanceText> inputDStream =jsc
                .fileStream(
                        hdfsInputPathStr,
                        ActeDeNaissanceLongWritable.class,
                        ActeDeNaissanceText.class,
                        ActeNaissanceFileInputFormat.class,
                        filter,
                        true
                );
        StringToActeDeNaissance dr=new StringToActeDeNaissance();
        JavaDStream<ActeDeNaissance> javaDStream=inputDStream.map(tuple -> dr.call(tuple._2().toString()));
        return javaDStream;
    }
}

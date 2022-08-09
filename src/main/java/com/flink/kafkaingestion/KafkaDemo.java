package main.java.com.flink.kafkaingestion;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "127.0.0.1:9092");

        DataStream < String > kafkaData = env.addSource(new FlinkKafkaConsumer < String > ("test",
                new SimpleStringSchema(),
                p));

        kafkaData.flatMap(new FlatMapFunction < String, Tuple2 < String, Integer >> () {
            public void flatMap(String value, Collector < Tuple2 < String, Integer >> out) {
                String[] words = value.split(" ");
                for (String word: words)
                    out.collect(new Tuple2 < String, Integer > (word, 1));
            }
        })

                .keyBy(t -> t.f0)
                .sum(1)
                .addSink(StreamingFileSink
                        .forRowFormat(new Path("/home/jivesh/kafka.txt"),
                                new SimpleStringEncoder < Tuple2 < String, Integer >> ("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build());

        env.execute("Kafka Example");
    }

}
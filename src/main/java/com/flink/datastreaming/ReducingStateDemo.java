package main.java.com.flink.datastreaming;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream < String > data = env.socketTextStream("localhost", 9090);

        DataStream < Long > sum = data.map(new MapFunction < String, Tuple2 < Long, String >> () {
            public Tuple2 < Long, String > map(String s) {
                String[] words = s.split(",");
                return new Tuple2 < Long, String > (Long.parseLong(words[0]), words[1]);
            }
        })
                .keyBy(t -> t.f0)
                .flatMap(new StatefulMap());
        //sum.writeAsText("/home/jivesh/state2");
        sum.addSink(StreamingFileSink
                .forRowFormat(new Path("/home/jivesh/state2"), new SimpleStringEncoder < Long > ("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                .build());

        // execute program
        env.execute("State");
    }

    public static class StatefulMap extends RichFlatMapFunction < Tuple2 < Long, String > , Long > {
        private transient ValueState < Long > count;
        private transient ReducingState < Long > sum;

        public void flatMap(Tuple2 < Long, String > input, Collector < Long > out) throws Exception {
            Long currValue = Long.parseLong(input.f1);
            Long currCount = 0L;

            if (count.value() != null) {
                currCount = count.value();
            }

            currCount += 1;

            count.update(currCount);
            sum.add(currValue);

            if (currCount >= 10) {
                /* emit sum of last 10 elements */
                out.collect(sum.get());
                /* clear value */
                count.clear();
                sum.clear();
            }
        }

        public void open(Configuration conf) {

            ValueStateDescriptor < Long > descriptor2 = new ValueStateDescriptor < Long > ("count", Long.class);
            count = getRuntimeContext().getState(descriptor2);

            ReducingStateDescriptor < Long > sumDesc = new ReducingStateDescriptor < Long > ("reducing sum", new SumReduce(), Long.class);
            sum = getRuntimeContext().getReducingState(sumDesc);
        }

        public class SumReduce implements ReduceFunction < Long > {
            public Long reduce(Long commlativesum, Long currentvalue) {
                return commlativesum + currentvalue;
            }
        }
    }
}


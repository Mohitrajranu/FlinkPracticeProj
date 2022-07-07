package main.java.com.flink.datastreaming;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//TumblingEvent

public class WindowType
{
    public static void main(String[] args) throws Exception
    {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple2<Long, String>> sum = data.map(new MapFunction<String, Tuple2<Long, String>>()
        {
            public Tuple2<Long, String> map(String s)
            {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        })

                .assignTimestampsAndWatermarks(new DemoWatermark())

                .windowAll(EventTimeSessionWindows.withGap(Time.seconds(10)))

                .reduce(new ReduceFunction<Tuple2<Long, String>>()
                {
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> t1, Tuple2<Long, String> t2)
                    {
                        int num1 = Integer.parseInt(t1.f1);
                        int num2 = Integer.parseInt(t2.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<Long, String>(t.getTime(), "" + sum);
                    }
                });
        sum.writeAsText("/home/jivesh/window");

        // execute program
        env.execute("Window");
    }

    public static class DemoWatermark implements AssignerWithPeriodicWatermarks<Tuple2<Long, String>>
    {
        private final long allowedlatetime = 3500; // 3.5 seconds

        private long currentMaxTimestamp=0;

        public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp)
        {
            long timestamp = element.f0;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        public Watermark getCurrentWatermark()
        {
            // return the watermark as current highest timestamp minus the out-of-orderness bound
            return new Watermark(currentMaxTimestamp - allowedlatetime);
        }
    }
}



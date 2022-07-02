package com.flink.dataset;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception
        {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            ParameterTool params = ParameterTool.fromArgs(args);

            env.getConfig().setGlobalJobParameters(params);

            DataSet<String> text = env.readTextFile(params.get("input"));

            DataSet<String> filtered = text.filter(new FilterFunction<String>()

            {
                public boolean filter(String value)
                {
                    return value.startsWith("N");
                }
            });
            DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

            DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);
            if (params.has("output"))
            {
                counts.writeAsCsv(params.get("output"), "\n", " ");

                env.execute("WordCount Example");
            }
        }

        public static final class Tokenizer
                implements MapFunction<String, Tuple2<String, Integer>>
        {
            public Tuple2<String, Integer> map(String value)
            {
                return new Tuple2(value, Integer.valueOf(1));
            }
        }

    public static final class Tokenizer1
            implements FlatMapFunction<String, Tuple2<String, Integer>>
    {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens=s.split(" ");
            for (String token:tokens){
                if (token.length()>0){
                    collector.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }

    }

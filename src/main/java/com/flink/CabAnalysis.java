package com.flink;

import java.lang.Iterable;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;

public class CabAnalysis {

    /*
     * popular_destination
     * avg. passengers per trip source
     * avg. passengers per driver
     *
     * Input Format: id,plate,type,driver_name,trip_status,src,dest,passengers_count
     */
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet < Tuple8 < String, String, String, String, Boolean, String, String, Integer >> data =
                env.readTextFile("/shared/myuser/codes/cab analysis/cab flink.txt")
                        .map((MapFunction<String, Tuple8<String, String, String, String, Boolean, String, String, Integer>>) value -> {
                            String[] words = value.split(",");
                            Boolean status = false;
                            if (words[4].equalsIgnoreCase("yes"))
                                status = true;
                            if (status)
                                return new Tuple8<>(words[0], words[1], words[2], words[3], status, words[5], words[6], Integer.parseInt(words[7]));
                            else
                                return new Tuple8<>(words[0], words[1], words[2], words[3], status, words[5], words[6], 0);
                        })
                        .filter((FilterFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>>) value -> value.f4);

        // most popular destination
        DataSet < Tuple8 < String, String, String, String, Boolean, String, String, Integer >> popularDest = data.groupBy(6).sum(7).maxBy(7);
        popularDest.writeAsText("/home/myuser/popular_dest_batch.txt");

        // avg. passengers per trip source: place to pickup most passengers
        DataSet < Tuple2 < String, Double >> avgPassPerTrip = data
                .map((MapFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>, Tuple3<String, Integer, Integer>>) value -> {
                    // driver,trip_passengers,trip_count
                    return new Tuple3 < String, Integer, Integer > (value.f5, value.f7, 1);
                })
                .groupBy(0)
                .reduce((ReduceFunction<Tuple3<String, Integer, Integer>>) (v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2))
                .map((MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>) value -> new Tuple2 < String, Double > (value.f0, ((value.f1 * 1.0) / value.f2)));
        //.reduceGroup(new AvgPassengersPerTrip(5));
        avgPassPerTrip.writeAsText("/home/myuser/avg_passengers_per_trip.txt");

        // avg. passengers per driver: popular/efficient driver
        DataSet < Tuple2 < String, Double >> avgPassPerDriver = data
                .map((MapFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>, Tuple3<String, Integer, Integer>>) value -> {
                    // driver,trip_passengers,trip_count
                    return new Tuple3 < String, Integer, Integer > (value.f3, value.f7, 1);
                })
                .groupBy(0)
                .reduce((ReduceFunction<Tuple3<String, Integer, Integer>>) (v1, v2) -> new Tuple3<>(v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2))
                .map((MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>) value -> new Tuple2<>(value.f0, ((value.f1 * 1.0) / value.f2)));
        //.reduceGroup(new AvgPassengersPerTrip(3));
        avgPassPerDriver.writeAsText("/shared/avg_passengers_per_driver.txt");

        env.execute("Cab Analysis");

    }

}
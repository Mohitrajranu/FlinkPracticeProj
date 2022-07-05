package com.flink.datastreaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class GlobalWindowsAvgProfit
{
	public static void main(String[] args) throws Exception 
	{
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		DataStream<String> data = env.socketTextStream("localhost", 9090);
		
		                // month, product, category, profit, count
		DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12,1]
		                                                                                                     //        [June,Category4,Perfume,10,1]
						// groupBy 'month'                                                                                           
		DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
				.keyBy(0)
				.window(GlobalWindows.create())
				.trigger(CountTrigger.of(5))
			//	.evictor(CountEvictor.of(4))
				.reduce(new Reduce1());
             																// June { [Category5,Bat,12,1] Category4,Perfume,10,1}	//rolling reduce			
		                                                                   // reduced = { [Category4,Perfume,22,2] ..... }
		reduced.writeAsText("/home/jivesh/www");
		// execute program
		env.execute("Avg Profit Per Month");
	}                                                                           
	
	public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>>
	{
		public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
																	   		Tuple5<String, String, String, Integer, Integer> pre_result)
		{
	return new Tuple5<String, String, String, Integer, Integer>(current.f0,
			current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4); 
		}
	}
	public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> 
	{
		public Tuple5<String, String, String, Integer, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
		{
			String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
			// ignore timestamp, we don't need it for any calculations
			//Long timestamp = Long.parseLong(words[5]);
			return new Tuple5<String, String, String, Integer, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]), 1); 
		}                                                            //    June    Category5      Bat                      12 
	}
}

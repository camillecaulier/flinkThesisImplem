package org.example;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.HashMap;

public class MaxPartialWindowProcessFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {

    // Define a ReducingState to store the maximum value seen so far in the window

    private HashMap<String, Integer> map ;

    @Override
    public void open(Configuration parameters) throws Exception {
        map = new HashMap<>();
    }

//    @Override
//    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//        for(Tuple2<String, Integer> value : input){
//            if(!map.containsKey(key)){
//                map.put(key, value.f1);
//            }
//            else if(value.f1 > map.get(key)){
//                map.put(key, value.f1);
//            }
//        }
//        for (String k : map.keySet()) {
//            out.collect(Tuple2.of(k, map.get(k)));
//        }
//
//    }

    @Override
    public void process(Context context, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> collector) throws Exception {
        for(Tuple2<String, Integer> value : input){
            String key = value.f0;
            if(!map.containsKey(key)){
                map.put(key, value.f1);
            }
            else if(value.f1 > map.get(key)){
                map.put(key, value.f1);
            }
        }

        for (String k : map.keySet()) {
            collector.collect(Tuple2.of(k, map.get(k)));
        }
//        printMapState();
    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : map.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + map.get(entry));
        }
    }
}

package metrics;//package org.example;//package org.example;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class KeyGroupMetricProcessFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private transient MapState<String, Long> keyGroupCount;

    @Override
    public void open(Configuration parameters) throws Exception {


        MapStateDescriptor<String, Long> keyGroupCountersStateDescriptor = new MapStateDescriptor<String, Long>("keyGroupCounters", String.class, Long.class);
        keyGroupCount = getRuntimeContext().getMapState(keyGroupCountersStateDescriptor);
    }


//    private void logMapStateContents() throws Exception {
//        StringBuilder sb = new StringBuilder("MapState contents:\n");
//        Iterable<Map.Entry<String, Long>> entries = keyGroupCount.entries();
//        for (Map.Entry<String, Long> entry : entries) {
//            sb.append("Key: ").append(entry.getKey()).append(", Count: ").append(entry.getValue()).append("\n");
//        }
//        System.out.println(sb.toString());
//
//        System.out.println(getSize(keyGroupCount.keys()));
//    }
    private void logMapStateContents() throws Exception {
        StringBuilder sb = new StringBuilder("Global MapState contents:\n");
        Map<String, Long> globalMap = new HashMap<>();
        for (Map.Entry<String, Long> entry : keyGroupCount.entries()) {
            globalMap.put(entry.getKey(), entry.getValue());
        }
        sb.append(globalMap);
        System.out.println(sb.toString());
    }

    @Override
    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String keyGroup = stringIntegerTuple2.f0;
        Long count = keyGroupCount.get(keyGroup);
        if (count == null) {
            count = 0L;
        }else{
            count++;
        }
        keyGroupCount.put(keyGroup, count);
        collector.collect(stringIntegerTuple2);

        // Log the contents of the MapState
//        logMapStateContents();
    }


    public static <T> int getSize(Iterable<T> iterable) {
        int size = 0;
        for (T element : iterable) {
            size++;
        }
        return size;
    }

}




//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.state.AggregatingState;
//import org.apache.flink.api.common.state.AggregatingStateDescriptor;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class KeyGroupMetricProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>> {
//
//    private transient AggregatingState<Long, Long> keyGroupCount;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        AggregatingStateDescriptor<Long, Long, Long> descriptor =
//                new AggregatingStateDescriptor<>("keyGroupCount", new CountAggregator(), Long.class);
//        keyGroupCount = getRuntimeContext().getAggregatingState(descriptor);
//    }
//
//    @Override
//    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//        keyGroupCount.add(value.f1);
//    }
//
//    private void logMapStateContents() throws Exception {
//        StringBuilder sb = new StringBuilder("Global MapState contents:\n");
//        Map<String, Long> globalMap = new HashMap<>();
//        for (Map.Entry<String, Long> entry : keyGroupCount.entries()) {
//            globalMap.put(entry.getKey(), entry.getValue());
//        }
//        sb.append(globalMap);
//        System.out.println(sb.toString());
//    }
//
//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//        // Emit the aggregated count for the current key group
//        out.collect(new Tuple2<>(ctx.getCurrentKey(), keyGroupCount.get()));
//        // Clear the state for the current key group
//        keyGroupCount.clear();
//    }
//
//    private static class CountAggregator implements AggregateFunction<Long, Long, Long> {
//        @Override
//        public Long createAccumulator() {
//            return 0L;
//        }
//
//        @Override
//        public Long add(Long value, Long accumulator) {
//            return accumulator + 1;
//        }
//
//        @Override
//        public Long getResult(Long accumulator) {
//            return accumulator;
//        }
//
//        @Override
//        public Long merge(Long a, Long b) {
//            return a + b;
//        }
//    }
//}

//

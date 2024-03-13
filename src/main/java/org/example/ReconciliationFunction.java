package org.example;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.configuration.Configuration;
public class ReconciliationFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private int max = Integer.MIN_VALUE;
    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        max = Math.max(max, value.f1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        out.collect(new Tuple2<>(ctx.getCurrentKey(), max));
        max = Integer.MIN_VALUE;
    }


//    public void open(Configuration parameters) throws Exception {
        // Register timer to emit reconciled result when watermark arrives
//        getRuntimeContext().getState(new ValueStateDescriptor<>("maxValue", Integer.class));
//        getTimerService().registerEventTimeTimer(Long.MAX_VALUE);

//    }

//    @Override
//    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
//        out.collect(value);
//    }


}
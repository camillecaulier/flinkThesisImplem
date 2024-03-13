package org.example;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
public class KeyGroupMetricBroadcastProcessFunction extends BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
    private transient MapState<String, Long> keyGroupCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        MapStateDescriptor<String, Long> keyGroupCountersStateDescriptor = new MapStateDescriptor<String, Long>("keyGroupCounters", String.class, Long.class);
        keyGroupCount = getRuntimeContext().getMapState(keyGroupCountersStateDescriptor);

    }

    @Override
    public void processElement(Tuple2<String, Integer> stringIntegerTuple2, BroadcastProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Integer>> collector) throws Exception {
        collector.collect(stringIntegerTuple2);
    }

    @Override
    public void processBroadcastElement(Tuple2<String, Integer> stringIntegerTuple2, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String keyGroup = stringIntegerTuple2.f0;
        Long count = keyGroupCount.get(keyGroup);
        if (count == null) {
            count = 0L;
        }else{
            count++;
        }
        keyGroupCount.put(keyGroup, count);
    }

}

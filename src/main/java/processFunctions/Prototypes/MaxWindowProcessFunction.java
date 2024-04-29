package processFunctions.Prototypes;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MaxWindowProcessFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String , TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
        int max = Integer.MIN_VALUE;
        for (Tuple2<String, Integer> value : iterable) {
            max = Math.max(max, value.f1);
        }
        collector.collect(new Tuple2<>(key, max));
    }

}

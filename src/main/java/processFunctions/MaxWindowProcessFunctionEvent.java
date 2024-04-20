package processFunctions;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MaxWindowProcessFunctionEvent extends ProcessWindowFunction<EventBasic, EventBasic, String , TimeWindow> {
    @Override
    public void process(String key, Context context, Iterable<EventBasic> iterable, Collector<EventBasic> collector) throws Exception {
        int max = Integer.MIN_VALUE;
        long time = 0;
        for (EventBasic value : iterable) {
            max = Math.max(max, value.value.valueInt);
            time = value.value.timeStamp;
        }
        collector.collect(new EventBasic(key, new Value(max,time )));
    }

}

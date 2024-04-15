package windowedProcessFunctions;

import eventTypes.EventBasic;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class fakeWindowedProcessFunctionMax extends ProcessFunction<EventBasic, EventBasic> {
    @Override
    public void processElement(EventBasic value, ProcessFunction<EventBasic, EventBasic>.Context ctx, Collector<EventBasic> out) throws Exception {

    }
}

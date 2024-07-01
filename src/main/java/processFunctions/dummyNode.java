package processFunctions;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class dummyNode extends ProcessFunction<EventBasic, EventBasic> {
    @Override
    public void processElement(EventBasic value, ProcessFunction<EventBasic, EventBasic>.Context ctx, Collector<EventBasic> out) throws Exception {
        out.collect(value);
    }
}

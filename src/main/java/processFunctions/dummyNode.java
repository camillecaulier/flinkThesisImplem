package processFunctions;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class dummyNode extends ProcessFunction<EventBasic, EventBasic> {
    /*
    * Dummy node made to reconcile all the different sources into one partition
    * not recommended since this creates a bottleneck even if it does nothing

     */
    @Override
    public void processElement(EventBasic value, ProcessFunction<EventBasic, EventBasic>.Context ctx, Collector<EventBasic> out) throws Exception {
        out.collect(value);
    }
}

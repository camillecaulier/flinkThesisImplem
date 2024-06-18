package sink;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RuntimeContext;

public class basicSinkFunction implements SinkFunction<EventBasic> {

    @Override
    public void invoke(EventBasic value, Context context) {
    }
}

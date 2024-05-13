package sink;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class sinkCollect implements SinkFunction<EventBasic> {

    // must be static
    public static final List<EventBasic> values = new ArrayList<>();

    @Override
    public synchronized void invoke(EventBasic value, Context context) {
        values.add(value);
    }
}

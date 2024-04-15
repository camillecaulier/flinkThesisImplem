package processOperators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.util.Collector;

@Internal
public class CustomProcessOperator<IN, KEY, OUT, W extends Window>
        extends ProcessOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private final MapStateDescriptor<KEY, W> windowStateDescriptor;
    private final MapState<KEY, W> windowState;

    public CustomProcessOperator(
            ProcessFunction<IN, OUT> function,
            TypeSerializer<KEY> keySerializer,
            TypeSerializer<W> windowSerializer) {
        super(function);
        windowStateDescriptor = new MapStateDescriptor<>("windowState", keySerializer, windowSerializer);
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final IN value = element.getValue();
        final KEY key = getKey(value);

        // Retrieve or create window
        W window = windowState.get(key);
        if (window == null) {
            window = createWindow(value);
            windowState.put(key, window);
        }

        // Process the element within the window
        processElementInWindow(value, window);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        System.out.println("Watermark: " + mark.getTimestamp());

    }

    protected KEY getKey(IN value) {
        // Implement logic to extract key from input value
        return null;
    }

    protected W createWindow(IN value) {
        // Implement logic to create window based on input value
        return null;
    }

    protected void processElementInWindow(IN value, W window) throws Exception {
        // Retrieve or initialize the maximum value for this window
        ValueWithTimestamp<OUT> maxValue = maxValues.get(window);
        if (maxValue == null) {
            maxValue = new ValueWithTimestamp<>(null, Long.MIN_VALUE);
            maxValues.put(window, maxValue);
        }

        // Update the maximum value if needed
        if (/* logic to determine if value is greater than current maximum */) {
            maxValue.setValue(/* value */);
            maxValue.setTimestamp(/* timestamp */);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception {
        // Emit the maximum value for each window
        for (Map.Entry<W, ValueWithTimestamp<OUT>> entry : maxValues.entrySet()) {
            W window = entry.getKey();
            ValueWithTimestamp<OUT> maxValue = entry.getValue();
            out.collect(maxValue.getValue());
        }
        maxValues.clear(); // Clear the maximum values for the next window
    }

    @Override
    public void open() throws Exception {
        super.open();

        // Initialize any necessary state or resources
    }

}

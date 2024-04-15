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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.util.Collector;

@Internal
public class CustomProcessOperator2<IN, KEY, OUT>
        extends ProcessOperator<IN, OUT> {


    private static final long serialVersionUID = 1L;



    public CustomProcessOperator2(
            ProcessFunction<IN, OUT> function, Time windowSize) {
        super(function);
//        windowStateDescriptor = new MapStateDescriptor<>("windowState",);
//        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        final IN value = element.getValue();
        final KEY key = getKey(value);

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


    @Override
    public void open() throws Exception {
        super.open();

        // Initialize any necessary state or resources
    }

}

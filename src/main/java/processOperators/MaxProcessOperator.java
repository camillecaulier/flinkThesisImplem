package processOperators;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import static org.apache.flink.util.Preconditions.checkState;

public class MaxProcessOperator<IN extends Comparable<IN>, OUT>
        extends ProcessOperator<IN, OUT> {

    private IN maxElement;
    private transient TimestampedCollector<OUT> collector;

//    private transient ContextImpl context;

    public MaxProcessOperator(ProcessFunction<IN, OUT> function) {
        super(function);
//        super(collector);
//        super.getRuntimeContext().

//        StreamConfig config1 = super.config;

//        collector = new TimestampedCollector<>(output);
//
//        context = new ContextImpl(userFunction, getProcessingTimeService());
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        IN value = element.getValue();
        if (maxElement == null || value.compareTo(maxElement) > 0) {
            maxElement = value;
            // Output the new max element
//            this.getRuntimeContext().
//            .collect(new StreamRecord<>(maxElement, element.getTimestamp()));
//            collector.collect(new StreamRecord<>(maxElement, element.getTimestamp()));
        }
        super.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        // Output the current max element when a watermark is received
        if (maxElement != null) {

        }
    }

    @Override
    public void open() throws Exception {
        super.open();

    }
//    private class ContextImpl extends ContextImpl
}
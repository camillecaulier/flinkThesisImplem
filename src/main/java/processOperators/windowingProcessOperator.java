package processOperators;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.ProcessOperator;

public class windowingProcessOperator extends ProcessOperator<EventBasic, EventBasic> {
    public windowingProcessOperator(ProcessFunction<EventBasic, EventBasic> function) {
        super(function);
    }

}

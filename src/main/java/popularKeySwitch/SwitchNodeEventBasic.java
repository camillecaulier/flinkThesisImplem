package popularKeySwitch;

import eventTypes.EventBasic;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SwitchNodeEventBasic extends ProcessFunction<EventBasic, EventBasic> {

    private final OutputTag<EventBasic> hotKeyOperatorTag;
    private final OutputTag<EventBasic> operator2OutputTag;

    public SwitchNodeEventBasic(OutputTag<EventBasic> operator1OutputTag, OutputTag<EventBasic> operator2OutputTag) {
        this.hotKeyOperatorTag = operator1OutputTag;
        this.operator2OutputTag = operator2OutputTag;
    }

    @Override
    public void processElement(EventBasic value, Context ctx, Collector<EventBasic> out) throws Exception {
//        is popular()
        if (value.key.equals("A") || value.key.equals("B") || value.key.equals("C")){
            ctx.output(hotKeyOperatorTag, value); //send to operator 1

        } else {
            ctx.output(operator2OutputTag, value); //send to operator 2
        }
        //TODO send in the output stream the non popular keys

    }

}

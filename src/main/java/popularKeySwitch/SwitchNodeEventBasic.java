package popularKeySwitch;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

import static StringConstants.StringConstants.WINDOW_END;

public class SwitchNodeEventBasic extends ProcessFunction<EventBasic, EventBasic> {

    private final OutputTag<EventBasic> hotKeyOperatorTag;
    private final OutputTag<EventBasic> operator2OutputTag;

    private int parallelism;

    public SwitchNodeEventBasic(OutputTag<EventBasic> operator1OutputTag, OutputTag<EventBasic> operator2OutputTag, int parallelism) {
        this.hotKeyOperatorTag = operator1OutputTag;
        this.operator2OutputTag = operator2OutputTag;
        this.parallelism = parallelism;

    }

    private HashMap<Long, Integer> endWindowCount = new HashMap<>();

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {

//        if( value.key.equals(WINDOW_END)){
//
//            long timestamp = value.value.timeStamp;
//            if(endWindowCount.containsKey(timestamp)){
//                endWindowCount.put(timestamp, endWindowCount.get(timestamp) + 1);
//            } else {
//                endWindowCount.put(timestamp, 1);
//            }
//
//
////            System.out.println("endWindowCount: " + endWindowCount.get(timestamp) + " timeWindow: " + timestamp + " parallelism: " + parallelism);
//            if(endWindowCount.get(timestamp) <= parallelism/2){
//                ctx.output(hotKeyOperatorTag, value);
//            }else{
//
//                ctx.output(operator2OutputTag, value);
//            }
//        }

        if(event.key.equals(WINDOW_END)){
            if(event.value.valueInt % 2 == 0){
                ctx.output(hotKeyOperatorTag, event);
            } else {
                ctx.output(operator2OutputTag, event);
            }
        }
//        is popular()
        if (event.key.equals('A') || event.key.equals('A') || event.key.equals('C')){

            ctx.output(hotKeyOperatorTag, event); //send to operator 1

        } else {
            ctx.output(operator2OutputTag, event); //send to operator 2
        }

    }

}

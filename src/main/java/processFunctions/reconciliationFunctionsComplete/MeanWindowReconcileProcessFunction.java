package processFunctions.reconciliationFunctionsComplete;



import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MeanWindowReconcileProcessFunction  extends ProcessWindowFunction<EventBasic, EventBasic, String , TimeWindow> {


    private int count;

    private int sum;

    private long timestamp;

//    public MeanWindowReconcileProcessFunction() {
//
//    }

    @Override
    public void process(String s, Context context, Iterable<EventBasic> elements, Collector<EventBasic> out) throws Exception {

        for(EventBasic element: elements){
            timestamp = element.value.timeStamp;
            int value = element.value.valueInt; // sum
            int valuetmp = element.value.valueTmp; // count

            sum += value;
            count +=  valuetmp;
        }
        out.collect(new EventBasic(s, new Value(sum/count, timestamp)));
    }
}

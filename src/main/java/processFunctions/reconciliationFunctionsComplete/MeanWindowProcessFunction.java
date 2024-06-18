package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MeanWindowProcessFunction  extends ProcessWindowFunction<EventBasic, EventBasic, String , TimeWindow> {

    private int count;

    private int sum;

    private long timeStamp;


    @Override
    public void process(String s, Context context, Iterable<EventBasic> elements, Collector<EventBasic> out) throws Exception {

        for(EventBasic element: elements){
            timeStamp = element.value.timeStamp;
            int value = element.value.valueInt;

            sum += value;
            count++;
//            System.out.println("key: "+s+" sum: " + sum + " count: " + count + " mean: " + sum/count);

        }
        out.collect(new EventBasic(s, new Value(sum/count, timeStamp)));
        sum = 0;
        count = 0;
    }
}

package processFunctions.Prototypes;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class basicWindowProcessFunction extends ProcessWindowFunction<EventBasic, EventBasic, String , TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<EventBasic, EventBasic, String, TimeWindow>.Context context, Iterable<EventBasic> elements, Collector<EventBasic> out) throws Exception {
        int sum = 0 ;
        int count = 0;
        for(EventBasic element: elements){
            sum += element.value.valueInt;
            count++;

        }
        out.collect(new EventBasic(s, sum/count, elements.iterator().next().value.timeStamp));
    }
}

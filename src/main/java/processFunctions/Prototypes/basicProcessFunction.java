package processFunctions.Prototypes;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class basicProcessFunction extends ProcessFunction<EventBasic, EventBasic> {

    ArrayList<EventBasic> buffer = new ArrayList<>();


    @Override
    public void processElement(EventBasic value, ProcessFunction<EventBasic, EventBasic>.Context ctx, Collector<EventBasic> out) throws Exception {
        if (buffer.size() == 100){
            int count =0 ;
            int sum = 0;
            for(EventBasic element: buffer){
                sum += element.value.valueInt;
                count++;
            }
            out.collect(new EventBasic(value.key, sum/count, value.value.timeStamp));
            buffer.clear();
        }
    }
}

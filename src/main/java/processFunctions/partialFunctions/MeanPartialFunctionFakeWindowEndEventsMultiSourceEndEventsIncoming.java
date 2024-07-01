package processFunctions.partialFunctions;

import eventTypes.EventBasic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import static StringConstants.StringConstants.WINDOW_END;


import java.util.*;


public class MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming extends ProcessFunction<EventBasic, EventBasic> {
    /***
     * this methods takes in end window events and outputs it, unlike the mena partial functionfakewindowendeventsMuktisource
     * this one does not use watermarking.
     */

    private HashMap<Long, ArrayList<EventBasic>> buffer = new HashMap<>();

    private HashMap<Long, HashSet<Integer>> endWindowEventsReceived = new HashMap<>();
    int sourceParallelism;

    public MeanPartialFunctionFakeWindowEndEventsMultiSourceEndEventsIncoming(int sourceParallelism) {

        this.sourceParallelism = sourceParallelism;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {

        if(Objects.equals(event.key, WINDOW_END)){
            long timeWindow = checkAllEndWindowEventsReceived(event);
            if(checkAllEndWindowEventsReceived(event) != -1){
                outputValues(out,timeWindow);
            }
        }
        else{
            if(!buffer.containsKey(event.value.timeStamp)){
                buffer.put(event.value.timeStamp, new ArrayList<>());
                buffer.get(event.value.timeStamp).add(event);
            }else{
                buffer.get(event.value.timeStamp).add(event);
            }
        }
    }


    public void outputValues(Collector<EventBasic> out, long timestamp) {

       //if is last element, output all
        HashMap<String, EventBasic> sumCount = getProcessedOutput(timestamp);

        for(String key : sumCount.keySet()){
            out.collect(sumCount.get(key));
        }
        out.collect(new EventBasic(WINDOW_END, -1, timestamp));
        buffer.remove(timestamp);

    }

    public HashMap<String, EventBasic> getProcessedOutput(long timestamp) {
        HashMap<String , EventBasic> sumCount = new HashMap<>();
        if (!buffer.containsKey(timestamp)){

            System.out.println("buffer does not contain timestamp");
            System.out.println(buffer);
            return sumCount;

        }
        for(EventBasic event:buffer.get(timestamp)){
            if(!sumCount.containsKey(event.key)){
                sumCount.put(event.key, new EventBasic(event.key, event.value.valueInt, event.value.timeStamp, 1));
            }
            else{
                EventBasic tmp = sumCount.get(event.key);
                sumCount.put(event.key, new EventBasic(event.key, tmp.value.valueInt + event.value.valueInt, event.value.timeStamp, tmp.value.valueTmp + 1));
            }
        }
        return sumCount;
    }


    public long checkAllEndWindowEventsReceived(EventBasic event) throws Exception{
        if(!Objects.equals(event.key, "WindowEnd")){
            throw  new IllegalArgumentException("Not a window end event");
        }
        if(endWindowEventsReceived.containsKey(event.value.timeStamp)){
            HashSet<Integer> set = endWindowEventsReceived.get(event.value.timeStamp);
            set.add(event.value.valueInt);
            if(set.size() == sourceParallelism){
                return event.value.timeStamp;
            }
        }else{
            HashSet<Integer> set = new HashSet<>();
            set.add(event.value.valueInt);
            endWindowEventsReceived.put(event.value.timeStamp, set);
        }
        return -1;
    }
}


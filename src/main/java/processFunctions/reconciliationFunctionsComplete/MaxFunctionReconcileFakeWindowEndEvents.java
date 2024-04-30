package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;


public class MaxFunctionReconcileFakeWindowEndEvents extends ProcessFunction<EventBasic, EventBasic> {

    int parallelism;

    HashMap<Long, List<EventBasic>> eventMap;

    HashMap<Long, Integer> endOfWindowCounter;





    public MaxFunctionReconcileFakeWindowEndEvents(long windowTime , int parallelism) {
        this.parallelism = parallelism ;
        endOfWindowCounter = new HashMap<>();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        eventMap = new HashMap<>();

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {

//        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//        System.out.println("subtaskIndex = " + subtaskIndex + " key = " + key + " event.value.timeStamp = " + event.value.timeStamp +  " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);



        if(eventMap.containsKey(event.value.timeStamp)){
            if(event.key.equals("WindowEnd")){
                updateEndOfWindowCounter(out,event.value.timeStamp);
            }
            else{
                eventMap.get(event.value.timeStamp).add(event);
            }
        }else{
            eventMap.put(event.value.timeStamp, new ArrayList<EventBasic>());
            eventMap.get(event.value.timeStamp).add(event);
        }

    }

    public void updateEndOfWindowCounter(Collector<EventBasic> out,long timeStamp){
        if(endOfWindowCounter.containsKey(timeStamp)){
            endOfWindowCounter.put(timeStamp, endOfWindowCounter.get(timeStamp) + 1);
            if(endOfWindowCounter.get(timeStamp) == parallelism ){
                outputValues(out, timeStamp);
            }
        }
        else{
            endOfWindowCounter.put(timeStamp, 1);
        }
    }

    public void outputValues(Collector<EventBasic> out, long timeStamp) {
        HashMap<String,Integer> sumCountMap = getMaxValues(timeStamp);
        for (String key : sumCountMap.keySet()) {
            out.collect(new EventBasic(key, sumCountMap.get(key),timeStamp));
        }
        eventMap.remove(timeStamp);
    }

    public HashMap<String, Integer> getMaxValues( long timeStamp){
        HashMap<String,Integer> sumCountMap = new HashMap<>();
        for (EventBasic event : eventMap.get(timeStamp)) {
            if(sumCountMap.containsKey(event.key)){
                int valueInt = sumCountMap.get(event.key);
                if (event.value.valueInt > valueInt){
                    sumCountMap.put(event.key, event.value.valueInt);
                }

            } else{
                sumCountMap.put(event.key,event.value.valueInt);
            }
        }
        return sumCountMap;
    }

    public long getTimeStampForWindow(){
        /**
         * this is for when we create windows larger than 1000 and therefore the windows need to be made larger not really the objective for now
         */
        return 0;

    }

//    public void printMapState() throws Exception {
//        System.out.println("Printing MapState contents:");
//        for (String entry : maxValues.keySet()) {
//            System.out.println("Key: " + entry + ", Value: " + maxValues.get(entry));
//        }
//        System.out.println("current time:" + this.currentTime +   " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);
//    }

}


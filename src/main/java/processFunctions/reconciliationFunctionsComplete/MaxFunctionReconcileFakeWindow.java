package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;


public class MaxFunctionReconcileFakeWindow extends ProcessFunction<EventBasic, EventBasic> {



    long currentTime;
    long endWindowTime;
    long windowTime; //in ms
    long startWindowTime;


    int windowElements;

    HashMap<Long, List<EventBasic>> eventMap;

    HashMap<Long, Integer> endOfWindowCounter;





    public MaxFunctionReconcileFakeWindow(long windowTime , int parallelism, int nKeys) {
        windowElements = parallelism * nKeys;
        this.windowTime = windowTime;//in ms
        this.startWindowTime = - windowTime;
        this.endWindowTime = 0;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        eventMap = new HashMap<>();

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;

//        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//        System.out.println("subtaskIndex = " + subtaskIndex + " key = " + key + " event.value.timeStamp = " + event.value.timeStamp +  " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);

        if (Objects.equals(key, "C")){
//            System.out.println("key = " + key);
        }




        if(eventMap.containsKey(event.value.timeStamp)){
            eventMap.get(event.value.timeStamp).add(event);
            System.out.println("eventMap.get(event.value.timeStamp).size() = " + eventMap.get(event.value.timeStamp).size());

            if(event.value.timeStamp == 500){
//                System.out.println("hi " + eventMap.get(event.value.timeStamp).size());
                int count = 1;
                for(EventBasic e : eventMap.get(event.value.timeStamp)){

                    if(e.key.equals("C")){
                        System.out.println(count);
                        count++;
                    }

                }
            }

//            System.out.println(eventMap.get(event.value.timeStamp));
            // check if window is complete
            if(eventMap.get(event.value.timeStamp).size() == windowElements){
                System.out.println("i'm here");
                outputValues(out, event.value.timeStamp);
            }
        }else{
            eventMap.put(event.value.timeStamp, new ArrayList<EventBasic>());
            eventMap.get(event.value.timeStamp).add(event);

        }

    }

    public void outputValues(Collector<EventBasic> out, long timeStamp) {
        HashMap<String,Integer> sumCountMap = getMaxValues(timeStamp);
        for (String key : sumCountMap.keySet()) {
            System.out.println("outputvalues");
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


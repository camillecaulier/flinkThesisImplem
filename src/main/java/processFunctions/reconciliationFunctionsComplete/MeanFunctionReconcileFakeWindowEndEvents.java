package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class MeanFunctionReconcileFakeWindowEndEvents extends ProcessFunction<EventBasic, EventBasic> {

    int parallelism;

    HashMap<Long, List<EventBasic>> eventMap;

    HashMap<Long, Integer> endOfWindowCounter;





    public MeanFunctionReconcileFakeWindowEndEvents(long windowTime , int parallelism) {
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
        HashMap<String,Integer> meanMap = getMeanValues(timeStamp);
        for (String key : meanMap.keySet()) {
//            System.out.println("outputvalues");
            out.collect(new EventBasic(key, meanMap.get(key),timeStamp));
        }
        eventMap.remove(timeStamp);
    }

    public HashMap<String, Integer> getMeanValues(long timeStamp){
        HashMap<String,Tuple2<Integer,Integer>> sumCountMap = new HashMap<>();
        for (EventBasic event : eventMap.get(timeStamp)) {
            if(sumCountMap.containsKey(event.key)){
                Tuple2<Integer,Integer> sumCountPair = sumCountMap.get(event.key);

                sumCountMap.put(event.key, new Tuple2<>(sumCountPair.f0 + event.value.valueInt, sumCountPair.f1 + event.value.valueTmp));


            } else{
                sumCountMap.put(event.key,new Tuple2<>(event.value.valueInt, event.value.valueTmp));
            }
        }

        HashMap<String,Integer> meanMap = new HashMap<>();
        for(String key : sumCountMap.keySet()){
            Tuple2<Integer,Integer> value = sumCountMap.get(key);
            meanMap.put(key, value.f0/value.f1);
        }

        return meanMap;
    }

    public long getTimeStampForWindow(){
        /**
         * this is for when we create windows larger than 1000 and therefore the windows need to be made larger not really the objective for now
         */
        return 0;

    }


}


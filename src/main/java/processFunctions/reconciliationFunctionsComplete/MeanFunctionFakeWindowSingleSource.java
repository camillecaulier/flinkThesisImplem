package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class MeanFunctionFakeWindowSingleSource  extends ProcessFunction<EventBasic, EventBasic> {

    long currentTime;
    long endWindowTime;
    long windowTime; //in ms
    long startWindowTime;
    //sum + count for each key
    private volatile HashMap<String, Tuple2<Integer, Integer>> SumCountMap;



    public MeanFunctionFakeWindowSingleSource(long windowTime) {
        this.windowTime = windowTime;//in ms
        this.startWindowTime = - windowTime;
        this.endWindowTime = 0;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        SumCountMap = new HashMap<>();

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;


        if(event.value.timeStamp > endWindowTime){
            // If the event is in the next window, output the current maximum values and update the window times
            outputMaxValues(out);
            startWindowTime = endWindowTime;
            endWindowTime += windowTime;
            currentTime = event.value.timeStamp;
        }

        if(event.value.timeStamp < startWindowTime){
            System.out.println("AKLERT ALERT ALERT RUN AWAY!!!! SOMETHING ISN'T WORKING!!!!");
        }

        // If no maximum value has been stored yet or the incoming value is greater, update the MapState

        if(!SumCountMap.containsKey(key)){
            SumCountMap.put(key, new Tuple2<>(event.value.valueInt , 1));
        }
        else{
//            long startTime = System.nanoTime();
            Tuple2<Integer, Integer> curr = SumCountMap.get(key);
            SumCountMap.put(key, new Tuple2<>(curr.f0 + event.value.valueInt , curr.f1 + 1));
//            long endTime = System.nanoTime();  // End timing
//            long duration = endTime - startTime;
//            System.out.println("Time taken to update map: " + duration);
        }


    }


    public void outputMaxValues(Collector<EventBasic> out) {
        for (String k : SumCountMap.keySet()) {
            Tuple2<Integer, Integer> curr = SumCountMap.get(k);
            //value int is the sum or vals and valuetmp is the count
            out.collect(new EventBasic(k, new Value(curr.f0/curr.f1, currentTime )));
        }
        out.collect(new EventBasic("WindowEnd", -1, currentTime));
        SumCountMap.clear();
    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : SumCountMap.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + SumCountMap.get(entry));
        }
        System.out.println("current time:" + this.currentTime +   " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);
    }


}

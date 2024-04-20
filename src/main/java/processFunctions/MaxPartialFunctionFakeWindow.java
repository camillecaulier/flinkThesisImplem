package processFunctions;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MaxPartialFunctionFakeWindow extends ProcessFunction<EventBasic, EventBasic> {



    long currentTime;
    long endWindowTime;
    long windowTime; //in ms
    long startWindowTime;
    private volatile HashMap<String, Integer> maxValues;



    public MaxPartialFunctionFakeWindow(long windowTime) {
        this.windowTime = windowTime;//in ms
        this.startWindowTime = - windowTime;
        this.endWindowTime = this.startWindowTime + windowTime;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        maxValues = new HashMap<>();

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;


        if(ctx.timestamp() > endWindowTime){
            outputMaxValues(out);
            startWindowTime = endWindowTime;
            endWindowTime += windowTime;
            currentTime = event.value.timeStamp;
        }

        // If no maximum value has been stored yet or the incoming value is greater, update the MapState

        if(!maxValues.containsKey(key)){
            maxValues.put(key, event.value.valueInt);
        }
        else if(event.value.valueInt > maxValues.get(key)){
            maxValues.put(key, event.value.valueInt);
        }

    }


    public void outputMaxValues(Collector<EventBasic> out) {
        for (String k : maxValues.keySet()) {
            out.collect(new EventBasic(k, new Value(maxValues.get(k), currentTime)));
        }
        maxValues.clear();
    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : maxValues.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + maxValues.get(entry));
        }
    }
//    @Override
//    public void onTimer(long timestamp, KeyedProcessFunction.OnTimerContext ctx, Collector<EventBasic> out) throws Exception {
//        // This timer triggers when no new data has arrived by the time the current watermark exceeds the timer's timestamp
//        outputMaxValues(out);
//    }
}


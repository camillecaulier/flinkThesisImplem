package processFunctions;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;


public class MaxPartialFunctionFakeWindowKeyed extends KeyedProcessFunction<String, EventBasic, EventBasic> {

//    private MapState<String, Integer> maxValues;
    private HashMap<String, Integer> maxValues;
    long windowTime; // in ms
    long startWindowTime;
    long endWindowTime;
    long currentTime;

    public MaxPartialFunctionFakeWindowKeyed(long windowTime) {
        this.windowTime = windowTime; // in ms
        this.startWindowTime = -windowTime;
        this.endWindowTime = 0;
    }

    @Override
    public void open(Configuration parameters) {
//        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>(
//                "maxValues",
//                String.class,
//                Integer.class
//        );
//        maxValues = getRuntimeContext().getMapState(descriptor);
//        super.open(parameters);
        maxValues = new HashMap<>();
    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;
//        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//        System.out.println("subtaskIndex = " + subtaskIndex + " key = " + key + " event.value.timeStamp = " + event.value.timeStamp +  " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);

        if(key.equals("C")){
            System.out.println("key = " + key + " val = "+ event.value.valueInt + " event.value.timeStamp = " + event.value.timeStamp +  " startWindowTime = " + startWindowTime + " endWindowTime = " + endWindowTime + " currentTime = " + currentTime);
//            printMaxState();

        }

        if(ctx.timestamp() > endWindowTime){
//            System.out.println("ctx.timestamp() = " + ctx.timestamp());
            outputMaxValues(out);
            startWindowTime = endWindowTime;
            endWindowTime += windowTime;
            currentTime = event.value.timeStamp;
        }

//        if(event.value.timeStamp < startWindowTime){
//            System.out.println("WHAT THE FUCK");
//        }


        Integer currentMax = maxValues.get(key);
        if (currentMax == null || event.value.valueInt > currentMax) {
            maxValues.put(key, event.value.valueInt);
        }

        if(key.equals("C")){
//            System.out.println("key = " + key +" current max: o"+ currentMax);
        }



        ctx.timerService().registerEventTimeTimer(startWindowTime + windowTime);
    }

//    public void outputMaxValues(Collector<EventBasic> out) throws Exception {
//        for (String k : maxValues.keys()) {
//            Integer maxValue = maxValues.get(k);
//            if(k.equals("C")){
//                System.out.println("hello");
//            }
//            out.collect(new EventBasic(k, new Value(maxValue, currentTime)));
//
//        }
////        System.out.println(maxValues.keys());
//        maxValues.clear();
//    }

    public void outputMaxValues(Collector<EventBasic> out) {
        for (String k : maxValues.keySet()) {
            out.collect(new EventBasic(k, new Value(maxValues.get(k), currentTime)));
        }
        maxValues.clear();
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EventBasic> out) throws Exception {
        // This timer triggers when no new data has arrived by the time the current watermark exceeds the timer's timestamp
        outputMaxValues(out);
    }

//    private void printMaxState() throws Exception {
//        Iterable<Map.Entry<String, Integer>> entries = maxValues.entries();
//        System.out.println("Current Max Values:");
//        for (Map.Entry<String, Integer> entry : entries) {
//            System.out.println("Key: " + entry.getKey() + ", Max Value: " + entry.getValue());
//        }
//    }
}
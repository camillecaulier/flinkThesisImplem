package processFunctions;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;



public class MaxPartialFunctionFakeWindowKeyed extends KeyedProcessFunction<String, EventBasic, EventBasic> {

    private transient MapState<String, Integer> maxValues;
    long windowTime; // in ms
    long startWindowTime;
    long endWindowTime;
    long currentTime;

    public MaxPartialFunctionFakeWindowKeyed(long windowTime) {
        this.windowTime = windowTime; // in ms
        this.startWindowTime = -windowTime;
        this.endWindowTime = this.startWindowTime + windowTime;
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>(
                "maxValues",
                String.class,
                Integer.class
        );
        maxValues = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;


        if(ctx.timestamp() > endWindowTime){
            System.out.println("ctx.timestamp() = " + ctx.timestamp());
            outputMaxValues(out);
            startWindowTime = endWindowTime;
            endWindowTime += windowTime;
            currentTime = event.value.timeStamp;
        }

        // Update max value in state
        Integer currentMax = maxValues.get(key);
        if (currentMax == null || event.value.valueInt > currentMax) {
            maxValues.put(key, event.value.valueInt);
        }
        // In processElement or another method, register a timer
        ctx.timerService().registerEventTimeTimer(startWindowTime + windowTime);
    }

    public void outputMaxValues(Collector<EventBasic> out) throws Exception {
        for (String k : maxValues.keys()) {
            Integer maxValue = maxValues.get(k);
            out.collect(new EventBasic(k, new Value(maxValue, currentTime)));

        }
        maxValues.clear(); // Opotionally clear state after outputting
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EventBasic> out) throws Exception {
        // This timer triggers when no new data has arrived by the time the current watermark exceeds the timer's timestamp
        outputMaxValues(out);
    }
}
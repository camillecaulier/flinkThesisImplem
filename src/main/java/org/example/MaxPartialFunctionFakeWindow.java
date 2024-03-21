package org.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MaxPartialFunctionFakeWindow extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {



    volatile long previousWindowTime;
    volatile long nextWindowTime;
    long windowTime;
    private volatile HashMap<String, Integer> maxValues;

    public MaxPartialFunctionFakeWindow(long i) {
        windowTime = i;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        maxValues = new HashMap<>();

    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        String key = value.f0;

        // If no maximum value has been stored yet or the incoming value is greater, update the MapState
        synchronized (maxValues){
            if(!maxValues.containsKey(key)){
                maxValues.put(key, value.f1);
                out.collect(new Tuple2<>(key, maxValues.get(key)));
            }
            else if(value.f1 > maxValues.get(key)){
                maxValues.put(key, value.f1);
                out.collect(new Tuple2<>(key, maxValues.get(key)));
            }
        }
        if(nextWindowTime == 0){
            nextWindowTime = ctx.timestamp() + windowTime;
            ctx.timerService().registerEventTimeTimer(nextWindowTime);
        }


        // output in ms 60 = 60 000
//        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000);

//        printMapState();
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        synchronized (maxValues){
            for (String k : maxValues.keySet()) {
                out.collect(Tuple2.of(k, maxValues.get(k)));
            }
        }

        nextWindowTime = nextWindowTime + windowTime;
        ctx.timerService().registerEventTimeTimer(nextWindowTime);

    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : maxValues.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + maxValues.get(entry));
        }
    }
}


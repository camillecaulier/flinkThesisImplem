package processFunctions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MaxPartialFunctionFakeWindow extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {



    volatile long previousWindowTime;
    volatile long nextWindowTime;
    long windowTime; //in ms
    long lastWindowTime;
    private volatile HashMap<String, Integer> maxValues;


    public MaxPartialFunctionFakeWindow(long windowTime) {
        this.windowTime = windowTime;//in ms
        this.lastWindowTime = 0;
        this.nextWindowTime = this.lastWindowTime + windowTime;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        maxValues = new HashMap<>();

    }

    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        String key = value.f0;

        System.out.println(ctx.timestamp());
        if(ctx.timestamp() > nextWindowTime){
            for (String k : maxValues.keySet()) {
                out.collect(new Tuple2<>(k, maxValues.get(k)));
            }
            lastWindowTime = nextWindowTime;
            nextWindowTime = lastWindowTime + windowTime;
        }

        // If no maximum value has been stored yet or the incoming value is greater, update the MapState

        if(!maxValues.containsKey(key)){
            maxValues.put(key, value.f1);
        }
        else if(value.f1 > maxValues.get(key)){
            maxValues.put(key, value.f1);
        }

        if(nextWindowTime == 0){
            nextWindowTime = ctx.timestamp() + windowTime;
        }


        // output in ms 60 = 60 000

//        printMapState();
    }



    public void outputMaxValues(Collector<Tuple2<String, Integer>> out) {
        for (String k : maxValues.keySet()) {
            out.collect(new Tuple2<>(k, maxValues.get(k)));
        }
    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : maxValues.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + maxValues.get(entry));
        }
    }
}


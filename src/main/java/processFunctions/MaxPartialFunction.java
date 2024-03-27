package processFunctions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;


public class MaxPartialFunction extends ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

//    private transient MapState<String, Integer> maxValues;

    private volatile HashMap<String, Integer> maxValues;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize the MapState
//        MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>("maxValues", String.class, Integer.class);
//        maxValues = getRuntimeContext().getMapState(descriptor);
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

        // Emit the updated maximum value for the key

//        printMapState();
    }

    public void printMapState() throws Exception {
        System.out.println("Printing MapState contents:");
        for (String entry : maxValues.keySet()) {
            System.out.println("Key: " + entry + ", Value: " + maxValues.get(entry));
        }
    }
}


package processFunctions.reconciliationFunctionsComplete;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

public class MeanFunctionFakeWindowMultiSource extends ProcessFunction<EventBasic, EventBasic> {

    long currentTime;
    long endWindowTime;
    long windowTime; //in ms
    long startWindowTime;
    //sum + count for each key

    long currentWatermark =-9223372036854775808L;


    private HashMap<Long, ArrayList<EventBasic>> buffer = new HashMap<>();

    int outOfOrderness;
    int numberOfWindows;


    public MeanFunctionFakeWindowMultiSource(long windowTime, int outOfOrderness, int numberOfWindows) {
        this.windowTime = windowTime;//in ms
        this.startWindowTime = - windowTime;
        this.endWindowTime = 0;
        this.outOfOrderness = outOfOrderness;
        this.numberOfWindows = numberOfWindows;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void processElement(EventBasic event, Context ctx, Collector<EventBasic> out) throws Exception {
        String key = event.key;

        checkWatermarkOrder(ctx);//just check if i'm not fuckign everything up



        long watermark = ctx.timerService().currentWatermark();

        if(watermark > currentWatermark){
            currentWatermark = watermark;
            outputValues(out);
        }

        if(!buffer.containsKey(event.value.timeStamp)){
            buffer.put(event.value.timeStamp, new ArrayList<>());
        }else{
            buffer.get(event.value.timeStamp).add(event);

        }


    }


    public void outputValues(Collector<EventBasic> out) {

        for(long timestamp : buffer.keySet()){
            if (timestamp < currentWatermark || isLastElement()){ //if is last element, output all
                HashMap<String, EventBasic> sumCount = getProcessedOutput(timestamp);

                for(String key : sumCount.keySet()){
                    out.collect(sumCount.get(key));
                }
                buffer.remove(timestamp);
            }
        }


    }

    public HashMap<String, EventBasic> getProcessedOutput(long key) {
        HashMap<String , EventBasic> sumCount = new HashMap<>();
        for(EventBasic event:buffer.get(key)){
            if(!sumCount.containsKey(event.key)){
                sumCount.put(event.key, new EventBasic(event.key, event.value.valueInt, event.value.timeStamp, 1));
            }
            else{
                EventBasic tmp = sumCount.get(event.key);
                sumCount.put(event.key, new EventBasic(event.key, tmp.value.valueInt + event.value.valueInt, event.value.timeStamp, tmp.value.valueTmp + 1));
            }
        }
        return sumCount;
    }


    public void checkWatermarkOrder(Context ctx){
        if(currentWatermark > ctx.timerService().currentWatermark()){
            System.out.println("Watermark is not increasing");
        }
        currentWatermark = ctx.timerService().currentWatermark();
        System.out.println(currentWatermark);
    }

    public boolean isLastElement(){
        return currentWatermark == (1000 * numberOfWindows - (outOfOrderness + 1) - 1000);
    }


}

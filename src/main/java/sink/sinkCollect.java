package sink;

import eventTypes.EventBasic;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class sinkCollect implements SinkFunction<EventBasic> {

//    public static List<EventBasic> values = Collections.synchronizedList(new ArrayList<EventBasic>());

    public static volatile List<EventBasic> values = new CopyOnWriteArrayList<>();


    @Override
    public synchronized void invoke(EventBasic value, Context context) {
//        System.out.println("context: "+context.timestamp()+ "watermarkL "+context.currentWatermark());
        values.add(value);
        if(value == null){
            System.out.println("value is null");
        }
    }

    public static synchronized List<EventBasic> getValues() {
        return values;
    }





}

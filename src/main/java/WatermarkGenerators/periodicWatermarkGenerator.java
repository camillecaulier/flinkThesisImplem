package WatermarkGenerators;

import eventTypes.EventBasic;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class periodicWatermarkGenerator implements WatermarkGenerator<EventBasic> {
    private long timeStamp;

    private final ConcurrentHashMap<Long, AtomicInteger> timestampCountMap = new ConcurrentHashMap<>();

    public periodicWatermarkGenerator(int elementsPerWindow){
        timeStamp = 0;
//        id = random();

    }

    @Override
    public void onEvent(EventBasic event, long eventTimestamp, WatermarkOutput output) {
        timeStamp  = Math.max(event.value.timeStamp, timeStamp);
//        output.emitWatermark(new Watermark(timeStamp));
//        System.out.println("Emitted Watermark: " + timeStamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        output.emitWatermark(new Watermark(timeStamp));
//        System.out.println("Emitted Watermark: " + timeStamp);

    }
}

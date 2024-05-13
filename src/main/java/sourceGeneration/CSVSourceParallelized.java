package sourceGeneration;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class CSVSourceParallelized implements FlatMapFunction<String, EventBasic> {
    @Override
    public void flatMap(String value, Collector<EventBasic> out) throws Exception {
        //Assuming CSV format is Key,ValueInt,ValueTimeStamp
        String[] parts = value.split(",");
        if (parts.length == 3) {
            String key = parts[0].trim();
            int valueInt = Integer.parseInt(parts[1]);
            long valueTimeStamp =  Long.parseLong(parts[2]);
            out.collect(new EventBasic(key, new Value(valueInt, valueTimeStamp)));
        } else {
            System.err.println("Invalid line: " + value);
        }
    }
}

//package sourceGeneration;
//
//import eventTypes.EventBasic;
//import eventTypes.Value;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//
//public class CSVSource extends RichParallelSourceFunction<Tuple2<String, Value>> {
//    private volatile boolean running = true;
//    private String csvFilePath;
//
//    public CSVSource(String csvFilePath) {
//        this.csvFilePath = csvFilePath;
//    }
//
//    @Override
//    public void run(SourceFunction.SourceContext<Tuple2<String, Value>> sourceContext) throws Exception {
//        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null && running) {
//                // Assuming CSV format is Key,ValueInt,ValueTimeStamp
//                String[] parts = line.split(",");
//                if (parts.length == 3) {
//                    String key = parts[0].trim();
//                    System.out.println(parts[1].trim());
//                    int valueInt = Integer.parseInt(parts[1]);
//                    long valueTimeStamp =  Long.parseLong(parts[2]);; // Assuming ValueTimeStamp is an integer
//                    sourceContext.collect(new Tuple2<>(key, new Value(valueInt, valueTimeStamp)));
//
//                } else {
//                    System.err.println("Invalid line: " + line);
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void cancel() {
//        running = false;
//    }
//}

package sourceGeneration;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.flink.api.common.state.MapState;

public class CSVSource extends RichParallelSourceFunction<EventBasic> {
    private String csvFilePath;

    public CSVSource(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }


    @Override
    public void run(SourceFunction.SourceContext<EventBasic> sourceContext) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Assuming CSV format is Key,ValueInt,ValueTimeStamp
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    String key = parts[0].trim();
                    int valueInt = Integer.parseInt(parts[1]);
                    long valueTimeStamp =  Long.parseLong(parts[2]);
                    sourceContext.collect(new EventBasic(key, new Value(valueInt, valueTimeStamp)));

                } else {
                    System.err.println("Invalid line: " + line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
//        running = false;
    }
}


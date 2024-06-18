package sourceGeneration;

import eventTypes.EventBasic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.api.watermark.Watermark.waterheadmark;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.streaming.api.watermark.Watermark;

public class CsvEventBasicSourceFunction implements SourceFunction<EventBasic> {

    private final String filePath;
    private volatile boolean isRunning = true;

    public CsvEventBasicSourceFunction(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<EventBasic> ctx) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    String key = parts[0];
                    int valueInt = Integer.parseInt(parts[1]);
                    long valueTimeStamp = Long.parseLong(parts[2]);
                    EventBasic event = new EventBasic(key, valueInt, valueTimeStamp);

                    ctx.collect(event);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("The file was not found: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("Error reading the file: " + e.getMessage());
        }
    }

    @Override
    public void cancel() {

    }
}

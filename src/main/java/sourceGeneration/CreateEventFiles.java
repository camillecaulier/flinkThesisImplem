package sourceGeneration;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import eventTypes.Value;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import eventTypes.EventBasic;
import org.apache.commons.text.RandomStringGenerator;

public class CreateEventFiles {
    public static void writeEventsToCSV(List<EventBasic> events, String filename, int Count ) throws IOException {
        try (Writer writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Key", "ValueInt", "ValueTimeStamp"))) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            for (EventBasic event : events) {
                csvPrinter.printRecord(event.key, event.value.valueInt, event.value.valueTimeStamp.format(formatter));
            }
        }
    }


    public static void UniformDistribution(int stampsPerSecond, String filename, int keySize, int time) {
        try (Writer writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("Key", "ValueInt", "ValueTimeStamp"))) {

            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            RandomStringGenerator rng = new RandomStringGenerator.Builder()
                    .withinRange('A', 'Z')
                    .usingRandom(rnd::nextInt)
                    .build();

            for(int t = 0 ; t < time; t++ ){
                for (int i = 0; i < stampsPerSecond; i++) {
                    Value value = new Value();
                    value.valueInt = i;
                    value.timeStamp = (long) (t * 1000L + 500); // 500 ms offset
                    EventBasic event = new EventBasic(rng.generate(keySize), value);

                    // Write event to CSV
                    csvPrinter.printRecord(event.key, value.valueInt, value.timeStamp);
                }
            }


            System.out.println("Events written to file: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }






    public static void main(String[] args) {
        // Generate 1000 events with a key size of 1
        UniformDistribution(10000, "uniform_distribution.csv", 1, 5);
    }
}

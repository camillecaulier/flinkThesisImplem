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
import org.apache.commons.math3.distribution.ZipfDistribution;


public class CreateEventFiles {
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
                    Value value = new Value(i,t * 1000L + 500);
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


    public static void zipfDistribution(int stampsPerSecond, String filename, int keySize, int time, double skew) throws IOException {
        try (Writer writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)){
//            CSVPrinter csvPrinter = new CSVPrinter(writer, null)){

            ZipfDistribution zipfDistribution = new ZipfDistribution(26, skew); // 26 for the number of characters in the English alphabet

            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            RandomStringGenerator rng = new RandomStringGenerator.Builder()
                    .withinRange('A', 'Z')
                    .usingRandom((x) -> {
                        int randomIndex = zipfDistribution.sample() - 1; // Sample from the Zipf distribution and subtract 1 to make it zero-based
                        return randomIndex; // Convert the random index to a character in the range 'A' to 'Z'
                    })
                    .build();

            for(int t = 0 ; t < time; t++ ){
                for (int i = 0; i < stampsPerSecond; i++) {
                    Value value = new Value(i,t * 1000L + 500);
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



    public static void main(String[] args) throws IOException {
        // Generate 1000 events with a key size of 1
//        UniformDistribution(10000, "uniform_distribution.csv", 1, 5);
        zipfDistribution(1000000, "zipf_distribution1000000_5.csv", 1, 5, 1.5);
    }
}

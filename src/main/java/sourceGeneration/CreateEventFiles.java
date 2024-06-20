package sourceGeneration;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import eventTypes.Value;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import eventTypes.EventBasic;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.commons.math3.distribution.ZipfDistribution;


public class CreateEventFiles {

    private static long seed = 123456L;
    public static void UniformDistribution(int stampsPerSecond, String filename, int keySize, int time) {
        try (Writer writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)){

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

    public static String convertToLetter(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException("Number must be positive");
        }

        StringBuilder result = new StringBuilder();
        while (number > 0) {
            int remainder = (number - 1) % 26; // Remainder when divided by 26
            char letter = (char) (remainder + 'A'); // Convert remainder to corresponding letter
            result.insert(0, letter); // Prepend the letter to the result
            number = (number - 1) / 26; // Update the number for next iteration
        }

        return result.toString();
    }


    public static void zipfDistribution(int stampsPerSecond, String filename, int keySize, int time, double skew) throws IOException {
        try (Writer writer = new FileWriter(filename); CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)){
            RandomGenerator randomGenerator = new Well19937c(seed);

            ZipfDistribution zipfDistribution = new ZipfDistribution(randomGenerator,(int) Math.pow(26,keySize), skew); // 26 for the number of characters in the English alphabet

            for(int t = 0 ; t < time; t++ ){
                for (int i = 0; i < stampsPerSecond; i++) {
                    Value value = new Value(i,t * 1000L + 500);
                    EventBasic event = new EventBasic(convertToLetter(zipfDistribution.sample()), value);

                    // Write event to CSV
                    csvPrinter.printRecord(event.key, value.valueInt, value.timeStamp);
//                    csvPrinter.flush();
                }
            }

            //add end of all processing

            for (int i = 0; i < 20; i++) {
                Value value = new Value(i,(time) * 1000L + 500);
                csvPrinter.printRecord("ENDD", value.valueInt, value.timeStamp);
            }


            System.out.println("Events written to file: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void zipfDistributionMultipleSources(int stampsPerSecond, String filename, int keySize, int numberOfWindows, double skew, int sources) throws IOException {
        try (Writer writer = new FileWriter(filename); CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT)){
            List<RandomGenerator> randomGenerators = new ArrayList<>();
            for(int i = 0 ;i < sources; i++){
                randomGenerators.add(new Well19937c(seed + i));
            }

            List<ZipfDistribution> zipfDistributions = new ArrayList<>();
            for(int i = 0 ;i < sources; i++){
                zipfDistributions.add(new ZipfDistribution(randomGenerators.get(i),(int) Math.pow(26,keySize), skew));
            }
            RandomGenerator randomGenerator = new Well19937c(seed);

            ZipfDistribution zipfDistribution = new ZipfDistribution(randomGenerator,(int) Math.pow(26,keySize), skew); // 26 for the number of characters in the English alphabet

            for(int t = 0 ; t < numberOfWindows; t++ ){
                for(int s = 0 ; s < sources; s++){
                    for (int i = 0; i < stampsPerSecond/sources; i++) {
                        Value value = new Value(i,t * 1000L + 500);
                        EventBasic event = new EventBasic(convertToLetter(zipfDistribution.sample()), value);

                        // Write event to CSV
                        csvPrinter.printRecord(event.key, value.valueInt, value.timeStamp);

                    }
                }
            }

            //add end of all processing

            for (int i = 0; i < 20; i++) {
                Value value = new Value(i,(numberOfWindows) * 1000L + 500);
                csvPrinter.printRecord("ENDD", value.valueInt, value.timeStamp);
            }


            System.out.println("Events written to file: " + filename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws IOException {
//        double[] skewValues = {0.000000000000001,0.35, 0.7, 1.4, 2.1};
//        int[] keySizes = {1,2,3};
//        int[] timeValues = {2};
//        int stampsPerSecond = 2500000;
//        for (int keySize : keySizes) {
//            for (int time : timeValues) {
//                for (double skew : skewValues) {
//                    String filename = "data_2_2500000/zipf_distribution_"+ stampsPerSecond+"_" + keySize + "_" + time + "_" + skew + ".csv";
//                    zipfDistribution(stampsPerSecond, filename, keySize, time, skew);
//                }
//            }
//        }


        double[] skewValues = {0.000000000000001, 1.4};
        int[] keySizes = {2};
        int[] windows = {2};
        int stampsPerSecond = 100;
        for (int keySize : keySizes) {
            for (int time : windows) {
                for (double skew : skewValues) {
                    String filename = "dataJavaMultiSourceTestData/zipf_distribution_"+ stampsPerSecond+"_" + keySize + "_" + time + "_" + skew + ".csv";
                    zipfDistribution(stampsPerSecond, filename, keySize, time, skew);
                }
            }
        }



    }
}
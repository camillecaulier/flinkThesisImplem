package Verification;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MeanValuesByWindow {
    static class ValueStats {
        int sum = 0;
        int count = 0;

        void addValue(int value) {
            sum += value;
            count++;
        }

        int getMean() {
            return count == 0 ? 0 : (int) sum / count;
        }
    }

    // Assuming window size in milliseconds
    public static void processFile(String filePath, long windowSize) {
        Map<String, ValueStats> currentValues = new HashMap<>();
        long windowStart = -1;

        try {
            FileReader fileReader = new FileReader(filePath);
            CSVParser parser = new CSVParser(fileReader, CSVFormat.DEFAULT);
            for (CSVRecord record : parser) {
                String key = record.get(0);
                int value = Integer.parseInt(record.get(1));
                long timeStamp = Long.parseLong(record.get(2));

                // Initialize window start if this is the first record
                if (windowStart == -1) {
                    windowStart = timeStamp;
                }

                // If current record's timestamp exceeds the current window's end time, print and reset
                if (timeStamp >= windowStart + windowSize) {
                    printWindowMeanValues(currentValues, windowStart, windowStart + windowSize);
                    currentValues.clear();
                    windowStart = timeStamp; // Set new window start to current timestamp
                }

                currentValues.computeIfAbsent(key, k -> new ValueStats()).addValue(value);
            }

            if (!currentValues.isEmpty()) {
                printWindowMeanValues(currentValues, windowStart, windowStart + windowSize);
            }

            parser.close();
            fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printWindowMeanValues(Map<String, ValueStats> currentValues, long windowStart, long windowEnd) {
        System.out.println("Window from " + windowStart + " to " + windowEnd + ":");
        for (Map.Entry<String, ValueStats> entry : currentValues.entrySet()) {
            System.out.println("Key: " + entry.getKey() + ", Mean Value: " + entry.getValue().getMean());
        }
        System.out.println();
    }

    public static void main(String[] args) {
        String filePath = "zipf_distribution100_5.csv"; // Update this path
        long windowSize = 1000; // Window size in milliseconds
        processFile(filePath, windowSize);
    }
}

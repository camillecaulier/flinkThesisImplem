package Verification;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MaxValuesByWindow {
    // Assuming window size in milliseconds
    public static void processFile(String filePath, long windowSize) {
        Map<String, Integer> currentValues = new HashMap<>();
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
                    printWindowMaxValues(currentValues, windowStart, windowStart + windowSize);
                    currentValues.clear();
                    windowStart = timeStamp; // Set new window start to current timestamp
                }

                currentValues.put(key, Math.max(currentValues.getOrDefault(key, Integer.MIN_VALUE), value));
            }


            if (!currentValues.isEmpty()) {
                printWindowMaxValues(currentValues, windowStart, windowStart + windowSize);
            }

            parser.close();
            fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void printWindowMaxValues(Map<String, Integer> currentValues, long windowStart, long windowEnd) {
        System.out.println("Window from " + windowStart + " to " + windowEnd + ":");
        for (Map.Entry<String, Integer> entry : currentValues.entrySet()) {
            if (entry.getKey().equals("A") || entry.getKey().equals("B") || entry.getKey().equals("C")){
                System.out.println("Key: " + entry.getKey() + ", Max Value: " + entry.getValue());
            }

        }
        System.out.println();
    }

    public static void main(String[] args) {
        String filePath = "zipf_distribution100_5.csv"; // Update this path
        long windowSize = 1000; // Window size in milliseconds
        processFile(filePath, windowSize);
    }
}
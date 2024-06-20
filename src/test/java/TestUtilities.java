import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import eventTypes.EventBasic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestUtilities {

    public TestUtilities(){

    }


    public static Set<EventBasic> readCSV(String filePath) {
        System.out.println("Reading csv file:" + filePath);
        Set<EventBasic> result = new HashSet<>();
        String line;
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine();

            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvSplitBy);
                long window = Long.parseLong(values[0]);
                String key = values[1];
                int mean = Integer.parseInt(values[2]);

                EventBasic wkm = new EventBasic(key, mean, window);
                result.add(wkm);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void printDetails(Set<EventBasic> expectedEvents, EventBasic ce){
        System.out.println("Ce: " + ce);
        System.out.println(expectedEvents.contains(ce));
        System.out.println("Expected events: " + expectedEvents);
    }
}

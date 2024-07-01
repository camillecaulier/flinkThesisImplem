import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import StringConstants.StringConstants;
import eventTypes.EventBasic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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


    public static ArrayList<EventBasic> readCSVList(String filePath) {
        System.out.println("Reading csv file:" + filePath);
        ArrayList<EventBasic> result = new ArrayList<>();
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

    public static Set<EventBasic> readSourceCSV(String filePath) {
        System.out.println("Reading csv file:" + filePath);
        Set<EventBasic> result = new HashSet<>();
        String line;
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvSplitBy);
                String key = values[0];
                int value = Integer.parseInt(values[1]);
                long window = Long.parseLong(values[2]);
                EventBasic wkm = new EventBasic(key, value, window);
                result.add(wkm);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static ArrayList<EventBasic> readSourceCSVList(String filePath) {
        System.out.println("Reading csv file:" + filePath);
        ArrayList<EventBasic> result = new ArrayList<>();
        String line;
        String csvSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvSplitBy);
                String key = values[0];
                int value = Integer.parseInt(values[1]);
                long window = Long.parseLong(values[2]);
                EventBasic wkm = new EventBasic(key, value, window);
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

    public static void removeENDD(List<EventBasic> events){
        events.removeIf(eventBasic -> eventBasic.key.equals(StringConstants.ENDD));
    }

    public static void printDetails(List<EventBasic> expectedEvents, EventBasic ce){
        System.out.println("Ce: " + ce);
        System.out.println(expectedEvents.contains(ce));
        System.out.println("Expected events: " + expectedEvents);
    }

    public static void removeNull(List<EventBasic> collectedEvents){
        if(collectedEvents.contains(null)){
            System.out.println("null in collected events");
            int count = 0;
            while(collectedEvents.remove(null)){
                count++;
            }
            System.out.println("removed " + count + " nulls");

        }

    }
}

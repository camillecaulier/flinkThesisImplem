import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import sink.sinkCollect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public class TestMeanOperator  {
    public Class clazz;

    public int parallelism = 6;// good number for testing
    public boolean isJavaSource = false;
    public int sourceParallelism = 1;
    private CompleteOperator<EventBasic> operator;
    final Class[] operators = {
            MeanBasic.class,
            MeanHybrid.class,
            MeanAggregateAware.class,
            MeanRoundRobin.class,

    };


    public CompleteOperator<EventBasic> giveOperator(Class clazz, StreamExecutionEnvironment env, String csvFilePath){
        if (clazz == MeanBasic.class){
            return new MeanBasic(csvFilePath, env, parallelism,isJavaSource, sourceParallelism);
        } else if (clazz == MeanHybrid.class){
            return new MeanHybrid(csvFilePath, env, parallelism/2, parallelism/2,isJavaSource, sourceParallelism);
        } else if (clazz == MeanAggregateAware.class){
            return new MeanAggregateAware(csvFilePath, env, parallelism, 3,isJavaSource, sourceParallelism);
        } else if (clazz == MeanRoundRobin.class){
            return new MeanRoundRobin(csvFilePath, env, parallelism,isJavaSource, sourceParallelism);
        }else{
            return null;
        }
    }


    public static Set<EventBasic> readCSV(String filePath) {
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


    @Test
    public void testNoSkewLong() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String csvFilePath = "data_10_100000/zipf_distribution_100000_2_10_1.0E-15.csv";
        String csvFilePathCorrect = "src/test/java/csvFileReadTestCorrectValues/zipf_distribution_100000_2_10_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);

            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }

//            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                assert(expectedEvents.contains(event));
                expectedEvents.remove(event);
            }
            System.out.println(expectedEvents);
            System.out.println(expectedEvents.size());
            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
        }
    }

    @Test
    public void testNoSkewShort() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String csvFilePath = "data_10_100000/zipf_distribution_100_1_5_1.0E-15.csv";
        String csvFilePathCorrect = "src/test/java/csvFileReadTestCorrectValues/zipf_distribution_100_1_5_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            sinkCollect.values.clear();

            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
//            System.out.println(expectedEvents);
            for(EventBasic event : collectedEvents){
                assert(expectedEvents.contains(event));
                expectedEvents.remove(event);
            }
            System.out.println("Expected events: " + expectedEvents); // the only thing left should be the end of windows which is

            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
            System.out.println(expectedEvents.size());
            // [EventBasic{key='oA', value=Value{valueInt=9, timeStamp=5500}}]
//            EventBasic endEvent = new EventBasic("A", 9, 5500);
//            assert(expectedEvents.contains(endEvent));
//            expectedEvents.remove(endEvent);
        }

    }

    @Test
    public void testHighSkew() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String csvFilePath = "data_10_100000/zipf_distribution_100000_2_10_1.4.csv";
        String csvFilePathCorrect = "src/test/java/csvFileReadTestCorrectValues/zipf_distribution_100000_2_10_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }
            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
//            System.out.println(collectedEvents);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                assert(expectedEvents.contains(event));

                expectedEvents.remove(event);
            }
            System.out.println(expectedEvents);
            System.out.println(expectedEvents.size());
            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
        }
    }
}

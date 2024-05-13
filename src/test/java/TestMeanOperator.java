import CompleteOperators.AggregateAware.MaxAggregateAware;
import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MaxBasic;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hybrid.MaxHybrid;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MaxRoundRobin;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sink.sinkCollect;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class TestMeanOperator  {
    public Class clazz;

    public int parallelism = 6;// good number for testing
    private CompleteOperator<EventBasic> operator;
    final Class[] operators = {
            MeanBasic.class,
            MeanHybrid.class,
            MeanAggregateAware.class,
            MeanRoundRobin.class,

    };


    public CompleteOperator<EventBasic> giveOperator(Class clazz, StreamExecutionEnvironment env, String csvFilePath){
        if (clazz == MeanBasic.class){
            return new MeanBasic(csvFilePath, env, parallelism);
        } else if (clazz == MeanHybrid.class){
            return new MeanHybrid(csvFilePath, env, parallelism/2, parallelism/2);
        } else if (clazz == MeanAggregateAware.class){
            return new MeanAggregateAware(csvFilePath, env, parallelism, 3);
        } else if (clazz == MeanRoundRobin.class){
            return new MeanRoundRobin(csvFilePath, env, parallelism);
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
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        String csvFilePath = "data_10_100000/zipf_distribution_100000_2_10_1.0E-15.csv";
        String csvFilePathCorrect = "src/test/java/zipf_distribution_100000_2_10_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
                System.out.println(event);
                assert(expectedEvents.contains(event));
                expectedEvents.remove(event);
            }
        }



    }

    @Test
    public void testNoSkewShort() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        String csvFilePath = "data_10_100000/zipf_distribution_100_1_5_1.0E-15.csv";
        String csvFilePathCorrect = "src/test/java/zipf_distribution_100_1_5_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
            System.out.println(expectedEvents);
            for(EventBasic event : collectedEvents){
                System.out.println(event);
                System.out.println(expectedEvents.contains(event));
                assert(expectedEvents.contains(event));
                expectedEvents.remove(event);
            }
        }



    }

    @Test
    public void testHighSkew() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        String csvFilePath = "data_10_100000/zipf_distribution_100000_2_10_1.4.csv";
        String csvFilePathCorrect = "src/test/java/zipf_distribution_100000_2_10_1.4o_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = readCSV(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                assert(expectedEvents.contains(event));
                expectedEvents.remove(event);
            }
        }
    }
}

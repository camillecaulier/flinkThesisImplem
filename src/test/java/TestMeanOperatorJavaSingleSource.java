import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.Cam_roundrobin_choices.MeanCAMRoundRobin;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hash.MeanHash;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import sink.sinkCollect;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class TestMeanOperatorJavaSingleSource {

    public boolean isJavaSource = true;
    public int sourceParallelism = 1;
    public RuntimeExecutionMode executionMode = RuntimeExecutionMode.STREAMING;
    public int parallelism = 6;// good number for testing
    final Class[] operators = {
            MeanBasic.class,
//            MeanHybrid.class,
            MeanAggregateAware.class,
            MeanRoundRobin.class,
            MeanHash.class,
            MeanCAMRoundRobin.class

    };


    public CompleteOperator<EventBasic> giveOperator(Class clazz, StreamExecutionEnvironment env, String javaSourceParameters){
        if (clazz == MeanBasic.class){
            return new MeanBasic(javaSourceParameters, env, parallelism, isJavaSource, sourceParallelism);
        } else if (clazz == MeanHybrid.class){
            return new MeanHybrid(javaSourceParameters, env, parallelism/2, parallelism/2, isJavaSource, sourceParallelism);
        } else if (clazz == MeanAggregateAware.class){
            return new MeanAggregateAware(javaSourceParameters, env, parallelism, 3, isJavaSource, sourceParallelism);
        } else if (clazz == MeanRoundRobin.class){
            return new MeanRoundRobin(javaSourceParameters, env, parallelism, isJavaSource, sourceParallelism);
        }else if (clazz == MeanHash.class){
            return new MeanHash(javaSourceParameters, env, parallelism, isJavaSource, sourceParallelism);
        } else if (clazz == MeanCAMRoundRobin.class) {
            return new MeanCAMRoundRobin(javaSourceParameters, env, parallelism, 3, isJavaSource, sourceParallelism);
        } else{
            return null;
        }
    }




    public List<EventBasic> waitForCompletion() throws InterruptedException {
        List<EventBasic> snapshot;
        do {
            snapshot = new ArrayList<>(sinkCollect.values);
            Thread.sleep(2000); // Wait for a second
        } while (!snapshot.equals(sinkCollect.values));
        return sinkCollect.values;
    }


    @Test
    public void testNoSkewLong() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(executionMode);

        String javaSourceParameters = "zipfdistribution,100000,10,2,1.0E-15";
        String csvFilePathCorrect = "src/test/java/javaSourceTestCorrectValues/zipf_distribution_100000_2_10_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, javaSourceParameters);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );



//            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());

            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }

            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents = TestUtilities.readCSV(csvFilePathCorrect);
//            System.out.println(expectedEvents);
//            System.out.println(collectedEvents);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                if(!event.key.equals("ENDD") && !event.key.equals("WindowEnd")){
                    assert(expectedEvents.contains(event));

                }

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
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        String csvFilePath = "zipfdistribution,100,10,2,1.0E-15";
        String csvFilePathCorrect = "src/test/java/javaSourceTestCorrectValues/zipf_distribution_100_2_10_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            List<EventBasic> collectedEvents = waitForCompletion();
            collectedEvents =new ArrayList<>(sinkCollect.values);


            sinkCollect.values.clear();

            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }

            Set<EventBasic> expectedEvents =TestUtilities.readCSV(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
                if(!event.key.equals("ENDD") && !event.key.equals("WindowEnd")) {
//                    System.out.println(event);
//                    System.out.println(expectedEvents.contains(event));
//                    System.out.println("Expected events: " + expectedEvents);
                    assert (expectedEvents.contains(event));
                    expectedEvents.remove(event);

                }

            }
            System.out.println("Expected events: " + expectedEvents); // the only thing left should be the end of windows which is
            System.out.println("collectedEvents: " + collectedEvents);
            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);

        }

    }

    @Test
    public void testHighSkew() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(executionMode);

        String csvFilePath = "zipfdistribution,100000,10,2,1.4";
        String csvFilePathCorrect = "src/test/java/javaSourceTestCorrectValues/zipf_distribution_100000_2_10_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );


//            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());


            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }
            sinkCollect.values.clear();

            Set<EventBasic> expectedEvents =TestUtilities.readCSV(csvFilePathCorrect);
//            System.out.println(collectedEvents);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                if(!event.key.equals("ENDD") && !event.key.equals("WindowEnd")){
                    assert(expectedEvents.contains(event));
                }


                expectedEvents.remove(event);
            }
            System.out.println(expectedEvents);
            System.out.println(expectedEvents.size());
            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
        }
    }



    @Test
    public void testHighSkewShort() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(executionMode);

        String csvFilePath = "zipfdistribution,100,2,2,1.4";
        String csvFilePathCorrect = "src/test/java/javaSourceTestCorrectValues/zipf_distribution_100_2_2_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());

            sinkCollect.values.clear();

            if(collectedEvents.contains(null)){
                System.out.println("null in collected events");
                int count = 0;
                while(collectedEvents.remove(null)){
                    count++;
                }
                System.out.println("removed " + count + " nulls");

            }

            Set<EventBasic> expectedEvents =TestUtilities.readCSV(csvFilePathCorrect);
            System.out.println("Expected events: " + expectedEvents);
            System.out.println("collected events: " + collectedEvents);
            for(EventBasic event : collectedEvents){

                if(!event.key.equals("ENDD") && !event.key.equals("WindowEnd")){
                    printDetails(expectedEvents, event);
                    assert(expectedEvents.contains(event));
                }

                expectedEvents.remove(event);
            }
            System.out.println("Expected events: " + expectedEvents); // the only thing left should be the end of windows which is

            assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
            System.out.println(expectedEvents.size());
            // [EventBasic{key='oA', value=Value{valueInt=9, timeStamp=5500}}]

        }

    }

    public void printDetails(Set<EventBasic> expectedEvents, EventBasic ce){
        System.out.println("Ce: " + ce);
        System.out.println(expectedEvents.contains(ce));
        System.out.println("Expected events: " + expectedEvents);
    }
}

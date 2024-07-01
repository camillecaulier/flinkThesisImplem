import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.Cam_roundrobin_choices.MeanCAMRoundRobin;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hash.MeanHash;
import CompleteOperators.HashRoundRobin.MeanHashRoundRobin;
import CompleteOperators.Hybrid.MeanHybrid;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import StringConstants.StringConstants.*;
import benchmarks.JavaSourceParameters;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import sink.sinkCollect;
import sourceGeneration.ZipfStringSourceRichProcessFunctionEndWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static StringConstants.StringConstants.ENDD;
import static StringConstants.StringConstants.WINDOW_END;


public class TestMeanOperatorJavaMultiSource {

    public boolean isJavaSource = true;
    public int sourceParallelism = 4; // THIS IS FIXED TO FOUR AND THE DATA INPUT IS FOR SORUCE PARALLELISM OF 4
    public RuntimeExecutionMode executionMode = RuntimeExecutionMode.STREAMING;
    public int parallelism = 6;
    final Class[] operators = {
//            MeanBasic.class,
//            MeanHybrid.class,
            MeanAggregateAware.class,
//            MeanHash.class,
//            MeanRoundRobin.class,
//            MeanCAMRoundRobin.class,
//            MeanHashRoundRobin.class

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
        } else if (clazz == MeanHashRoundRobin.class){
            return new MeanHashRoundRobin(javaSourceParameters, env, parallelism, isJavaSource, sourceParallelism);
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
//    @Test
//    public void testSources() throws Exception{
//        String[] sourceFiles = {
//                "dataJavaMultiSourceTestData/zipf_distribution_40_2_2_1.4.csv",
//                "dataJavaMultiSourceTestData/zipf_distribution_100_2_10_1.0E-15.csv",
//                "dataJavaMultiSourceTestData/zipf_distribution_100_2_10_1.4.csv",
//                "dataJavaMultiSourceTestData/zipf_distribution_10000_2_5_1.0E-15.csv",
//                "dataJavaMultiSourceTestData/zipf_distribution_10000_2_5_1.4.csv"
//        };
//        JavaSourceParameters[] javaSourceParameters = {
//                new JavaSourceParameters("zipfdistribution", 40, 2, 2, 1.4),
//                new JavaSourceParameters("zipfdistribution", 100, 10, 2, 1.0E-15),
//                new JavaSourceParameters("zipfdistribution", 100, 10, 2, 1.4),
//                new JavaSourceParameters("zipfdistribution", 10000, 5, 2, 1.0E-15),
//                new JavaSourceParameters("zipfdistribution", 10000, 5, 2, 1.4)
//        };
//
//        for(int i = 0 ; i < sourceFiles.length; i++){
//            testSource(sourceFiles[i], javaSourceParameters[i]);
//        }
//
//
//    }
//    public void testSource(String sourceFile, JavaSourceParameters javaSourceParameters) throws Exception{
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(executionMode);
//        DataStream<EventBasic> source = env.
//                addSource(new ZipfStringSourceRichProcessFunctionEndWindow(javaSourceParameters.windowSize, javaSourceParameters.numWindow, javaSourceParameters.keySpaceSize, javaSourceParameters.skewness, sourceParallelism, parallelism))
//                .setParallelism(sourceParallelism)
//                .name("source");
//        source.addSink(new sinkCollect());
//        env.execute("Testing the sources" );
//        List<EventBasic> collectedEvents = waitForCompletion();
//
//        ArrayList<EventBasic> expectedEvents = TestUtilities.readSourceCSVList(sourceFile);
//        for(EventBasic event : collectedEvents){
//            if(!event.key.equals(WINDOW_END) && !event.key.equals(ENDD)){
////                printDetails(expectedEvents, event);
//                assert(expectedEvents.contains(event));
//                expectedEvents.remove(event);
//            }
//            else{
//                expectedEvents.remove(event);
//            }
//        }
//        TestUtilities.removeENDD(expectedEvents);
//        assert(expectedEvents.isEmpty() || expectedEvents.size() == 1);
//        sinkCollect.values.clear();
//
//    }


    @Test
    public void testNoSkewLong() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(executionMode);

        String javaSourceParameters = "zipfdistribution,10000,5,2,1.0E-15";
        String csvFilePathCorrect = "src/test/java/javaMultiSourceTestCorrectValues/zipf_distribution_10000_2_5_1.0E-15_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, javaSourceParameters);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );



//            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());

            TestUtilities.removeNull(collectedEvents);

            sinkCollect.values.clear();

            ArrayList<EventBasic> expectedEvents = TestUtilities.readCSVList(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                if(!event.key.equals(ENDD) && !event.key.equals(WINDOW_END)){
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
        String csvFilePathCorrect = "src/test/java/javaMultiSourceTestCorrectValues/zipf_distribution_100_2_10_1.0E-15_correct.csv";
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

            TestUtilities.removeNull(collectedEvents);

            ArrayList<EventBasic> expectedEvents =TestUtilities.readCSVList(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){
                if(!event.key.equals(ENDD) && !event.key.equals(WINDOW_END)) {
//                    System.out.println(event);
//                    System.out.println(expectedEvents.contains(event));
//                    System.out.println("Expected events: " + expectedEvents);
                    TestUtilities.printDetails(expectedEvents, event);
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
    public void testHighSkewLong() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(executionMode);

        String csvFilePath = "zipfdistribution,10000,5,2,1.4";
        String csvFilePathCorrect = "src/test/java/javaMultiSourceTestCorrectValues/zipf_distribution_10000_2_5_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );


//            ArrayList<EventBasic> collectedEvents = new ArrayList<>(sinkCollect.values);
            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());


            TestUtilities.removeNull(collectedEvents);
            sinkCollect.values.clear();

            ArrayList<EventBasic> expectedEvents =TestUtilities.readCSVList(csvFilePathCorrect);
//            System.out.println(collectedEvents);
            for(EventBasic event : collectedEvents){
//                System.out.println(event);
                if(!event.key.equals(ENDD) && !event.key.equals(WINDOW_END)){
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

        //distribution, windowSize, numWindow, keySpaceSize, skewness
        String csvFilePath = "zipfdistribution,100,10,2,1.4";
        String csvFilePathCorrect = "src/test/java/javaMultiSourceTestCorrectValues/zipf_distribution_100_2_10_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());
            sinkCollect.values.clear();
            TestUtilities.removeNull(collectedEvents);
            ArrayList<EventBasic> expectedEvents =TestUtilities.readCSVList(csvFilePathCorrect);
            for(EventBasic event : collectedEvents){

                if(!event.key.equals(ENDD) && !event.key.equals(WINDOW_END)){
                    TestUtilities.printDetails(expectedEvents, event);
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


    @Test
    public void testHighSkewSuperShort() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(executionMode);

        //distribution, windowSize, numWindow, keySpaceSize, skewness
        String csvFilePath = "zipfdistribution,20,2,2,1.4";
        String csvFilePathCorrect = "src/test/java/javaMultiSourceTestCorrectValues/zipf_distribution_20_2_2_1.4_correct.csv";
        for(Class operatorClass : operators){
            System.out.println(operatorClass);
            CompleteOperator<EventBasic> operator = giveOperator(operatorClass,env, csvFilePath);
            System.out.println(operator.getClass());
            DataStream<EventBasic> dataStream = operator.execute();

            dataStream.addSink(new sinkCollect());

            env.execute("Testing mean operator" );

            ArrayList<EventBasic> collectedEvents = new ArrayList<>(waitForCompletion());

            TestUtilities.removeNull(collectedEvents);
            ArrayList<EventBasic> expectedEvents =TestUtilities.readCSVList(csvFilePathCorrect);
            System.out.println(expectedEvents.size());
            System.out.println(collectedEvents.size());
            System.out.println("collected events: " +collectedEvents);
            sinkCollect.values.clear();
            for(EventBasic event : collectedEvents){

                if(!event.key.equals(ENDD) && !event.key.equals(WINDOW_END)){
                    TestUtilities.printDetails(expectedEvents, event);
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


}

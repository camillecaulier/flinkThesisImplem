package benchmarks;

import CompleteOperators.AggregateAware.MaxAggregateAware;
import CompleteOperators.AggregateAware.MaxRoundRobin;
import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.AggregateAware.MeanRoundRobin;
import CompleteOperators.Basic.MaxBasic;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.Hybrid.MaxHybrid;
import CompleteOperators.Hybrid.MeanHybrid;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Benchmark {
    public static void main(String[] args) throws Exception {



        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

//        String csvFilePath = "zipf_distribution100000_25.csv";
        String csvFilePath = "zipf_distribution100_5.csv";

//        long startTime = System.nanoTime();
        long startTime = System.currentTimeMillis();
//        MeanHybrid maxHybrid = new MeanHybrid(csvFilePath, env, 5);
//        maxHybrid.execute();

//        MeanAggregateAware meanAggregateAware = new MeanAggregateAware(csvFilePath, env, 10,10);
//        meanAggregateAware.execute();
//        MeanRoundRobin meanRoundRobin = new MeanRoundRobin(csvFilePath, env,10);
//        meanRoundRobin.execute();

//        MeanBasic meanBasic = new MeanBasic(csvFilePath, env, 10);
//        meanBasic.execute();

//        MaxBasic maxBasic = new MaxBasic(csvFilePath, env, 10);
//        maxBasic.execute();

//        MaxHybrid maxHybrid = new MaxHybrid(csvFilePath, env, 10);
//        maxHybrid.execute();

//        MaxAggregateAware maxAggregateAware = new MaxAggregateAware(csvFilePath, env, 10,10);
//        maxAggregateAware.execute();

        MaxRoundRobin maxRoundRobin = new MaxRoundRobin(csvFilePath, env,10);
        maxRoundRobin.execute();

        env.execute("Benchmark");

//        long endTime = System.nanoTime();
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);  // Duration in nanoseconds

        System.out.println("Execution time: " + duration + " nanoseconds");

    }

    public String readArgs(String[] args){
        int parallelism;
        String operator;
        String keyGrouping;
        return "hufa";
    }

    public Object test(String nameClass,int mainParallelism, int hybridParallelism, int choices){
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(5);
        return  new MaxRoundRobin("csvFilePath",StreamExecutionEnvironment.getExecutionEnvironment(),10);
    }
}

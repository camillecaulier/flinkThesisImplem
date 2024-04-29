package benchmarks;

import CompleteOperators.Basic.MaxBasic;
import CompleteOperators.Hybrid.MaxHybrid;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Benchmark {
    public static void main(String[] args) throws Exception {
//        System.out.println("Benchmarking...");
//        long startTime = System.nanoTime();
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);
//
//        String csvFilePath = "zipf_distribution100_5.csv";
//
//        MaxBasic maxBasic = new MaxBasic(csvFilePath, env);
//        maxBasic.execute(5);
//
//        env.execute("Benchmark");
//
//        long endTime = System.nanoTime();
//        long duration = (endTime - startTime);  // Duration in nanoseconds
//
//        System.out.println("Execution time: " + duration + " nanoseconds");




        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        String csvFilePath = "zipf_distribution1000_1.csv";

        long startTime = System.nanoTime();
        MaxHybrid maxHybrid = new MaxHybrid(csvFilePath, env);
        maxHybrid.execute();

        env.execute("Benchmark");

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);  // Duration in nanoseconds

        System.out.println("Execution time: " + duration + " nanoseconds");

    }

    public String readArgs(String[] args){
        int parallelism;
        String operator;
        String keyGrouping;
        return "hufa";
    }
}

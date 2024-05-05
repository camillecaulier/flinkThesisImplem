package benchmarks;

import CompleteOperators.AggregateAware.MaxAggregateAware;
import CompleteOperators.RoundRobin.MaxRoundRobin;
import CompleteOperators.AggregateAware.MeanAggregateAware;
import CompleteOperators.RoundRobin.MeanRoundRobin;
import CompleteOperators.Basic.MaxBasic;
import CompleteOperators.Basic.MeanBasic;
import CompleteOperators.CompleteOperator;
import CompleteOperators.Hybrid.MaxHybrid;
import CompleteOperators.Hybrid.MeanHybrid;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Benchmark {

    public static void main(String[] args) throws Exception {

        int mainParallelism = 2;

        List<BenchmarkParameters> benchmarkParameters = new ArrayList<>(
                Arrays.asList(
                        new BenchmarkParameters("MeanBasic", mainParallelism, 0, 0),
                        new BenchmarkParameters("MeanAggregateAware", mainParallelism, 0, 3)
//                        new BenchmarkParameters("MeanRoundRobin", mainParallelism, 0, 0),
//                        new BenchmarkParameters("MeanHybrid", mainParallelism/2, mainParallelism/2, 0),
//
//
//                        new BenchmarkParameters("MaxBasic", mainParallelism, 0, 0),
//                        new BenchmarkParameters("MaxHybrid", mainParallelism/2, mainParallelism/2, 0),
//                        new BenchmarkParameters("MaxAggregateAware", mainParallelism, 0, 3),
//                        new BenchmarkParameters("MaxRoundRobin", mainParallelism, 0, 0)
                )
        );

        String directory = "data/";
        List<String> csvSources = listFilenamesInDirectory(directory);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //executeOrder66
        for (BenchmarkParameters benchmarkParameter : benchmarkParameters) {
//            String csvSource = "zipf_distribution100000_1_50_0.35.csv";

            for (String csvSource : csvSources) {
//            for(int i = 0 ; i < 2; i++){
                System.out.println("Benchmarking operator: " + benchmarkParameter.operator + " with file: " + csvSource);
                CompleteOperator<EventBasic> operator = createOperatorFromParameters(benchmarkParameter, directory + csvSource,env);
                operator.execute();
                long startTime = System.nanoTime();

                env.execute("\"Benchmarking operator: \" + benchmarkParameter.operator + \" with file: \" + csvSource");
                long endTime = System.nanoTime();
                long duration = (endTime - startTime);
                System.out.println("Execution time: " + duration + " nanoseconds");


            }
        }

    }


    public static CompleteOperator<EventBasic> createOperatorFromParameters(BenchmarkParameters benchmarkParameters, String csvFilePath, StreamExecutionEnvironment env){
        String nameClass = benchmarkParameters.operator;
        int mainParallelism = benchmarkParameters.MainParallelism;
        int hybridParallelism = benchmarkParameters.HybridParallelism;
        int choices = benchmarkParameters.Choices;

        switch (nameClass){
            case "MeanBasic":
                return new MeanBasic(csvFilePath,env,mainParallelism);
            case "MeanAggregateAware":
                return new MeanAggregateAware(csvFilePath,env,mainParallelism,choices);
            case "MeanRoundRobin":
                return new MeanRoundRobin(csvFilePath,env,mainParallelism);
            case "MeanHybrid":
                return new MeanHybrid(csvFilePath,env,mainParallelism, hybridParallelism);


            case "MaxBasic":
                return new MaxBasic(csvFilePath,env,mainParallelism);
            case "MaxHybrid":
                return new MaxHybrid(csvFilePath,env,mainParallelism, hybridParallelism);
            case "MaxAggregateAware":
                return new MaxAggregateAware(csvFilePath,env,mainParallelism,choices);
            case "MaxRoundRobin":
                return new MaxRoundRobin(csvFilePath,env,mainParallelism);
            default: // add other lock classes here
                System.err.println("Invalid class name " + nameClass);
                System.exit(-1);
                return null; // remove compiler warning

        }

    }

    public static List<String> listFilenamesInDirectory(String directoryPath) {
        File directory = new File(directoryPath);
        File[] filesList = directory.listFiles();
        List<String> filenames = new ArrayList<>();

        if (filesList != null) {
            for (File file : filesList) {
                // Add only files to the list (ignore directories)
                if (file.isFile()) {
                    filenames.add(file.getName());
                }
            }
        } else {
            System.out.println("The specified path does not exist or is not a directory.");
        }

        return filenames;
    }

    public void printMetrics(BenchmarkParameters benchmarkParameter, String csvSource, long duration) {
        System.out.println(benchmarkParameter.operator+ "," + duration/1000000+","+benchmarkParameter.MainParallelism+","+benchmarkParameter.HybridParallelism+","+benchmarkParameter.Choices+","+csvSource);
    }

}



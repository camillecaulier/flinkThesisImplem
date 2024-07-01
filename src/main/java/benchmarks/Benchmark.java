package benchmarks;

import CompleteOperators.AggregateAware.MaxAggregateAware;
import CompleteOperators.Cam_roundrobin_choices.MeanCAMRoundRobin;
import CompleteOperators.Hash.MeanHash;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sink.basicSinkFunction;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        printClassLocation(Benchmark.class);

        int mainParallelism = args[0].isEmpty() ? 10 : Integer.parseInt(args[0]);
        int sourceParallelism = args[1].isEmpty() ? 3 : Integer.parseInt(args[1]);
        System.out.println("Main parallelism: " + mainParallelism);
        System.out.println("Source parallelism: " + sourceParallelism);

        String fileName = args[2];
        System.out.println("Directory name: " + fileName);

        boolean isJavaSource = args[3].equals("javaSource");

        List<BenchmarkParameters> benchmarkParameters = new ArrayList<>(
                Arrays.asList(
//                        new BenchmarkParameters("MeanBasic", mainParallelism, 0, 0, sourceParallelism),
                        new BenchmarkParameters("MeanAggregateAware", mainParallelism, 0, 3, sourceParallelism),
//                        new BenchmarkParameters("MeanRoundRobin", mainParallelism, 0, 0, sourceParallelism),
//                        new BenchmarkParameters("MeanHybrid", mainParallelism / 2, mainParallelism / 2, 0, sourceParallelism),
//                        new BenchmarkParameters("MeanCAMRoundRobin", mainParallelism, 0, 0, sourceParallelism),
                        new BenchmarkParameters("MeanHash", mainParallelism, 0, 0, sourceParallelism)

//                        new BenchmarkParameters("MaxBasic", mainParallelism, 0, 0),
//                        new BenchmarkParameters("MaxHybrid", mainParallelism/2, mainParallelism/2, 0),
//                        new BenchmarkParameters("MaxAggregateAware", mainParallelism, 0, 3),
//                        new BenchmarkParameters("MaxRoundRobin", mainParallelism, 0, 0)
                )
        );
        List<String> sources;
        if (isJavaSource) {
            sources = listParamsInFile(fileName);
        } else {
            String directory = System.getProperty("user.dir") + "/" + fileName + "/";
            sources = listFilenamesInDirectory(directory);
        }

        printAllOperators(benchmarkParameters);


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        long totalStart = System.currentTimeMillis();

        //executeOrder66
        for (BenchmarkParameters benchmarkParameter : benchmarkParameters) {

            for (String source : sources) {
                System.out.println("Benchmarking operator: " + benchmarkParameter.operator + " with file: " + source);
                CompleteOperator<EventBasic> operator = createOperatorFromParameters(benchmarkParameter, source, env, isJavaSource);
                DataStream<EventBasic> output = operator.execute();
//                output.addSink(new basicSinkFunction());
                long startTime = System.nanoTime();

                env.execute("Benchmarking operator: " + benchmarkParameter.operator + " with file: " + source);
                long endTime = System.nanoTime();
                long duration = (endTime - startTime);
                printMetrics(benchmarkParameter, source, duration);


            }
        }
        long totalEnd = System.currentTimeMillis();
        long totalDuration = ((totalEnd - totalStart) / 1000) / 60;

        System.out.println("Total execution time: " + totalDuration + " minutes");

    }


    public static CompleteOperator<EventBasic> createOperatorFromParameters(BenchmarkParameters benchmarkParameters, String csvFilePath, StreamExecutionEnvironment env, boolean isJavaSource) {
        String nameClass = benchmarkParameters.operator;
        int mainParallelism = benchmarkParameters.MainParallelism;
        int hybridParallelism = benchmarkParameters.HybridParallelism;
        int choices = benchmarkParameters.Choices;
        int sourceParallelism = benchmarkParameters.sourceParallelism;

        switch (nameClass) {
            case "MeanBasic":
                return new MeanBasic(csvFilePath, env, mainParallelism, isJavaSource, sourceParallelism);
            case "MeanAggregateAware":
                return new MeanAggregateAware(csvFilePath, env, mainParallelism, choices, isJavaSource, sourceParallelism);
            case "MeanRoundRobin":
                return new MeanRoundRobin(csvFilePath, env, mainParallelism, isJavaSource, sourceParallelism);
            case "MeanHybrid":
                return new MeanHybrid(csvFilePath, env, mainParallelism, hybridParallelism, isJavaSource, sourceParallelism);
            case "MeanCAMRoundRobin":
                return new MeanCAMRoundRobin(csvFilePath, env, mainParallelism, choices, isJavaSource, sourceParallelism);
            case "MeanHash":
                return new MeanHash(csvFilePath, env, mainParallelism, isJavaSource, sourceParallelism);


            case "MaxBasic":
                return new MaxBasic(csvFilePath, env, mainParallelism, isJavaSource, sourceParallelism );
            case "MaxHybrid":
                return new MaxHybrid(csvFilePath, env, mainParallelism, hybridParallelism, isJavaSource, sourceParallelism );
            case "MaxAggregateAware":
                return new MaxAggregateAware(csvFilePath, env, mainParallelism, choices, isJavaSource, sourceParallelism   );
            case "MaxRoundRobin":
                return new MaxRoundRobin(csvFilePath, env, mainParallelism, isJavaSource, sourceParallelism);
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
                    filenames.add(directoryPath+file.getName());
                }
            }
        } else {
            System.out.println("The specified path does not exist or is not a directory.");
        }

        return filenames;
    }

    public static void printAllOperators(List<BenchmarkParameters> benchmarkList){
        for (BenchmarkParameters operator : benchmarkList){
            System.out.println(operator.operator);
        }
    }

    public static void printMetrics(BenchmarkParameters benchmarkParameter, String csvSource, long duration) {
        System.out.println("metric:" + benchmarkParameter.operator + "," + duration / 1000000 + "," + benchmarkParameter.MainParallelism + "," + benchmarkParameter.HybridParallelism + "," + benchmarkParameter.Choices + "," + csvSource);
    }

    public static void printClassLocation(Class<?> clazz) {
        java.security.ProtectionDomain pd = clazz.getProtectionDomain();
        java.security.CodeSource cs = pd.getCodeSource();
        if (cs != null) {
            java.net.URL url = cs.getLocation();
            if (url != null) {
                System.out.println(clazz.getSimpleName() + " is loaded from " + url.getPath());
            } else {
                System.out.println("The location of " + clazz.getSimpleName() + " could not be determined.");
            }
        } else {
            System.out.println("No CodeSource available for " + clazz.getSimpleName());
        }
    }


    public static List<String> listParamsInFile(String fileName) throws IOException {
        return Files.lines(Paths.get(fileName))
                .skip(1) // Skips the first line of the file
                .collect(Collectors.toList());
    }
}




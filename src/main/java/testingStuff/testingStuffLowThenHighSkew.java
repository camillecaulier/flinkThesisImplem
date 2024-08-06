package testingStuff;

import CompleteOperators.CompleteOperator;
import benchmarks.BenchmarkParameters;
import eventTypes.EventBasic;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static benchmarks.Benchmark.createOperatorFromParameters;

public class testingStuffLowThenHighSkew {
    public static void main(String[] args) throws Exception {
        int mainParallelism = 6;
        int sourceParallelism = 6;
        int aggregatorParallelism = 1;
        BenchmarkParameters test = new BenchmarkParameters("MeanHash", mainParallelism, 0, 0, sourceParallelism, 0);
        String source ="lowThenHighSkew,100000,300,0,0";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        System.out.println(test.);
        System.out.println("Benchmarking operator: " + test.operator
                +" sourceParallelism: "+ String.valueOf(sourceParallelism)+ " mainParallelism: " + String.valueOf(mainParallelism) +" aggregatorParallelism: " +String.valueOf(aggregatorParallelism)
                + " with file: " + source);
        CompleteOperator<EventBasic> operator = createOperatorFromParameters(test, source, env, true);
        DataStream<EventBasic> output = operator.execute();
//                output.addSink(new basicSinkFunction());
        long startTime = System.nanoTime();

        env.execute("Benchmarking operator: " + test.operator +" sourceParallelism: "+ String.valueOf(sourceParallelism)+ " mainParallelism: " + String.valueOf(mainParallelism) +" aggregatorParallelism: " +String.valueOf(aggregatorParallelism)
                + " with file: " + source);
    }
}

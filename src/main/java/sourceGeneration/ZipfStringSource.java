package sourceGeneration;

import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.concurrent.ThreadLocalRandom;

public class ZipfStringSource extends RichParallelSourceFunction<Tuple2<String, Integer>>{


    private volatile boolean running = true;


    private volatile int size = 100;
    volatile int count = 0;


    @Override
    public void run(SourceFunction.SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {

        int populationSize = 26;
        double exponent = 2.0; // skeness


        ZipfDistribution zipfDistribution = new ZipfDistribution(populationSize, exponent);

        while (count < size) {
            synchronized (sourceContext.getCheckpointLock()) {
                count++;


                int rank = zipfDistribution.sample() - 1;
                char key = (char) ('A' + rank);

                sourceContext.collect(Tuple2.of(String.valueOf(key), count));
                System.out.println("Generated: " + key + "," + count);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
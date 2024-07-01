package sourceGeneration;

import StringConstants.StringConstants;
import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

public class ZipfStringSourceRichProcessFunction extends RichParallelSourceFunction<EventBasic> {

    private volatile boolean running = true;
    private volatile int windowSize;
    private volatile int numWindow;
    private  long seed = 123456L;
    private double skewness;
    private int keySpaceSize;
    private int parallelism;

    public ZipfStringSourceRichProcessFunction(int windowSize, int numWindow, int keySpaceSize, double skewness, int parallelism) {
        this.windowSize = windowSize;
        this.numWindow = numWindow;
        this.skewness = skewness;
        this.keySpaceSize = keySpaceSize;

        this.parallelism = parallelism;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.seed += getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void run(SourceContext<EventBasic> sourceContext) throws Exception {
        int populationSize = (int) Math.pow(26, keySpaceSize);

        RandomGenerator randomGenerator = new Well19937c(seed);
        ZipfDistribution zipfDistribution = new ZipfDistribution(randomGenerator, populationSize, skewness);

        for (int window = 0; window < numWindow; window++) {
            generateWindow(sourceContext, zipfDistribution, window * 1000L + 500);
        }

        for(int j = 0 ; j < 2; j++){
            for (int i = 0; i < 10; i++) {
                Value value = new Value(i, (numWindow) *(j+1) *1000L + 500);
                System.out.println(value);
                sourceContext.collect(new EventBasic(StringConstants.WINDOW_END, value.valueInt, value.timeStamp));
            }
        }


        sourceContext.close();
    }

    private void generateWindow(SourceContext<EventBasic> sourceContext, ZipfDistribution zipfDistribution, long timeStamp) throws InterruptedException {
        List<EventBasic> batch = new ArrayList<>();
        for (int i = 0; i < windowSize/parallelism; i++) {
            String key = convertToLetter(zipfDistribution.sample());
            batch.add(new EventBasic(key, i, timeStamp));
        }

        synchronized (sourceContext.getCheckpointLock()) {
            for (EventBasic event : batch) {
                sourceContext.collect(event);
            }
        }
    }

    public static String convertToLetter(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException("Number must be positive");
        }

        StringBuilder result = new StringBuilder();
        while (number > 0) {
            int remainder = (number - 1) % 26;
            char letter = (char) (remainder + 'A');
            result.insert(0, letter);
            number = (number - 1) / 26;
        }

        return result.toString();
    }

    @Override
    public void cancel() {
        running = false;
    }
}

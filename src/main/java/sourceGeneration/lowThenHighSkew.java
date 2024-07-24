package sourceGeneration;

import CustomWindowing.Windowing;
import StringConstants.StringConstants;
import static StringConstants.StringConstants.WINDOW_END;
import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class lowThenHighSkew extends RichParallelSourceFunction<EventBasic> {

    private volatile boolean running = true;
    private volatile int windowSize;
    private volatile int numWindow;
    private  long seed = 123456L;
    private double skewness;
    private int keySpaceSize;
    private int sourceParallelism;

    private int parallelism;

    private int subtaskIndex;

    Windowing windowing;


    public lowThenHighSkew(int windowSize, int numWindow, int keySpaceSize, double skewness, int sourceParallelism, int parallelism) {
        this.windowSize = windowSize;
        this.numWindow = numWindow;
        this.skewness = skewness;
        this.keySpaceSize = keySpaceSize;

        this.sourceParallelism = sourceParallelism;
        this.parallelism = parallelism;



    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.seed += getRuntimeContext().getIndexOfThisSubtask();
        this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        windowing = new Windowing(sourceParallelism, parallelism, getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void run(SourceContext<EventBasic> sourceContext) throws Exception {
        int populationSize = (int) Math.pow(26, keySpaceSize);

        RandomGenerator randomGenerator = new Well19937c(seed);
        ZipfDistribution zipfDistribution = new ZipfDistribution(randomGenerator, populationSize, skewness);

        for (int window = 0; window < numWindow; window++) {
            generateWindow(sourceContext, zipfDistribution, window * 1000L + 500);
        }


//        for (int i = 0; i < parallelism; i++) {
//            Value value = new Value(subtaskIndex, (numWindow+1) *1000L + 500);
//            sourceContext.collect(new EventBasic(StringConstants.WINDOW_END, subtaskIndex, value.timeStamp));
//        }



        sourceContext.close();
    }

    private void generateWindow(SourceContext<EventBasic> sourceContext, ZipfDistribution zipfDistribution, long timeStamp) throws InterruptedException {

        for (int i = 0; i < windowSize/ sourceParallelism; i++) {
            String key = convertToLetter(zipfDistribution.sample());
            sourceContext.collect(new EventBasic(key, i, timeStamp));

        }

        windowing.outputEndWindow(sourceContext, timeStamp);
//        outputEndWindow(sourceContext, timeStamp);
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

//    public void outputEndWindow(SourceContext<EventBasic> sourceContext, long timestamp) {
//        for(int i = 0; i < parallelism; i++){
//            sourceContext.collect(new EventBasic(WINDOW_END, subtaskIndex, timestamp));
//        }
//    }


    @Override
    public void cancel() {
        running = false;
    }
}


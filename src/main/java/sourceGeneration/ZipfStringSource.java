package sourceGeneration;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class ZipfStringSource implements SourceFunction<EventBasic> {


    private volatile boolean running = true;


    private volatile int windowSize ;
    private volatile int numWindow;
    volatile int count = 0;

    final long seed = 123456L;

    private double skewness;
    private int keySpaceSize;

    public ZipfStringSource(int windowSize, int numWindow,  int keySpaceSize, double skewness){
        this.windowSize = windowSize;
        this.numWindow = numWindow;
        this.skewness = skewness;
        this.keySpaceSize = keySpaceSize;
    }



    @Override
    public void run(SourceFunction.SourceContext<EventBasic> sourceContext) throws Exception {

        int populationSize = (int) Math.pow(26,keySpaceSize);

        RandomGenerator randomGenerator = new Well19937c(seed);
        ZipfDistribution zipfDistribution = new ZipfDistribution(randomGenerator,populationSize, skewness);

        for(int window =0 ; window < numWindow; window++){
            generateWindow(sourceContext, zipfDistribution, window*1000L + 500);
        }

        for (int i = 0; i < 20; i++) {
            Value value = new Value(i,(numWindow) * 1000L + 500);
            System.out.println(value);
            sourceContext.collect(new EventBasic("ENDD", value.valueInt, value.timeStamp));
        }

        sourceContext.close();

    }

    private void generateWindow(SourceFunction.SourceContext<EventBasic> sourceContext, ZipfDistribution zipfDistribution, long timeStamp) throws InterruptedException {
        for (int i = 0; i < windowSize; i++) {

            String key = convertToLetter(zipfDistribution.sample());
//                System.out.println("key: " + key + " i: " + i + " time: " + timeStamp);
            sourceContext.collect(new EventBasic(key,i ,timeStamp));


        }

    }


    public static String convertToLetter(int number) {
        if (number <= 0) {
            throw new IllegalArgumentException("Number must be positive");
        }

        StringBuilder result = new StringBuilder();
        while (number > 0) {
            int remainder = (number - 1) % 26; // Remainder when divided by 26
            char letter = (char) (remainder + 'A'); // Convert remainder to corresponding letter
            result.insert(0, letter); // Prepend the letter to the result
            number = (number - 1) / 26; // Update the number for next iteration
        }

        return result.toString();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
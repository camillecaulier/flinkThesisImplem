package sourceGeneration;

import eventTypes.EventBasic;
import eventTypes.Value;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class BasicDistributionSource implements SourceFunction<EventBasic> {





    boolean running = true;
    private static Random random = new Random();

//    public BasicDistributionSource(int windowSize, int numWindows) {
//    }

    public static char getRandomLetter() {
        int r = random.nextInt(100);  // Generate a random number between 0 and 99
        if (r < 40) {
            return 'a';  // 40% chance to return 'a'
        } else if (r < 70) {
            return 'b';  // 30% chance to return 'b' (from 40 to 69)
        } else if (r < 85) {
            return 'c';  // 15% chance to return 'c' (from 70 to 84)
        } else {
            return 'd';  // 15% chance to return 'd' (from 85 to 99)
        }
    }


    @Override
    public void run(SourceContext<EventBasic> sourceContext) throws Exception {

        while(running){
// Generate a random key
            String key = String.valueOf(getRandomLetter());
            int value = random.nextInt(100);
            long timestamp = System.currentTimeMillis();
            sourceContext.collect(new EventBasic(key, 10, timestamp));


        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
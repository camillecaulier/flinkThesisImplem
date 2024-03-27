package sourceGeneration;

import org.apache.commons.text.RandomStringGenerator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.concurrent.ThreadLocalRandom;

public class RandomStringSource extends RichParallelSourceFunction<Tuple2<String, Integer>>{
//    private final int keySize;

    private volatile boolean running = true;

//    RandomStringSource(ParameterTool pt) {
//
//        this.keySize = pt.getInt("keySize", 1);
//    }
    private volatile int size = 100;
    volatile int count = 0;

    @Override
    public void run(SourceFunction.SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        RandomStringGenerator rng = new RandomStringGenerator.Builder()
                .withinRange('A', 'Z')
                .usingRandom(rnd::nextInt)
                .build();
        while (count < size ) {
            synchronized (sourceContext.getCheckpointLock()) {
                count ++;
                int keySize = 1; // 26^1 26^2 26^3 26^4 26^5
                sourceContext.collect(Tuple2.of(rng.generate(keySize), count));
                System.out.println("Generated: "+ rng.generate(keySize)+ "," + count);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
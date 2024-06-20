package keygrouping;

import java.util.concurrent.atomic.AtomicInteger;

public class EndWindowPropagation {

    int parallelism;
    private final AtomicInteger counter = new AtomicInteger(-1);
    public EndWindowPropagation(int parallelism){
        this.parallelism = parallelism;
    }

    public int endWindow(){
        int count = counter.incrementAndGet();
        return Math.abs(count % parallelism);
    }
}

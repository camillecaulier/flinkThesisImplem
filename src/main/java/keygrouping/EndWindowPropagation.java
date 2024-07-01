package keygrouping;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class EndWindowPropagation implements Serializable {

    int parallelism;
    private final AtomicInteger counter = new AtomicInteger(-1);
    public EndWindowPropagation(int parallelism){
        this.parallelism = parallelism;
    }

    public int endWindowRouting(){
        int count = counter.incrementAndGet();
        return Math.abs(count % parallelism);
    }
}

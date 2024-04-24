package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinThreadSafe implements Partitioner<String>, Serializable {
    private final AtomicInteger index = new AtomicInteger(0);



    @Override
    public int partition(String key, int numPartitions) {
        int current, next;
        do {
            current = index.get();
            next = (current + 1) % numPartitions;
        } while (!index.compareAndSet(current, next));
        return next;
    }
}
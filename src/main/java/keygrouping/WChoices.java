package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class WChoices implements Partitioner<String>, Serializable {
    //TODO: implement a partitioner that uses the W-Choices algorithm


    private final AtomicInteger index;

    public WChoices() {
        this.index = new AtomicInteger(0);
    }


    @Override
    public int partition(String key, int numPartitions) {
        int currentIndex = index.getAndIncrement();
//        System.out.println("index: " + Math.abs(this.index % numPartitions) + " numPartitions: " + numPartitions + " key: " + key);
        currentIndex = Math.max(currentIndex, 0);
        return  Math.abs(currentIndex % numPartitions) ;
    }
}
package org.example;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Random;

public class ShufflePartitioner implements Partitioner<String> {

    int index;

    ShufflePartitioner() {
        index = 0;
    }

    @Override
    public int partition(String key, int numPartitions) {
        index++;

        return  Math.abs(this.index % numPartitions) ;
    }
}
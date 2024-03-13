package org.example;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Random;

public class RandomPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {

        return  Math.abs(new Random().nextInt() % numPartitions) ;
    }
}
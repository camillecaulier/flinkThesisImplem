package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

public class RoundRobin implements Partitioner<String> {

    int index;

    public RoundRobin() {
        index = 0;
    }

    @Override
    public int partition(String key, int numPartitions) {
        index++;
//        System.out.println("index: " + Math.abs(this.index % numPartitions) + " numPartitions: " + numPartitions + " key: " + key);

        return  Math.abs(this.index % numPartitions) ;
    }
}
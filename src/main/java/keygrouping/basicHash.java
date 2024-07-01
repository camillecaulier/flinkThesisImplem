package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;

import java.util.Objects;

public class basicHash extends keyGroupingBasic{

    public basicHash(int parallelism) {
        super(parallelism);
    }


    @Override
    public int customPartition(String key, int numPartitions) {

        int hash = key.hashCode();
        return Math.abs(hash % numPartitions);
    }


}

package keygrouping;

import org.apache.commons.math3.util.FastMath;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import java.util.Objects;

public class basicHash extends keyGroupingBasic{

    private HashFunction h1 = Hashing.murmur3_128(13);
    public basicHash(int parallelism) {
        super(parallelism);
    }

    @Override
    public int customPartition(String key, int numPartitions) {

        return (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
    }

}

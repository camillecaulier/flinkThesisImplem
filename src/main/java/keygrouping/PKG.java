package keygrouping;

import org.apache.commons.math3.util.FastMath;
import org.apache.flink.shaded.guava31.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

public class PKG extends keyGroupingBasic{
    /*
    * method based on the PKG algorithm see  https://github.com/anisnasir/SLBStorm
    * */

    long[] targetTaskStats;
    public PKG(int parallelism) {
        super(parallelism);
        this.targetTaskStats = new long[parallelism];
    }

    private HashFunction h1 = Hashing.murmur3_128(13);
    private HashFunction h2 = Hashing.murmur3_128(17);
    @Override
    public int customPartition(String key, int numPartitions) {
        int firstChoice = (int) (FastMath.abs(h1.hashBytes(key.getBytes()).asLong()) % numPartitions);
        int secondChoice = (int) (FastMath.abs(h2.hashBytes(key.getBytes()).asLong()) % numPartitions);
        int selected = targetTaskStats[firstChoice]>targetTaskStats[secondChoice]?secondChoice:firstChoice;
        targetTaskStats[selected]++;
        return selected;
    }
}

package keygrouping;

import org.apache.flink.api.common.functions.Partitioner;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import StringConstants.StringConstants;
import redis.clients.jedis.JedisPool;

public class cam_n_external implements Partitioner<String> {
    public int n;
    int parallelism;

    transient Jedis jedis; // Transient as it's not serializable
//    private static JedisPool pool = null;

    public EndWindowPropagation endWindowPropagation;
    public cam_n_external(int n_choices, int numPartitions) {
        this.endWindowPropagation = new EndWindowPropagation(parallelism);
        this.parallelism = numPartitions;
        this.n = n_choices;
        try {
            this.jedis = new Jedis("localhost", 6379);
        } catch (Exception e) {
            System.err.println("Failed to initialize Jedis: " + e.getMessage());
            System.out.println("ERRRRORRRR");
            this.jedis = null;
        }
    }

    @Override
    public int partition(String key, int numPartitions){
        // Special handling, assuming 'endWindowPropagation' is also adapted to work with Redis
        if(key.equals(StringConstants.WINDOW_END)){

            return endWindowPropagation.endWindowRouting();
        }
        return customPartition(key, numPartitions);
    }

    public int customPartition(String key, int numPartitions) {
        jedis = new Jedis("localhost", 6379);
        int[] hashes = generateHashes(key);

        int minCount = Integer.MAX_VALUE;
        int choice = 0;
        if (jedis == null) {
            System.err.println("Jedis not initialized");
            return -1;
        }
        jedis.hset("cadinality","a", "0");
//        jedis.hincrBy("cadinality","a", 1);
        for (int i = 0; i < n; i++) {
            int partition = hashes[i];
            String partitionKey = "partition:" + partition;
            // Ensure the set exists
            jedis.sadd(partitionKey, key); // Add key to set

            // Increment and get tuple count
            long tupleCount = jedis.hincrBy("tupleCount", partitionKey, 1);
            if (tupleCount < minCount) {
                minCount = (int) tupleCount;
                choice = partition;
            }

            if (jedis.sismember(partitionKey, key)) {
                // Debug log
                System.out.println("Choice: " + partition + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + tupleCount + " Cardinality: " + jedis.scard(partitionKey));
                return partition;
            }
        }

        // New choice made based on minimum count
        jedis.sadd("partition:" + choice, key); // Ensure key is added
        long finalCount = jedis.hincrBy("tupleCount", "partition:" + choice, 1);

        // Debug log
        System.out.println("Choice: " + choice + " Key: " + key + " Partition: " + numPartitions + " TupleCount: " + finalCount + " Cardinality: " + jedis.scard("partition:" + choice));

        return choice;
    }

    public int[] generateHashes(String input) {
        int[] hashes = new int[n];
        int baseHash = input.hashCode();

        for (int i = 0; i < n; i++) {
            hashes[i] = Math.abs((baseHash + i * 31) % this.parallelism);
        }

        return hashes;
    }

    // Ensure to handle connection close appropriately, possibly in a method called at the end of usage
    public void close() {
        if (jedis != null) jedis.close();
    }
}
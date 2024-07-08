package keygrouping;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import eventTypes.EventBasic;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class cam_n_richProcessFunction extends ProcessFunction<EventBasic, EventBasic> {
//    private transient HazelcastInstance hazelcastInstance;
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        Config config = new Config();
//        config.setInstanceName("flink-hazelcast");
//        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
//    }
//
//    @Override
//    public void processElement(EventBasic value, Context ctx, Collector<EventBasic> out) throws Exception {
//        out.collect(value);
//        hazelcastInstance.getMap("CardinalityMap").put(value.key, value.value);
//    }

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Initialize the Redis connection
        this.jedis = new Jedis("localhost");  // Adjust IP/hostname as necessary
        System.out.println("Redis connection established");
    }

    @Override
    public void processElement(EventBasic value, Context ctx, Collector<EventBasic> out) throws Exception {
        out.collect(value);

        // Use Redis to manage the cardinality map
        // Increment the value in Redis and fetch the new value
        jedis.hincrBy("CardinalityMap", value.key, 1);

        // Optionally fetch the updated value to use in your processing logic
        String updatedCount = jedis.hget("CardinalityMap", value.key);
        System.out.println("Updated count for key " + value.key + ": " + updatedCount);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

}

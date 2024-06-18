package keygrouping;

import eventTypes.EventBasic;
import org.apache.flink.api.common.functions.Partitioner;

public class basicNoBackpressure implements Partitioner<EventBasic> {

    @Override
    public int partition(EventBasic key, int numPartitions) {
        //a and c go to 0
        if(key.key == "a" || key.key == "c"){
            return 0;
        }
        else if( key.key == "b" || key.key == "d"){
            return 1;

        }
        return 0;
    }
}

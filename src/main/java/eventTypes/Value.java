package eventTypes;

import java.time.LocalDateTime;

public class Value {

    public Integer valueInt;
    public Long timeStamp;


    public Value(int valueInt, long valueTimeStamp) {
        this.valueInt = valueInt;
        this.timeStamp = valueTimeStamp;
    }

    @Override
    public String toString() {
        return "Value{" +
                "valueInt=" + valueInt +
                ", timeStamp=" + timeStamp +
                '}';
    }
}


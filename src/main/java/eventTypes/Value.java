package eventTypes;

import java.time.LocalDateTime;

public class Value {

    public Integer valueInt;
    public Long timeStamp;

    public Integer valueTmp;


    public Value(int valueInt, long valueTimeStamp) {
        this.valueInt = valueInt;
        this.timeStamp = valueTimeStamp;
    }

    public Value(int valueInt, long valueTimeStamp, int valueTmp) {
        this.valueInt = valueInt;
        this.timeStamp = valueTimeStamp;
        this.valueTmp = valueTmp;
    }

    @Override
    public String toString() {
        return "Value{" +
                "valueInt=" + valueInt +
                ", timeStamp=" + timeStamp +
                '}';
    }
}


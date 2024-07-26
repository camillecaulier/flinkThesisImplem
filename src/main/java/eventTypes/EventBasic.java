package eventTypes;


import java.util.Objects;

public class EventBasic {
    /**
     * This class is the basic class for all the events that will be used in the project. key string and value
     */

    public String key;
    public Value value;

    public EventBasic(String key, Value value){
        this.key = key;
        this.value = value;
    }

    public EventBasic(String key, int valueInt, long valueTimeStamp){
        this.key = key;
        this.value = new Value(valueInt, valueTimeStamp);
    }

    public EventBasic(String key, int valueInt, long valueTimeStamp, int valueTmp){
        this.key = key;
        this.value = new Value(valueInt, valueTimeStamp, valueTmp);
    }
    @Override
    public String toString() {
        return "EventBasic{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        EventBasic eventBasic = (EventBasic) obj;
        return key.equals(eventBasic.key) && value.valueInt.equals(eventBasic.value.valueInt) && value.timeStamp.equals(eventBasic.value.timeStamp) ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value.timeStamp, value.valueInt);
    }

    public static boolean isPopularKey(String key){
        return key.equals("A") || key.equals("B") || key.equals("C") || key.equals("D") || key.equals("E");
    }

    public boolean isPopularKey(){
        return  this.key.equals("A") ||  this.key.equals("B") || this.key.equals("C") ||  this.key.equals("D") ||  this.key.equals("E");
    }

}

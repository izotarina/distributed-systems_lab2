package lab2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportGroupingComparator extends WritableComparator {
    public AirportGroupingComparator(){
        super(KeyWritableComparable.class);
    }

    @Override
    public int compare(WritableComparable o, WritableComparable o2){
        KeyWritableComparable m = (KeyWritableComparable)o;
        KeyWritableComparable m2 = (KeyWritableComparable)o2;
        return m.getAirportID() - m2.getAirportID();
    }
}

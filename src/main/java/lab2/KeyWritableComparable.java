package lab2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWritableComparable implements WritableComparable<KeyWritableComparable> {
    private int airportID;
    private int isFlightData;

    public KeyWritableComparable() {}

    @Override
    public void readFields(DataInput in) throws IOException {
        airportID.readFields(in);
        isFlightData.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        airportID.write(out);
        isFlightData.write(out);
    }

    @Override
    public int compareTo(KeyWritableComparable o) {
        if (airportID != o.airportID) {
            return airportID - o.airportID;
        }
        return isFlightData - o.isFlightData;
    }

    @Override
    public int hashCode() {
        return airportID.hashCode();
    }
}

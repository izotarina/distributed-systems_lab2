package lab2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWritableComparable implements WritableComparable<KeyWritableComparable> {
    private int airportID;
    private int isFlightData;

    public KeyWritableComparable() {}

    public KeyWritableComparable(int airportID, int isFlightData) {
        this.airportID = airportID;
        this.isFlightData = isFlightData;
    }

    public int getAirportID() {
        return airportID;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airportID = in.readInt();
        isFlightData = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airportID);
        out.writeInt(isFlightData);
    }

    @Override
    public int compareTo(KeyWritableComparable o) {
        if (airportID != o.airportID) {
            return airportID - o.airportID;
        }
        return isFlightData - o.isFlightData;
    }
}

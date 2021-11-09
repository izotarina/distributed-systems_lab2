package lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyWritableComparable {
    private int airportID;
    private boolean isFlightData;

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
        if (ipaddress.compareTo(o.ipaddress)==0)
        {
            return (timestamp.compareTo(o.timestamp));
        }
        else return (ipaddress.compareTo(o.ipaddress));
    }

    @Override
    public boolean equals(Object o)
    {Rules for creating custom Hadoop Writable
        if (o instanceof KeyWritableComparable)
        {
            KeyWritableComparable other = (KeyWritableComparable) o;
            return ipaddress.equals(other.ipaddress) && timestamp.equals(other.timestamp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return airportID.hashCode();
    }
}

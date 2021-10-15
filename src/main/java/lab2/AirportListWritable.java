package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportListWritable implements Writable {
    // Some data
    private int airportId;
    private  timestamp;

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeLong(timestamp);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        timestamp = in.readLong();
    }

    public static AirportListWritable read(DataInput in) throws IOException {
        AirportListWritable w = new AirportListWritable();
        w.readFields(in);
        return w;
    }
}

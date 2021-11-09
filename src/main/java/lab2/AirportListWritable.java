package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportListWritable implements Writable {
    private int airportId;
    private String airportName;

    public AirportListWritable() {
        
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeChars(airportName);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        airportName = in.readLine();
    }

    public static AirportListWritable read(DataInput in) throws IOException {
        AirportListWritable w = new AirportListWritable();
        w.readFields(in);
        return w;
    }
}

package lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportFlightsWritable {
    private int airportId;
    private String airportName;

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeChars(airportName);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        airportName = in.readLine();
    }

    public static AirportFlightsWritable read(DataInput in) throws IOException {
        AirportFlightsWritable w = new AirportFlightsWritable();
        w.readFields(in);
        return w;
    }
}

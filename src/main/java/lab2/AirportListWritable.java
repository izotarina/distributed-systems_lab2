package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportListWritable implements Writable {
    private int airportId;
    private String airportName;

    public AirportListWritable(int airportId, String airportName) {
        this.airportId = airportId;
        this.airportName = airportName;
    }

    public String getAirportName() {
        return airportName;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeChars(airportName);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        airportName = in.readLine();
    }
}

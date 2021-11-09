package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportFlightsWritable implements Writable {
    private int airportId;
    private boolean isCanceled;
    private int delayTime;

    public AirportFlightsWritable(int airportId, boolean isCanceled, int delayTime) {
        this.airportId = airportId;
        this.isCanceled = isCanceled;
        this.delayTime = delayTime;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeBoolean(isCanceled);
        out.writeInt(delayTime);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        isCanceled = in.readBoolean();
        delayTime = in.readInt();
    }
}

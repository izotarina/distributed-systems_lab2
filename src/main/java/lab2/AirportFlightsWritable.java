package lab2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportFlightsWritable {
    private int airportId;
    private boolean isCanceled;
    private int delayTime;

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

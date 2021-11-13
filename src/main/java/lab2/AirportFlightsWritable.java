package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportFlightsWritable implements Writable {
    private int airportId;
    private int isCanceled;
    private float delayTime;

    public AirportFlightsWritable(int airportId, int isCanceled, float delayTime) {
        this.airportId = airportId;
        this.isCanceled = isCanceled;
        this.delayTime = delayTime;
    }

    public float getDelayTime() {
        return delayTime;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeInt(isCanceled);
        out.writeFloat(delayTime);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        isCanceled = in.readInt();
        delayTime = in.readFloat();
    }
}

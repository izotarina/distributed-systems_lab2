package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportFlightsWritable implements Writable {
    private int airportId;
    private float isCanceled;
    private float delayTime;

    public AirportFlightsWritable(int airportId, float isCanceled, float delayTime) {
        this.airportId = airportId;
        this.isCanceled = isCanceled;
        this.delayTime = delayTime;
    }

    public float getDelayTime() {
        return delayTime;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(airportId);
        out.writeFloat(isCanceled);
        out.writeFloat(delayTime);
    }

    public void readFields(DataInput in) throws IOException {
        airportId = in.readInt();
        isCanceled = in.readFloat();
        delayTime = in.readFloat();
    }
}

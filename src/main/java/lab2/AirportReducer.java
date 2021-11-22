package lab2;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportReducer extends Reducer<KeyWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(KeyWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String airportName = "";
        int counter = 0;
        float sumDelay = 0;
        float minDelay = Integer.MAX_VALUE;
        float maxDelay = 0;

        while (iterator.hasNext()) {
            String value = iterator.next().toString();
            if (!NumberUtils.isParsable(value)) {
                airportName = value;
                continue;
            }

            float newValue = Float.parseFloat(value);
            counter++;
            sumDelay += newValue;
            minDelay = Math.min(newValue, minDelay);
            maxDelay = Math.max(newValue, maxDelay);
        }

        if (counter > 0) {
            float average = sumDelay / counter;
            context.write(new Text(airportName), new Text(String.format("min: %f, avg: %f, max: %f", minDelay, average, maxDelay)));
        }
    }
}
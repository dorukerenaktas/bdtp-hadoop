package reducer;

import model.OHLCTuple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VolumeAverageReducer  extends Reducer<Text, OHLCTuple, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<OHLCTuple> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

        double sum = 0;
        double count = 0;

        for (OHLCTuple value: values) {
            sum += value.getVolume();
            count++;
        }

        context.write(key, new DoubleWritable(sum / count));
    }
}

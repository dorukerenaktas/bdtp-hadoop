package reducer;

import model.OHLCTuple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxPriceDifferenceReducer extends Reducer<Text, OHLCTuple, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<OHLCTuple> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

        double max = 0;

        for (OHLCTuple value: values) {
            if (value.getHigh() - value.getLow() > max) {
                max = value.getHigh() - value.getLow();
            }
        }

        context.write(key, new DoubleWritable(max));
    }
}

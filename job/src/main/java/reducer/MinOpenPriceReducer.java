package reducer;

import model.OHLCTuple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinOpenPriceReducer extends Reducer<Text, OHLCTuple, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<OHLCTuple> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

        double min = Double.MAX_VALUE;

        for (OHLCTuple value: values) {
            if (value.getOpen() < min) {
                min = value.getOpen();
            }
        }

        context.write(key, new DoubleWritable(min));
    }
}

package reducer;

import model.OHLCTuple;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ClosePriceStdReducer extends Reducer<Text, OHLCTuple, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<OHLCTuple> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);

        double sum = 0;
        double count = 0;

        for (OHLCTuple value: values) {
            sum += value.getClose();
            count++;
        }

        double mean = sum / count;
        double sumOfSquares = 0;

        for (OHLCTuple value: values) {
            sumOfSquares += (value.getClose() - mean) * (value.getClose() - mean);
        }

        context.write(key, new DoubleWritable(Math.sqrt(sumOfSquares / (count - 1))));
    }
}

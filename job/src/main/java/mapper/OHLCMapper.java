package mapper;

import model.OHLCTuple;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OHLCMapper extends Mapper<LongWritable, Text, Text, OHLCTuple> {

    private final Text name = new Text();
    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();

        if (key.get() == 0 && value.toString().contains("Date")) {
            return;
        }

        String[] field = data.split(",", -1);
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (field.length == 7) {
            this.name.set(fileName.split("\\.")[0]);

            if (field[0].length() > 0) {
                date = field[0];
            }

            if (field[1].length() > 0) {
                this.open = Double.parseDouble(field[1]);
            }

            if (field[2].length() > 0) {
                this.high = Double.parseDouble(field[2]);
            }

            if (field[3].length() > 0) {
                this.low = Double.parseDouble(field[3]);
            }

            if (field[4].length() > 0) {
                this.close = Double.parseDouble(field[4]);
            }

            if (field[5].length() > 0) {
                this.volume = Double.parseDouble(field[5]);
            }

            context.write(this.name, new OHLCTuple(date, open, high, low, close, volume));
        }
    }
}

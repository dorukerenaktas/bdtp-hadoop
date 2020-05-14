import mapper.OHLCMapper;
import model.OHLCTuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.MaxPriceDifferenceReducer;

public class MaxPriceDifferenceMapReduce {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Max Price Difference Map-Reduce");
        job.setJarByClass(MaxPriceDifferenceMapReduce.class);

        job.setMapperClass(OHLCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OHLCTuple.class);

        job.setReducerClass(MaxPriceDifferenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
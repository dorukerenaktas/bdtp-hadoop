package command;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Test {

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000/");
            FileSystem fs = FileSystem.get(conf);
            fs.copyFromLocalFile(new Path("../data/stocks/a.us.txt"), new Path("/inputs/a.us.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

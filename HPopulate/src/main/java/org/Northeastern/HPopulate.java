package  org.Northeastern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class HPopulate {

    public static class PopulateMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            Put put = new Put(Bytes.toBytes(values[0]));

            for (int i = 1; i < values.length; i++) {
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("field" + i), Bytes.toBytes(values[i]));
            }
            context.write(new ImmutableBytesWritable(Bytes.toBytes(values[0])), put);
//      System.out.println("done");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());
        Job job = Job.getInstance(conf, "H-POPULATE");
        job.setJarByClass(HPopulate.class);
        job.setMapperClass(PopulateMapper.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("s3://buckerson/FlightData/bigflight.csv"));
        TableMapReduceUtil.initTableReducerJob(
                "table1", // table
                null, // reducer class
                job);
        job.setOutputFormatClass(NullOutputFormat.class); // because we aren't emitting any output from a reducer

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
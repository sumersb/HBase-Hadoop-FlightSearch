package org.northeastern;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVParser;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.kerby.config.Conf;


public class Main {


    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]); // Take input path for data
        Path outputPath = new Path(args[1]); // Take output path for data

        // Set up job
        Configuration conf = HBaseConfiguration.create();
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hbaseSite).toURI().toURL());
        runMapRed(outputPath, conf);
    }


    public static void runMapRed(Path outputPath, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "HCompute");
        job.setJarByClass(Main.class);

        // Input from HBase table
        job.setInputFormatClass(TableInputFormat.class);
        job.getConfiguration().set(TableInputFormat.INPUT_TABLE, "FlightRecords");

        // Mapper and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(FlightReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Output path (modify as per your requirement)
        FileOutputFormat.setOutputPath(job, outputPath);

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<ImmutableBytesWritable, Result, Text, Text> {

        private static final int TARGET_YEAR = 2008;
        private static final int YEAR_INDEX = 0;
        private static final int MONTH_INDEX = 2;
        private static final int CARRIER_INDEX = 6;
        private static final int ARR_DELAY_MINUTES_INDEX = 37;
        private static final int CANCELLED_INDEX = 41;
        private static final int DIVERTED_INDEX = 43;

        private final CSVParser csvParser = new CSVParser();

        public void map(ImmutableBytesWritable key, Result value, Context context
        ) throws IOException, InterruptedException {
            String line = new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("data")));
            String[] records = csvParser.parseLine(line);

            if (isValidDate(records) && isFlightSuccessful(records)) {
                context.write(getOutputKey(records), getOutputValue(records) );
            }
        }


        private static Text getOutputKey(String[] records) {
            return new Text(records[CARRIER_INDEX]);
        }

        private static Text getOutputValue(String[] records) {
            return new Text(records[MONTH_INDEX] + "," + records[ARR_DELAY_MINUTES_INDEX]);
        }

        /**
         *
         * @param records - the csv in an array of Strings
         * @return boolean - Whether the date is inside the valid range June 2007 <= FlightDate <= May 2008
         */
        private static boolean isValidDate(String[] records) {
            return Integer.parseInt(records[YEAR_INDEX]) == TARGET_YEAR;
        }

        /**
         *
         * @param records - the csv in an array of Strings
         * @return boolean - Whether the flight was not diverted or cancelled
         */
        private static boolean isFlightSuccessful(String[] records) {
            return (((int) Float.parseFloat(records[DIVERTED_INDEX])) != 1 && ((int)Float.parseFloat((records[CANCELLED_INDEX])) != 1));
        }

    }

    public static class FlightReducer extends Reducer<Text, Text, Text, Text> {

        CSVParser csvParser = new CSVParser(); // Used to parse text value
        int flightCount = 0; // Used to keep track of total flight combinations
        float flightDelayTotal = 0; // Used to keep track of total delay


        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Float[] arrDelayMonths = new Float[12];
            Arrays.fill(arrDelayMonths,0f);
            Integer[] monthCounter = new Integer[12];
            Arrays.fill(monthCounter,0);
            for (Text v : values) {
                String[] monthDelay = csvParser.parseLine(v.toString());
                Integer month = Integer.parseInt(monthDelay[0]);
                Float arrDelay = Float.parseFloat(monthDelay[1]);
                arrDelayMonths[month-1] += arrDelay;
                monthCounter[month-1] ++;
            }
            Integer[] averageMonthlyDelay = new Integer[12];
            for (int i = 0; i < 12; i ++) {
                averageMonthlyDelay[i] = Math.round((arrDelayMonths[i]/monthCounter[i]) + 0.5f);
            }

            context.write(getOutputKey(key), getOutputValue(averageMonthlyDelay));
        }

        public static Text getOutputKey(Text key) {
            return new Text("AIR-" + key.toString());
        }

        public static Text getOutputValue(Integer[] delay) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0 ; i < 12; i++) {
                stringBuilder.append(", (").append(i+1).append(",").append(delay[i]).append(")");
            }
            return new Text(stringBuilder.toString());
        }
    }

}
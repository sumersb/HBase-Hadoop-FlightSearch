package org.northeastern;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVParser;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.kerby.config.Conf;


public class Main {

    public static class CarrierKey implements WritableComparable<CarrierKey> {
        private String carrier;

        public CarrierKey(String carrier) {
            this.carrier = carrier;
        }

        public CarrierKey(){}

        @Override
        public int compareTo(CarrierKey o) {
            if (this.carrier == null || o.getCarrier() == null) {
                return 0;
            }
            return this.carrier.compareTo(o.carrier);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CarrierKey that = (CarrierKey) o;
            return Objects.equals(carrier, that.carrier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(carrier);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(carrier);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            carrier = dataInput.readUTF();
        }

        public String getCarrier() {
            return carrier;
        }

        public void setCarrier(String carrier) {
            this.carrier = carrier;
        }

    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, CarrierKey, Text>{

        private static final int TARGET_YEAR = 2008;
        private static final int YEAR_INDEX = 0;
        private static final int MONTH_INDEX = 2;
        private static final int CARRIER_INDEX = 6;
        private static final int ARR_DELAY_MINUTES_INDEX = 37;
        private static final int CANCELLED_INDEX = 41;
        private static final int DIVERTED_INDEX = 43;


        private final CSVParser csvParser = new CSVParser();

        public void map(LongWritable key, Text text, Context context
        ) throws IOException, InterruptedException {

            String line = text.toString();
            String[] records = csvParser.parseLine(line);

            if (isValidDate(records) && isFlightSuccessful(records)) {
                context.write(getOutputKey(records), getOutputValue(records) );
            }
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

        /**
         *
         * @param records - the csv in an array of Strings
         * @return string - returns the combined string of FlightDate + middleSpot so all the flights on the same day with the same location have same key
         */
        private static CarrierKey getOutputKey(String[] records) {
            return new CarrierKey(records[CARRIER_INDEX]);
        }

        private static Text getOutputValue(String[] records) {
            return new Text(records[MONTH_INDEX] + "," + records[ARR_DELAY_MINUTES_INDEX]);
        }
    }



    public static class FlightReducer extends Reducer<CarrierKey, Text, Text, Text> {

        CSVParser csvParser = new CSVParser(); // Used to parse text value
        int flightCount = 0; // Used to keep track of total flight combinations
        float flightDelayTotal = 0; // Used to keep track of total delay


        protected void reduce(CarrierKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

        public static Text getOutputKey(CarrierKey key) {
            return new Text("AIR-" + key.getCarrier());
        }

        public static Text getOutputValue(Integer[] delay) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0 ; i < 12; i++) {
                stringBuilder.append(", (").append(i+1).append(",").append(delay[i]).append(")");
            }
            return new Text(stringBuilder.toString());
        }



    }



    public static void main(String[] args) throws Exception {
        Path inputPath = new Path(args[0]); // Take input path for data
        Path outputPath = new Path(args[1]); // Take output path for data

        // Set up job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FlightDelayTracker");
        job.setJarByClass(Main.class);
        job.setMapperClass(Main.TokenizerMapper.class);
        job.setReducerClass(Main.FlightReducer.class);
        job.setOutputKeyClass(CarrierKey.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean jobCompleted = job.waitForCompletion(true);
    }
}
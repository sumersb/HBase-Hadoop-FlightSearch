package org.northeastern;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVParser;



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

        // Keep Track of indexes of all relevant variables
        private static final Integer YEAR_INDEX = 0;
        private static final Integer MONTH_INDEX = 2;
        private static final Integer CARRIER_INDEX = 6;
        private static final Integer ARR_DELAY_MINUTES_INDEX = 37;
        private static final Integer CANCELLED_INDEX = 41;
        private static final Integer DIVERTED_INDEX = 43;


        private CSVParser csvParser = new CSVParser();
        private static final int YEAR = 2007;



        public void map(LongWritable key, Text text, Context context
        ) throws IOException, InterruptedException {

            String line = text.toString();
            String[] records = csvParser.parseLine(line);
            System.out.println(records.length);



            if (isValidDate(records) && isFlightSuccessful(records)) {

                CarrierKey outputKey = getOutputKey(records);
//                System.out.println(outputKey.getCarrier());
                String outputValue = getOutputValue(records);
                context.write(outputKey,new Text(outputValue));
            }
        }
        /**
         *
         * @param records - the csv in an array of Strings
         * @return boolean - Whether the date is inside the valid range June 2007 <= FlightDate <= May 2008
         */
        private static boolean isValidDate(String[] records) {
            Integer departDate = Integer.parseInt(records[YEAR_INDEX]);
            return departDate.equals(YEAR);
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

        private static String getOutputValue(String[] records) {
            return records[MONTH_INDEX]+","+records[ARR_DELAY_MINUTES_INDEX];
        }
    }



    public static class FlightReducer extends Reducer<CarrierKey, Text, Text, Text> {

        CSVParser csvParser = new CSVParser(); // Used to parse text value
        int flightCount = 0; // Used to keep track of total flight combinations
        float flightDelayTotal = 0; // Used to keep track of total delay

        /**
         *
         * @param key - Text of flightDate+MiddleLocation
         * @param values - Itreable Texts of flight records
         * @param context - Writes output as key of total delay, value as total flight count
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(CarrierKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.getCarrier());
            // Create array to seperate 2 flight legs
            Float[] arrDelay = new Float[12];
            Integer[] flightCount = new Integer[12];
            Float[] avgDelay = new Float[12];
            Arrays.fill(arrDelay,0.0f);
            Arrays.fill(flightCount,0);

            for (Text v : values) {
                String[] monthDelay = csvParser.parseLine((v.toString()));
                int month = Integer.parseInt(monthDelay[0]);
                float arrDelayMinutes = Float.parseFloat(monthDelay[1]);
                arrDelay[month-1] += arrDelayMinutes;
                flightCount[month-1] += 1;
            }

            for (int i = 0; i<12; i++) {
                avgDelay[i] = flightCount[i] > 0 ? arrDelay[i]/flightCount[i] : 0;
            }
            System.out.println(key.getCarrier());
            String outputKey = "AIR-"+key.getCarrier();
            String outputValue = getOutputValue(avgDelay);
            context.write(new Text(outputKey), new Text(outputValue));
        }

        public String getOutputValue(Float[] arrDelay) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < 12; i++) {
                stringBuilder.append("(").append(i+1).append(", ").append(arrDelay[i]).append("), ");
            }
            return stringBuilder.toString();
        }

    }

    public static void main(String[] args) throws Exception {
        Path inputPath = new Path("/Users/sumer/Downloads/data.csv"); // Take input path for data
        Path outputPath = new Path("/Users/sumer/Downloads/applesauce"); // Take output path for data

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
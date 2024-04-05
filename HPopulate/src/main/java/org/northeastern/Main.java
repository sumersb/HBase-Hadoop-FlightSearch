package org.northeastern;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
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
        createTable(conf);
        runMapRed(inputPath, outputPath, conf);
    }


    public static void runMapRed(Path inputPath, Path outputPath, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "FlightCSVToHBase");
        job.setJarByClass(Main.class);
        job.setMapperClass(Main.TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean jobCompleted = job.waitForCompletion(true);
    }
    public static void createTable(Configuration conf) {
        try (
                Connection connection = ConnectionFactory.createConnection(conf);
                Admin admin = connection.getAdmin();
        ) {
            TableName tableName = TableName.valueOf("FlightTable");
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder tsb = TableDescriptorBuilder.newBuilder(tableName);
                tsb.setColumnFamily(ColumnFamilyDescriptorBuilder.of("info"));
                TableDescriptor td = tsb.build();
                admin.createTable(td);
                System.out.println("Here");
            } else {
                System.out.println("here2");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Connection connection;
        private Table table;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("FlightTable"));
        }
        private static final int YEAR_INDEX = 0;
        private static final int CARRIER_INDEX = 6;

        private final CSVParser csvParser = new CSVParser();

        public void map(LongWritable key, Text text, Context context
        ) throws IOException, InterruptedException {
            String line = text.toString();
            String[] records = csvParser.parseLine(line);
            String rowKey = UUID.randomUUID().toString();
            Put put = new Put(rowKey.getBytes());
            put.addColumn("info".getBytes(), "data".getBytes(), line.getBytes());
            table.put(put);
        }




        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }


}
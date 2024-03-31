package org.Northeastern;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.opencsv.CSVParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static class PopMap extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private final Integer YEAR_INDEX = 0;
        private final Integer CARRIER_INDEX = 6;

        CSVParser csvParser = new CSVParser();
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            String line = text.toString();
            String[] records = csvParser.parseLine(line);

            String year = records[YEAR_INDEX];
            String carrier = records[CARRIER_INDEX];
            String rowKey = carrier + year;

            Put put = new Put(rowKey.getBytes());

            byte[] byteRecords = getBytes(records);
            put.addColumn(Bytes.toBytes("flight"), Bytes.toBytes("records"),byteRecords);
            context.write(new ImmutableBytesWritable(rowKey.getBytes()),put);

        }
    }

    private static byte[] getBytes(String[] records) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(records);
        objectOutputStream.close();
        return byteArrayOutputStream.toByteArray();
    }
}
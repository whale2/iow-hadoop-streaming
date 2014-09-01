package net.iponweb.hadoop.streaming.avro;

import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Input format for streaming jobs converting avro records into JSON representation
 * In case avro file is unreadable starting from some record, all
 * records up to that broken record still could be read and job would not fail.
 */

public class AvroAsJsonInputFormat extends AvroAsTextInputFormat {
    private static Logger log = LoggerFactory.getLogger(AvroAsJsonInputFormat.class);

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        try {
            return new AvroAsJsonRecordReader<Object>(job, (FileSplit) split);
        } catch (IOException e) {
            String file = ((FileSplit) split).getPath().toString();
            log.warn("Error open avro file: ({})", file);
            log.warn("", e);
            return new TextEmptyRecordReader();
        }
    }
}
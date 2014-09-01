package net.iponweb.hadoop.streaming.avro;

import org.apache.hadoop.io.Text;

public class TextEmptyRecordReader extends EmptyRecordReader<Text, Text> {

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }
}
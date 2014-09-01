package net.iponweb.hadoop.streaming.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  Input format for streaming jobs that converts avro records into tab separated text
 *  The trick is guaranteed to work only for plain avro schemas. At least there should not
 *  be any variable length records, such as arrays.
 *  Useful for aggregate generation.
 */

public class AvroAsTextInputFormat extends org.apache.avro.mapred.AvroAsTextInputFormat {
    private static Logger log = LoggerFactory.getLogger(AvroAsTextInputFormat.class);

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        try {
            return new AvroAsTextRecordReader(job, (FileSplit) split);
        } catch (IOException e) {
            String file = ((FileSplit) split).getPath().toString();
            log.warn("Error open avro file: ({})", file);
            log.warn("", e);
            return new TextEmptyRecordReader();
        }
    }

    protected class AvroAsTextRecordReader<T> implements RecordReader<Text, Text> {

        private FileReader<T> reader;
        private T datum;
        private long start;
        private long end;
        private GenericDataTSV tsv;

        AvroAsTextRecordReader(JobConf job, FileSplit split) throws IOException {

            this(DataFileReader.openReader
                    (new FsInput(split.getPath(), job), new GenericDatumReader<T>()), split);
        }

        protected AvroAsTextRecordReader(FileReader<T> reader, FileSplit split)
                throws IOException {

            this.reader = reader;
            reader.sync(split.getStart());                    // sync to start
            this.start = reader.tell();
            this.end = split.getStart() + split.getLength();
            tsv = new GenericDataTSV();
        }

        public Text createKey() {
            return new Text();
        }

        public Text createValue() {
            return new Text();
        }

        public boolean next(Text key, Text value) throws IOException {
            if (!reader.hasNext() || reader.pastSync(end))
                return false;
            datum = reader.next(datum);
            if (datum instanceof ByteBuffer) {
                ByteBuffer b = (ByteBuffer) datum;
                if (b.hasArray()) {
                    int offset = b.arrayOffset();
                    int start = b.position();
                    int length = b.remaining();
                    key.set(b.array(), offset + start, offset + start + length);
                } else {
                    byte[] bytes = new byte[b.remaining()];
                    b.duplicate().get(bytes);
                    key.set(bytes);
                }
            } else {
                key.set(tsv.toString(datum,0,0));
                value.set(tsv.toString(datum,1,-1));
            }
            return true;
        }

        public float getProgress() throws IOException {
            if (end == start) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (getPos() - start) / (float) (end - start));
            }
        }

        public long getPos() throws IOException {
            return reader.tell();
        }

        public void close() throws IOException {
            reader.close();
        }
    }
}
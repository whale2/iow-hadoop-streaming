package net.iponweb.hadoop.streaming.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Copy of AvroAsTextRecordReader (version 1.7.2)
 *
 * Few modifications: NaN could be stored in Avro but can not be converted to JSON
 * We have added some quick-and-dirty workaround, NaNs are just converted to text.
 * We could deal with them later, when converting back from JSON to Avro and even
 * do that way with certain degree of success.
 * Another modification is that we salvage what we can from corrupted files but
 * you better add some switch for this.
 *
 */
public class AvroAsTextRecordReaderCopy<T> implements RecordReader<Text, Text> {
    private FileReader<T> reader;
    private T datum;
    private long start;
    private long end;

    public AvroAsTextRecordReaderCopy(JobConf job, FileSplit split)
            throws IOException {

        this(DataFileReader.openReader
                (new FsInput(split.getPath(), job), new GenericDatumReader<T>()), split);
    }

    protected AvroAsTextRecordReaderCopy(FileReader<T> reader, FileSplit split)
            throws IOException {

        this.reader = reader;
        reader.sync(split.getStart());                    // sync to start
        this.start = reader.tell();
        this.end = split.getStart() + split.getLength();
    }

    public Text createKey() {
        return new Text();
    }

    public Text createValue() {
        return new Text();
    }

    public boolean next(Text key, Text ignore) throws IOException {
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
            key.set(GenericData0.INSTANCE.toString(datum));
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

    private static class GenericData0 extends GenericData {
        public static GenericData0 INSTANCE = new GenericData0();

        public void toString(Object datum, StringBuilder buffer) {
            boolean nan = (datum instanceof Float && ((Float) datum).isNaN())
                    || (datum instanceof Double && ((Double) datum).isNaN());
            if (nan) {
                buffer.append("\"NaN\"");
                return;
            }
            super.toString(datum, buffer);
        }
    }
}
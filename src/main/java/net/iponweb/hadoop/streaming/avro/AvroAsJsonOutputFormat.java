package net.iponweb.hadoop.streaming.avro;


import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;

/**
 * Output format for streaming jobs converting JSON representation into Avro record
 * Multiple output schemas supported in combination with ByKeyOutputFormat. To use
 * this feature one must set configuration option
 * 'iow.streaming.schema.use.prefix' to true, provide output
 * schemas as files to the job and name them in the first column of output in following
 * format: 'filename:schema' where filename is name of file, where current record should
 * be placed and 'schema' is name of file containing output schema
 *
 * For single type of output schema, it should be set in parameter
 * 'iow.streaming.output.schema' which defaults to 'streaming_output_schema'
 *
 *
 * @param <K>   key of streaming job
 * @param <V>   value of streaming job; both usually of Text type
 */

public class AvroAsJsonOutputFormat<K, V> extends AvroOutputFormat<K> {

    protected static Log LOG = LogFactory.getLog(AvroAsJsonOutputFormat.class);

    static <T> void configureDataFileWriter(DataFileWriter<T> writer,
        JobConf job) throws UnsupportedEncodingException {

        if (FileOutputFormat.getCompressOutput(job)) {
            int level = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            String codecName = job.get(AvroJob.OUTPUT_CODEC, DEFLATE_CODEC);
            CodecFactory factory = codecName.equals(DEFLATE_CODEC) ?
                CodecFactory.deflateCodec(level) : CodecFactory.fromString(codecName);
            writer.setCodec(factory);
        }

        writer.setSyncInterval(job.getInt(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

        // copy metadata from job
        for (Map.Entry<String,String> e : job) {
            if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()),e.getValue());
            if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
                writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
                       URLDecoder.decode(e.getValue(), "ISO-8859-1")
                       .getBytes("ISO-8859-1"));
        }
    }

    @Override
    public RecordWriter // <K, V>
        getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog)
        throws IOException {

            String schemaFile = job.get("iow.streaming.output.schema", "streaming_output_schema");
            Schema.Parser p = new Schema.Parser();

            if (job.getBoolean("iow.streaming.schema.use.prefix", false)) {
                // guess schema from file name
                // format is: schema:filename
                // with special keyword default - 'default:filename'

                String str[] = name.split(":");
                if (!str[0].equals("default"))
                    schemaFile = str[0];

                name = str[1];
            }

            LOG.info("getRecordWriter: Using schema: " + schemaFile);
            File f = new File(schemaFile);
            Schema schema = p.parse(f);

            if (schema == null) {
                throw new IOException("Can't find proper output schema");
            }

            DataFileWriter writer = new DataFileWriter(new GenericDatumWriter());

            configureDataFileWriter(writer, job);

            Path path = FileOutputFormat.getTaskOutputPath(job, name + EXT);
            writer.create(schema, path.getFileSystem(job).create(path));

            return createRecordWriter(writer,schema);
    }

    protected RecordWriter<K, V> createRecordWriter(final DataFileWriter w, final Schema schema) {

        return new RecordWriter<K, V>() {

            private DataFileWriter writer = w;

            public void write(K key, V value)
                throws IOException {

                GenericRecord record = fromJson(key.toString(), schema);
                AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>(record);
                writer.append(wrapper.datum());
            }
            public void close(Reporter reporter) throws IOException {
                writer.close();
            }

            protected GenericRecord fromJson(String v, Schema schema) throws IOException {

                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                Decoder decoder = new IOWJsonDecoder(schema, v);
                return reader.read(null, decoder);
            }
        };

    }

}

/**
 * Copyright 2014 IPONWEB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 */

public class AvroAsJsonOutputFormat extends FileOutputFormat<Text, NullWritable> {

    protected static Log LOG = LogFactory.getLog(AvroAsJsonOutputFormat.class);

    static <K> void configureDataFileWriter(DataFileWriter<K> writer,
        JobConf job) throws UnsupportedEncodingException {

        if (FileOutputFormat.getCompressOutput(job)) {
            int level = job.getInt(org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY,
                    org.apache.avro.mapred.AvroOutputFormat.DEFAULT_DEFLATE_LEVEL);
            String codecName = job.get(AvroJob.OUTPUT_CODEC, DEFLATE_CODEC);
            CodecFactory factory = codecName.equals(DEFLATE_CODEC) ?
                CodecFactory.deflateCodec(level) : CodecFactory.fromString(codecName);
            writer.setCodec(factory);
        }

        writer.setSyncInterval(job.getInt(org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY,
                DEFAULT_SYNC_INTERVAL));

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
    public RecordWriter<Text, NullWritable>
        getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog)
        throws IOException {

        Schema schema;
        Schema.Parser p = new Schema.Parser();
        String strSchema = job.get("iow.streaming.output.schema");
        if (strSchema == null) {

            String schemaFile = job.get("iow.streaming.output.schema.file", "streaming_output_schema");

            if (job.getBoolean("iow.streaming.schema.use.prefix", false)) {
                // guess schema from file name
                // format is: schema:filename
                // with special keyword default - 'default:filename'

                String str[] = name.split(":");
                if (!str[0].equals("default"))
                    schemaFile = str[0];

                name = str[1];
            }

            LOG.info(this.getClass().getSimpleName() + ": Using schema from file: " + schemaFile);
            File f = new File(schemaFile);
            schema = p.parse(f);
        }
        else {
            LOG.info(this.getClass().getSimpleName() + ": Using schema from jobconf.");
            schema = p.parse(strSchema);
        }

        if (schema == null) {
            throw new IOException("Can't find proper output schema");
        }

        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());

        configureDataFileWriter(writer, job);

        Path path = FileOutputFormat.getTaskOutputPath(job, name + org.apache.avro.mapred.AvroOutputFormat.EXT);
        writer.create(schema, path.getFileSystem(job).create(path));

        return createRecordWriter(writer, schema);
    }

    protected RecordWriter<Text, NullWritable> createRecordWriter(final DataFileWriter<GenericRecord> w, final Schema schema) {

        return new AvroAsJsonRecordWriter(w, schema);
    }

    protected class AvroAsJsonRecordWriter implements RecordWriter<Text, NullWritable> {

        private final DataFileWriter<GenericRecord> writer;
        private final Schema schema;

        public AvroAsJsonRecordWriter(DataFileWriter<GenericRecord> writer, Schema schema) {
            this.writer = writer;
            this.schema = schema;
        }

        @Override
        public void write(Text k, NullWritable v) throws IOException {
            writer.append(fromJson(k.toString(), schema));
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            writer.close();
        }

        protected GenericRecord fromJson(String txt, Schema schema) throws IOException {

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            Decoder decoder = new IOWJsonDecoder(schema, txt);
            return reader.read(null, decoder);
        }
    }

}

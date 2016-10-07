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

package net.iponweb.hadoop.streaming.parquet;


import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class ParquetAsTextOutputFormat extends FileOutputFormat<Text, Text> {

    private static final Log LOG = LogFactory.getLog(ParquetAsTextOutputFormat.class);
    protected ParquetOutputFormat<SimpleGroup> realOutputFormat = new ParquetOutputFormat<>();

    public static void setWriteSupportClass(Configuration configuration,  Class<?> writeSupportClass) {
        configuration.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, writeSupportClass.getName());
    }

    public static void setBlockSize(Configuration configuration, int blockSize) {
        configuration.setInt(ParquetOutputFormat.BLOCK_SIZE, blockSize);
    }

    public static void setPageSize(Configuration configuration, int pageSize) {
        configuration.setInt(ParquetOutputFormat.PAGE_SIZE, pageSize);
    }

    public static void setCompression(Configuration configuration, CompressionCodecName compression) {
        configuration.set(ParquetOutputFormat.COMPRESSION, compression.name());
    }

    public static void setEnableDictionary(Configuration configuration, boolean enableDictionary) {
        configuration.setBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, enableDictionary);
    }

    private static Path getDefaultWorkFile(JobConf conf, String name, String extension) {
        String file = getUniqueName(conf, name) + extension;
        return new Path(getWorkOutputPath(conf), file);
    }

    private static CompressionCodecName getCodec(JobConf conf) {

        CompressionCodecName codec;

        if (ParquetOutputFormat.isCompressionSet(conf)) { // explicit parquet config
            codec = ParquetOutputFormat.getCompression(conf);
        } else if (getCompressOutput(conf)) { // from hadoop config
            // find the right codec
            Class<?> codecClass = getOutputCompressorClass(conf, DefaultCodec.class);
            LOG.info("Compression set through hadoop codec: " + codecClass.getName());
            codec = CompressionCodecName.fromCompressionCodec(codecClass);
        } else {
            codec = CompressionCodecName.UNCOMPRESSED;
        }

        LOG.info("Compression: " + codec.name());
        return codec;
    }

    public RecordWriter<Text, Text> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress)
        throws IOException {

        // find and load schema


        String writeSchema = job.get("iow.streaming.output.schema");
        MessageType s;

        if (writeSchema == null) {

            String schemaFile = job.get("iow.streaming.output.schema.file","streaming_output_schema");

            if (job.getBoolean("iow.streaming.schema.use.prefix", false)) {
                // guess schema from file name
                // format is: schema:filename
                // with special keyword default - 'default:filename'

                String str[] = name.split(":");
                if (!str[0].equals("default"))
                    schemaFile = str[0];

                name = str[1];
            }

            LOG.info("Using schema: " + schemaFile);
            File f = new File(schemaFile);
            try {
                BufferedReader reader = new BufferedReader(new FileReader(f));
                StringBuilder r = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null)
                    r.append(line);

                writeSchema = r.toString();

            } catch (Throwable e) {
                LOG.error("Can't read schema file " + schemaFile);
                Throwables.propagateIfPossible(e, IOException.class);
                throw new RuntimeException(e);
            }
        }
        s = MessageTypeParser.parseMessageType(writeSchema);

        setWriteSupportClass(job,GroupWriteSupport.class);
        GroupWriteSupport.setSchema(s, job);

        CompressionCodecName codec = getCodec(job);
        String extension = codec.getExtension() + ".parquet";
        Path file = getDefaultWorkFile(job, name, extension);

        ParquetRecordWriter<SimpleGroup> realWriter;
        try {
            realWriter = (ParquetRecordWriter<SimpleGroup>) realOutputFormat.getRecordWriter(job, file, codec);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }

        return createRecordWriter(realWriter, fs, job, name, progress);
    }

    protected RecordWriter<Text, Text>
        createRecordWriter(ParquetRecordWriter<SimpleGroup> w, FileSystem fs, JobConf job, String name, Progressable p)
            throws IOException {

        return new TextRecordWriterWrapper(w, fs, job, name, p);
    }
}

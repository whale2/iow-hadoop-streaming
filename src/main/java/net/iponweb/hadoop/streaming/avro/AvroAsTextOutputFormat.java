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
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class AvroAsTextOutputFormat extends AvroAsJsonOutputFormat {

    protected static Log LOG = LogFactory.getLog(AvroAsTextOutputFormat.class);
    protected GenericDataTSV tsv = new GenericDataTSV();

    @Override
    protected RecordWriter<Text, NullWritable> createRecordWriter(final DataFileWriter<GenericRecord> w, final Schema schema){

        return new AvroAsTextRecordWriter<Text, NullWritable>(w, schema);
    }

    protected class AvroAsTextRecordWriter<K2, V2> implements RecordWriter<K2, V2> {

        private final DataFileWriter<GenericRecord> writer;
        private final Schema schema;

        public AvroAsTextRecordWriter(DataFileWriter<GenericRecord> writer, Schema schema) {
            this.writer = writer;
            this.schema = schema;
        }

        @Override
        public void write(K2 k, V2 v) throws IOException {

            GenericRecord record = fromText(k.toString() + "\t" + v.toString(), schema);
            AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>(record);
            writer.append(wrapper.datum());
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            writer.close();
        }

        protected GenericRecord fromText(String v, Schema schema) throws IOException {

            return tsv.getDatum(v, schema);
        }
    }

}

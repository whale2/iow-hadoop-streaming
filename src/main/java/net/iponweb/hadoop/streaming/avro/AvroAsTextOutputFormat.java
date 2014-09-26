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
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class AvroAsTextOutputFormat<K, V> extends AvroAsJsonOutputFormat<K, V> {

    protected static Log LOG = LogFactory.getLog(AvroAsTextOutputFormat.class);
    protected GenericDataTSV tsv = new GenericDataTSV();

    @Override
    protected RecordWriter<K,V> createRecordWriter(final DataFileWriter w, final Schema schema){

        return new RecordWriter<K, V>() {

            private final DataFileWriter writer = w;

            public void write(K key, V value)
                throws IOException {

                GenericRecord record = fromText(key.toString() + "\t" + value.toString(), schema);
                AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>(record);
                writer.append(wrapper.datum());
            }
            public void close(Reporter reporter) throws IOException {
                writer.close();
            }

            protected GenericRecord fromText(String v, Schema schema) throws IOException {

                return tsv.getDatum(v, schema);
            }
        };
    }
}

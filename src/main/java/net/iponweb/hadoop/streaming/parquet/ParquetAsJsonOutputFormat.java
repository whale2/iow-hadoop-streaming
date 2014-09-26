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


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import parquet.hadoop.ParquetRecordWriter;

import java.io.IOException;

public class ParquetAsJsonOutputFormat<K, V> extends ParquetAsTextOutputFormat<K,V> {

    @Override
    protected RecordWriter<K,V>
        createRecordWriter(ParquetRecordWriter<V> w, FileSystem fs, JobConf job, String name, Progressable p)
            throws IOException {

        return new JsonRecordWriterWrapper<K, V>(w, fs, job, name, p);
    }

}

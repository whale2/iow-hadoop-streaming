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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetRecordWriter;

import java.io.IOException;

public class ParquetAsJsonOutputFormat extends ParquetAsTextOutputFormat {

    @Override
    protected RecordWriter<Text, Text>
        createRecordWriter(ParquetRecordWriter<SimpleGroup> w, FileSystem fs, JobConf job, String name, Progressable p)
            throws IOException {

        return new JsonRecordWriterWrapper(w, fs, job, name, p);
    }

}

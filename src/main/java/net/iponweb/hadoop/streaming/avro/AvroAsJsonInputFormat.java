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

import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Input format for streaming jobs converting avro records into JSON representation
 * In case avro file is unreadable starting from some record, all
 * records up to that broken record still could be read and job would not fail.
 */

public class AvroAsJsonInputFormat extends AvroAsTextInputFormat {
    private static Logger log = LoggerFactory.getLogger(AvroAsJsonInputFormat.class);

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        try {
            return new AvroAsJsonRecordReader<Object>(job, (FileSplit) split);
        } catch (IOException e) {
            String file = ((FileSplit) split).getPath().toString();
            log.warn("Error open avro file: ({})", file);
            log.warn("", e);
            return new TextEmptyRecordReader();
        }
    }
}
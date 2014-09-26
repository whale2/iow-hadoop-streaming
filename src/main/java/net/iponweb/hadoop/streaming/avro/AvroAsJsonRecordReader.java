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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.FileReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * RecordReader for AvoAsJsonInputFormat. Actually a wrapper around
 * AvroAsTextRecordReader with some fault-recovery added. If avro file
 * is broken from particular point, all records up to that point would be
 * read and job have a chance to finish without errors. Number of warning
 * would be emitted in that case including one to MagicSocket
 *
 */

public class AvroAsJsonRecordReader<T> extends AvroAsTextRecordReaderCopy<T> {
    private static Logger log = LoggerFactory.getLogger(AvroAsJsonRecordReader.class);
    private String file;

    public AvroAsJsonRecordReader(JobConf job, FileSplit split) throws IOException {
        super(job, split);
        this.file = split.getPath().toString();
    }

    protected AvroAsJsonRecordReader(FileReader<T> reader, FileSplit split) throws IOException {
        super(reader, split);
    }

    @Override
    public boolean next(Text key, Text ignore) throws IOException {
        try {
            return super.next(key, ignore);
        } catch (AvroRuntimeException e) {
            log.warn("Cannot get next Key from avro file, may be corrupt: ({})", file);
            log.warn("", e);
            return false;
        }
    }
}
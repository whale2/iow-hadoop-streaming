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

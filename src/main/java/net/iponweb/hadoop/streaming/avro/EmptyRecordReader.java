package net.iponweb.hadoop.streaming.avro;

import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public abstract class EmptyRecordReader<K, V> implements RecordReader<K, V> {
    @Override
    public boolean next(K key, V value) throws IOException {
        return false;
    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }
}
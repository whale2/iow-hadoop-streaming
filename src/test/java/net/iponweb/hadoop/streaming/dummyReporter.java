package net.iponweb.hadoop.streaming;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

public class dummyReporter implements Reporter {

    @Override
    public void setStatus(String status) {

    }

    @Override
    public Counters.Counter getCounter(Enum<?> name) {
        return null;
    }

    @Override
    public Counters.Counter getCounter(String group, String name) {
        return null;
    }

    @Override
    public void incrCounter(Enum<?> key, long amount) {

    }

    @Override
    public void incrCounter(String group, String counter, long amount) {

    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
        return null;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void progress() {

    }
}

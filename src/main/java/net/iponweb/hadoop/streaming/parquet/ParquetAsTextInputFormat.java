package net.iponweb.hadoop.streaming.parquet;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.mapred.Container;
import parquet.schema.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetAsTextInputFormat<K, V> extends org.apache.hadoop.mapred.FileInputFormat<K, V> {

  protected ParquetInputFormat<V> realInputFormat = new ParquetInputFormat<V>();

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
    return new TextRecordReaderWrapper<K, V>(realInputFormat, split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    List<Footer> footers = getFooters(job);
    List<ParquetInputSplit> splits = realInputFormat.getSplits(job, footers);

      if (splits == null) {
        return null;
      }

      InputSplit[] resultSplits = new InputSplit[splits.size()];
      int i = 0;
      for (ParquetInputSplit split : splits) {
          resultSplits[i++] = new StreamingParquetInputSplitWrapper(split);
      }

      return resultSplits;
  }

  public List<Footer> getFooters(JobConf job) throws IOException {
    return realInputFormat.getFooters(job, Arrays.asList(super.listStatus(job)));
  }

  protected static class TextRecordReaderWrapper<K, V> implements RecordReader<K, V> {

    private ParquetRecordReader<V> realReader;
    private long splitLen; // for getPos()

    protected Container<V> valueContainer = null;

    private boolean firstRecord = false;
    private boolean eof = false;
    private List<String> ls;

    public TextRecordReaderWrapper(ParquetInputFormat<V> newInputFormat,
                               InputSplit oldSplit,
                               JobConf oldJobConf,
                               Reporter reporter) throws IOException {

      splitLen = oldSplit.getLength();

      try {
        ReadSupport rs = newInputFormat.getReadSupport(oldJobConf);
        realReader = new ParquetRecordReader<V>(rs);
        realReader.initialize(((StreamingParquetInputSplitWrapper)oldSplit).realSplit, oldJobConf, reporter);

        oldJobConf.set("map.input.file",((StreamingParquetInputSplitWrapper)oldSplit).realSplit.getPath().toString());

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {

          firstRecord = true;
          valueContainer = new Container<V>();
          V v = realReader.getCurrentValue();
          valueContainer.set(v);
          ls = groupToStrings((SimpleGroup)v);
        } else {

          eof = true;
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    protected List<String> groupToStrings(SimpleGroup grp) {

        ArrayList<String> s = new ArrayList<String>();

        for (int n = 0; n < grp.getType().getFieldCount(); n ++) {

            Type field = grp.getType().getType(n);
            if (field.isPrimitive()) {
                try {
                    s.add(grp.getValueToString(n, 0));
                }
                catch (RuntimeException e) {
                    if(e.getMessage().startsWith("not found") && field.getRepetition() == Type.Repetition.OPTIONAL)
                        s.add("");
                    else
                        throw e;
                }
            } else {
                s.addAll(groupToStrings((SimpleGroup) grp.getGroup(n, 0)));
            }
        }

        return s;
    }

    @Override
    public K createKey() {
        return (K) ( valueContainer == null ? new Text() : fetchKey() );
    }

    @Override
    public V createValue() {
        return (V) ( valueContainer == null ? new Text() : fetchValue() );
    }

    protected Text fetchKey() {

        return new Text(ls.get(0));
    }

    protected Text fetchValue() {

        return new Text(Joiner.on("\t").join(ls.subList(1, ls.size())));
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(K key, V value) throws IOException {
      if (eof) {
        return false;
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false;
        return true;
      }

      try {
        if (realReader.nextKeyValue()) {
          SimpleGroup g = (SimpleGroup)(realReader.getCurrentValue());
          ls = groupToStrings(g);
          if (key != null) ((Text)key).set(fetchKey());
          if (value != null) ((Text)value).set(fetchValue());
          return true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      eof = true; // strictly not required, just for consistency
      return false;
    }
  }

  private static class StreamingParquetInputSplitWrapper implements InputSplit {

    ParquetInputSplit realSplit;


    @SuppressWarnings("unused") // MapReduce instantiates this.
    public StreamingParquetInputSplitWrapper() {}

    public StreamingParquetInputSplitWrapper(ParquetInputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
        try {
            return realSplit.getLength();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        try {
            return realSplit.getLocations();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realSplit = new ParquetInputSplit();
      realSplit.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      realSplit.write(out);
    }
  }
}

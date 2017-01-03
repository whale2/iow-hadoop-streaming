package net.iponweb.hadoop.streaming.parquet;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.mapred.Container;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetAsTextInputFormat extends org.apache.hadoop.mapred.FileInputFormat<Text, Text> {

    protected ParquetInputFormat<SimpleGroup> realInputFormat = new ParquetInputFormat<>();

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
        return new TextRecordReaderWrapper(realInputFormat, split, job, reporter);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

        //List<Footer> footers = getFooters(job);

        JobContext cnt = ContextUtil.newJobContext(job, new JobID("xxx", 0));

        List<org.apache.hadoop.mapreduce.InputSplit> splits = realInputFormat.getSplits(cnt);
        if (splits == null)
        return null;

        InputSplit[] resultSplits = new InputSplit[splits.size()];
        int i = 0;
        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
          resultSplits[i++] = new StreamingParquetInputSplitWrapper(split);
        }

        return resultSplits;
    }

    public List<Footer> getFooters(JobConf job) throws IOException {
    return realInputFormat.getFooters(job, Arrays.asList(super.listStatus(job)));
    }

    protected static class TextRecordReaderWrapper implements RecordReader<Text, Text> {

        private ParquetRecordReader<SimpleGroup> realReader;
        private long splitLen; // for getPos()

        protected Container<SimpleGroup> valueContainer = null;

        private boolean firstRecord = false;
        private boolean eof = false;
        private List<String> ls;

        public TextRecordReaderWrapper(ParquetInputFormat<SimpleGroup> newInputFormat,
                                   InputSplit oldSplit,
                                   JobConf oldJobConf,
                                   Reporter reporter) throws IOException {

            splitLen = oldSplit.getLength();

            try {
                ReadSupport<SimpleGroup> rs = ParquetInputFormat.getReadSupportInstance(oldJobConf);
                realReader = new ParquetRecordReader<>(rs);
                realReader.initialize(((StreamingParquetInputSplitWrapper)oldSplit).realSplit, oldJobConf, reporter);

                oldJobConf.set("map.input.file",((StreamingParquetInputSplitWrapper)oldSplit).realSplit.getPath().toString());
                oldJobConf.set("mapreduce.map.input.file",((StreamingParquetInputSplitWrapper)oldSplit).realSplit.getPath().toString());

                // read once to gain access to key and value objects
                if (realReader.nextKeyValue()) {

                  firstRecord = true;
                  valueContainer = new Container<>();
                  SimpleGroup v = realReader.getCurrentValue();
                  valueContainer.set(v);
                  ls = groupToStrings(v);
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

            ArrayList<String> s = new ArrayList<>();

            for (int n = 0; n < grp.getType().getFieldCount(); n ++) {

                Type field = grp.getType().getType(n);
                    try {
                        if (!field.isPrimitive())
                           s.addAll(groupToStrings((SimpleGroup) grp.getGroup(n, 0))); // array of groups not (yet) supported
                        else if (field.getRepetition() == Type.Repetition.REPEATED) {

                            boolean is_binary =
                                field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY;
                            StringBuilder sb = new StringBuilder("[");
                            ArrayList<String> arr = new ArrayList<>();
                            for (int i = 0; i < grp.getFieldRepetitionCount(n); i ++)
                                arr.add(is_binary ? "\"" + grp.getValueToString(n, i) + "\"" :
                                    grp.getValueToString(n, i));

                            sb.append(Joiner.on(", ").join(arr));
                            sb.append("]");
                            s.add(sb.toString());
                        }
                        else
                            s.add(grp.getValueToString(n, 0));
                    }
                    catch (RuntimeException e) {
                        if(e.getMessage().startsWith("not found") && field.getRepetition() == Type.Repetition.OPTIONAL)
                            s.add("");
                        else
                            throw e;
                    }
            }

            return s;
        }

        @Override
        public Text createKey() {
            return valueContainer == null ? new Text() : fetchKey();
        }

        @Override
        public Text createValue() {
            return valueContainer == null ? new Text() : fetchValue();
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
        public boolean next(Text key, Text value) throws IOException {

            if (eof) {
                return false;
            }

            try {
                if (!firstRecord ) {
                    if(!realReader.nextKeyValue())
                        return false;
                    SimpleGroup g = realReader.getCurrentValue();
                    ls = groupToStrings(g);
                }

                if (firstRecord)
                    firstRecord = false;

                if (key != null) key.set(fetchKey());
                if (value != null) value.set(fetchValue());
                return true;
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }

    private static class StreamingParquetInputSplitWrapper implements InputSplit {

      FileSplit realSplit;


      @SuppressWarnings("unused") // MapReduce instantiates this.
      public StreamingParquetInputSplitWrapper() {}

      public StreamingParquetInputSplitWrapper(org.apache.hadoop.mapreduce.InputSplit split) throws IOException {
          this.realSplit = (FileSplit)split;
      }

      @Override
      public long getLength() throws IOException {
          return realSplit.getLength();
      }

      @Override
      public String[] getLocations() throws IOException {
          return realSplit.getLocations();
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

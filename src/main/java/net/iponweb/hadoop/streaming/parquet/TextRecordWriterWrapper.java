package net.iponweb.hadoop.streaming.parquet;


import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetRecordWriter;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

public class TextRecordWriterWrapper<K, V> implements RecordWriter<K, V> {

    protected ParquetRecordWriter<V> realWriter;
    protected MessageType schema;
    protected SimpleGroupFactory factory;
    private static final String TAB ="\t";
    protected ArrayList<PathAction> recorder;

    TextRecordWriterWrapper(ParquetRecordWriter<V> w, FileSystem fs, JobConf conf, String name, Progressable progress)
            throws IOException {

        realWriter = w;
        schema = parquet.hadoop.example.GroupWriteSupport.getSchema(conf);
        factory = new SimpleGroupFactory(schema);

        recorder = new ArrayList<PathAction>();
        ArrayList<String[]> Paths = (ArrayList)schema.getPaths();
        Iterator<String[]> pi = Paths.listIterator();

        String[] prevPath = {};

        while (pi.hasNext()) {

            String p[] = pi.next();

            // Find longest common path between prev_path and current
            ArrayList<String> commonPath = new ArrayList<String>();
            int m = 0;
            for (int n = 0; n < prevPath.length; n++) {
                if (n < p.length && p[n].equals(prevPath[n])) {
                    commonPath.add(p[n]);
                } else
                    break;
            }

            // If current element is not inside previous group, restore to the group of common path
            for (int n = commonPath.size(); n < prevPath.length - 1; n++)
                recorder.add(new PathAction(PathAction.ActionType.GROUPEND));

            // If current element is not right after common path, create all required groups
            for (int n = commonPath.size(); n < p.length - 1; n++) {
                PathAction a = new PathAction(PathAction.ActionType.GROUPSTART);
                a.setName(p[n]);
                recorder.add(a);
            }

            prevPath = p;

            PathAction a = new PathAction(PathAction.ActionType.FIELD);

            Type colType = schema.getType(p);

            a.setType(colType.asPrimitiveType().getPrimitiveTypeName());
            a.setRepetition(colType.getRepetition());
            a.setName(p[p.length - 1]);

            recorder.add(a);
        }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        try {
            realWriter.close(null);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
    }

    @Override
    public void write(K key, V value) throws IOException {

        Group grp = factory.newGroup();

        String[] strK = key.toString().split(TAB,-1);
        String[] strV = value.toString().split(TAB,-1);
        String kv_combined[] = (String[]) ArrayUtils.addAll(strK, strV);

        Iterator<PathAction> ai = recorder.iterator();

        Stack<Group> groupStack = new Stack<Group>();
        groupStack.push(grp);
        int i = 0;

        try {
            while(ai.hasNext()) {

                PathAction a = ai.next();
                switch (a.getAction()) {
                    case GROUPEND:
                        grp = groupStack.pop();
                        break;

                    case GROUPSTART:
                        groupStack.push(grp);
                        grp = grp.addGroup(a.getName());
                        break;

                    case FIELD:
                        String s;
                        PrimitiveType.PrimitiveTypeName primType = a.getType();
                        String colName = a.getName();

                        if (i < kv_combined.length) {
                            s = kv_combined[i ++];
                        } else {
                            if (a.getRepetition() == Type.Repetition.OPTIONAL) {
                                i ++;
                                continue;
                            }
                            s = primType == PrimitiveType.PrimitiveTypeName.BINARY ? "" : "0";
                        }

                        try {
                            switch (primType) {

                                case INT32:
                                    grp.append(colName, Integer.parseInt(s));
                                    break;
                                case INT64:
                                case INT96:
                                    grp.append(colName, Long.parseLong(s));
                                    break;
                                case DOUBLE:
                                    grp.append(colName, Double.parseDouble(s));
                                    break;
                                case FLOAT:
                                    grp.append(colName, Float.parseFloat(s));
                                    break;
                                case BOOLEAN:
                                    grp.append(colName, s.equals("true") || s.equals("1"));
                                    break;
                                case BINARY:
                                    grp.append(colName, Binary.fromString(s));
                                    break;
                                default:
                                    throw new RuntimeException("Can't handle type " + primType);
                            }
                        } catch (NumberFormatException e) {

                            grp.append(colName, 0);
                        }
                }
            }

            realWriter.write(null, (V)grp);

        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
    }
}
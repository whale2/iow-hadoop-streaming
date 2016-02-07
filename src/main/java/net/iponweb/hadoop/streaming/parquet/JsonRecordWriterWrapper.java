package net.iponweb.hadoop.streaming.parquet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.io.InvalidRecordException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

public class JsonRecordWriterWrapper<K, V> extends TextRecordWriterWrapper<K, V> {

    ObjectMapper mapper;

    JsonRecordWriterWrapper(ParquetRecordWriter<V> w, FileSystem fs, JobConf conf, String name, Progressable progress)
    throws IOException {

        super(w,fs,conf,name,progress);

        mapper = new ObjectMapper();
    }

    @Override
    public void write(K key, V value) throws IOException {

        try {
           // parse K as JSON and convert into parquet group

            Group grp = factory.newGroup();
            JsonNode node = mapper.readTree(key.toString());

            Iterator<PathAction> ai = recorder.iterator();

            Stack<Group> savedGroup = new Stack<Group>();
            Stack<JsonNode> savedNode = new Stack<JsonNode>();
            while (ai.hasNext()) {

                PathAction a = ai.next();

                switch(a.getAction()) {

                    case GROUPSTART:
                        savedGroup.push(grp);
                        grp = grp.addGroup(a.getName());
                        savedNode.push(node);
                        node = node.get(a.getName());
                        break;

                    case GROUPEND:
                        grp = savedGroup.pop();
                        node = savedNode.pop();
                        break;

                    case FIELD:
                        String colName = a.getName();
                        JsonNode stubNode = node.get(colName);
                        PrimitiveType.PrimitiveTypeName primType = a.getType();
                        try {
                            if (stubNode == null || stubNode.isNull()) {
                                if (a.getRepetition() == Type.Repetition.OPTIONAL ||
                                        a.getRepetition() == Type.Repetition.REPEATED)
                                    continue;
                                else
                                    throw new InvalidRecordException("json column '" +
                                            colName + "' is null, while defined as non-optional in parquet schema");
                            }

                            // If we have 'repeated' field, assume that we should expect JSON-encoded array
                            // Convert array and append all values

                            int repetition = 1;
                            boolean repeated = false;
                            ArrayList<JsonNode> s_vals = null;
                            if (a.getRepetition() == Type.Repetition.REPEATED) {
                                repeated = true;
                                s_vals = new ArrayList();
                                Iterator <JsonNode> itr = stubNode.iterator();
                                repetition = 0;
                                while(itr.hasNext()) {
                                    s_vals.add(itr.next());  // No array-of-objects!
                                    repetition ++;
                                }
                            }

                            for (int j = 0; j < repetition; j ++) {

                                if (repeated) {
                                    // extract new s
                                    stubNode = s_vals.get(j);
                                    if (stubNode == null || stubNode.isNull())
                                        continue;
                                }

                                switch (primType) {

                                    case INT32:
                                        grp.append(colName, stubNode.getIntValue());
                                        break;
                                    case INT64:
                                    case INT96:
                                        grp.append(colName, stubNode.getLongValue());
                                        break;
                                    case DOUBLE:
                                        grp.append(colName, stubNode.getDoubleValue());
                                        break;
                                    case FLOAT:
                                        grp.append(colName, (float) stubNode.getDoubleValue());
                                        break;
                                    case BOOLEAN:
                                        grp.append(colName, stubNode.getBooleanValue());
                                        break;
                                    case BINARY:
                                        grp.append(colName, stubNode.getTextValue());
                                        break;
                                    default:
                                        throw new RuntimeException("Can't handle type " + primType);
                                }
                            }
                        } catch (Exception e) {

                            e.printStackTrace();
                            throw new IOException(e);
                        }
                }
            }

            realWriter.write(null, (V)grp);
        }
        catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
    }
}

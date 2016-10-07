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


import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Stack;

public class TextRecordWriterWrapper implements RecordWriter<Text, Text> {

    protected ParquetRecordWriter<SimpleGroup> realWriter;
    protected MessageType schema;
    protected SimpleGroupFactory factory;
    private static final String TAB ="\t";
    protected ArrayList<PathAction> recorder;

    TextRecordWriterWrapper(ParquetRecordWriter<SimpleGroup> w, FileSystem fs, JobConf conf, String name, Progressable progress)
            throws IOException {

        realWriter = w;
        schema = GroupWriteSupport.getSchema(conf);
        factory = new SimpleGroupFactory(schema);

        recorder = new ArrayList<>();
        ArrayList<String[]> Paths = (ArrayList<String[]>)schema.getPaths();
        Iterator<String[]> pi = Paths.listIterator();

        String[] prevPath = {};

        short grpDepth = 0;
        while (pi.hasNext()) {

            String p[] = pi.next();

            // Find longest common path between prev_path and current
            ArrayList<String> commonPath = new ArrayList<String>();
            for (int n = 0; n < prevPath.length; n++) {
                if (n < p.length && p[n].equals(prevPath[n])) {
                    commonPath.add(p[n]);
                } else
                    break;
            }

            // If current element is not inside previous group, restore to the group of common path
            for (int n = commonPath.size(); n < prevPath.length - 1; n++) {
                recorder.add(new PathAction(PathAction.ActionType.GROUPEND));
                grpDepth --;
            }

            // If current element is not right after common path, create all required groups
            for (int n = commonPath.size(); n < p.length - 1; n++) {
                PathAction a = new PathAction(PathAction.ActionType.GROUPSTART);
                a.setName(p[n]);
                recorder.add(a);
                grpDepth ++;
            }

            prevPath = p;

            PathAction a = new PathAction(PathAction.ActionType.FIELD);

            Type colType = schema.getType(p);

            a.setType(colType.asPrimitiveType().getPrimitiveTypeName());
            a.setRepetition(colType.getRepetition());
            a.setName(p[p.length - 1]);

            recorder.add(a);
        }

        // Close trailing groups
        while(grpDepth -- > 0)
            recorder.add(new PathAction(PathAction.ActionType.GROUPEND));
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
    public void write(Text key, Text value) throws IOException {

        Group grp = factory.newGroup();

        String[] strK = key.toString().split(TAB,-1);
        String[] strV = value == null ? new String[0] : value.toString().split(TAB,-1);
        String kv_combined[] = (String[]) ArrayUtils.addAll(strK, strV);

        Iterator<PathAction> ai = recorder.iterator();

        Stack<Group> groupStack = new Stack<>();
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
                        String s = null;
                        PrimitiveType.PrimitiveTypeName primType = a.getType();
                        String colName = a.getName();

                        if (i < kv_combined.length)
                            s = kv_combined[i ++];

                        if (s == null) {
                            if (a.getRepetition() == Type.Repetition.OPTIONAL) {
                                i ++;
                                continue;
                            }
                            s = primType == PrimitiveType.PrimitiveTypeName.BINARY ? "" : "0";
                        }

                        // If we have 'repeated' field, assume that we should expect JSON-encoded array
                        // Convert array and append all values
                        int repetition = 1;
                        boolean repeated = false;
                        ArrayList<String> s_vals = null;
                        if (a.getRepetition() == Type.Repetition.REPEATED) {
                            repeated = true;
                            s_vals = new ArrayList<>();
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode node = mapper.readTree(s);
                            Iterator <JsonNode> itr = node.iterator();
                            repetition = 0;
                            while(itr.hasNext()) {

                                String o;
                                switch (primType) {
                                    case BINARY:
                                        o = itr.next().getTextValue();  // No array-of-objects!
                                        break;
                                    case BOOLEAN:
                                        o = String.valueOf(itr.next().getBooleanValue());
                                        break;
                                    default:
                                        o = String.valueOf(itr.next().getNumberValue());
                                }

                                s_vals.add(o);
                                repetition ++;
                            }
                        }

                        for (int j = 0; j < repetition; j ++) {

                            if (repeated)
                                // extract new s
                                s = s_vals.get(j);


                            try {
                                switch (primType) {

                                    case INT32:
                                        grp.append(colName, new Double(Double.parseDouble(s)).intValue());
                                        break;
                                    case INT64:
                                    case INT96:
                                        grp.append(colName, new Double(Double.parseDouble(s)).longValue());
                                        break;
                                    case DOUBLE:
                                        grp.append(colName, Double.parseDouble(s));
                                        break;
                                    case FLOAT:
                                        grp.append(colName, Float.parseFloat(s));
                                        break;
                                    case BOOLEAN:
                                        grp.append(colName, s.equalsIgnoreCase("true") || s.equals("1"));
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
            }

            realWriter.write(null, (SimpleGroup)grp);

        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new IOException(e);
        }
        catch (Exception e) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            e.printStackTrace(new PrintStream(out));
            throw new RuntimeException("Failed on record " + grp + ", schema=" + schema + ", path action=" + recorder +
                    " exception = " + e.getClass() + ", msg=" + e.getMessage() + ", cause=" + e.getCause() + ", trace=" + out.toString());
        }

    }
}
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import parquet.example.data.Group;
import parquet.hadoop.ParquetRecordWriter;
import parquet.org.codehaus.jackson.JsonNode;
import parquet.org.codehaus.jackson.map.ObjectMapper;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;

import java.io.IOException;
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
            savedGroup.push(grp);

            Stack<JsonNode> savedNode = new Stack<JsonNode>();
            savedNode.push(node);

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
                            if (node.isNull()) {
                                if (a.getRepetition() == Type.Repetition.OPTIONAL)
                                    continue;
                                else
                                    throw new NullPointerException("json column '" +
                                            colName + "' is null, while defined as non-optional in parquet schema");
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

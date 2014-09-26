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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import parquet.example.data.simple.SimpleGroup;
import parquet.hadoop.ParquetInputFormat;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetAsJsonInputFormat<K, V> extends ParquetAsTextInputFormat {

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
      return new JsonRecordReaderWrapper<K, V>(realInputFormat, split, job, reporter);
  }

  protected static class JsonRecordReaderWrapper<K, V>
          extends TextRecordReaderWrapper<K, V> {

      public JsonRecordReaderWrapper(ParquetInputFormat<V> newInputFormat, InputSplit oldSplit, JobConf oldJobConf, Reporter reporter) throws IOException {
          super(newInputFormat, oldSplit, oldJobConf, reporter);
      }

      private void groupToJson(JsonGenerator currentGenerator, SimpleGroup grp)
              throws IOException {

          GroupType gt = grp.getType();

          currentGenerator.writeStartObject();
          for(int i = 0; i < gt.getFieldCount(); i ++) {

              String field = gt.getFieldName(i);
              try {
                  Type t = gt.getType(i);
                  if (t.isPrimitive()) {
                      switch (t.asPrimitiveType().getPrimitiveTypeName()) {
                          case BINARY:
                              currentGenerator.writeStringField(field, grp.getString(i, 0));
                              break;
                          case INT32:
                              currentGenerator.writeNumberField(field, grp.getInteger(i, 0));
                              break;
                          case INT96:
                          case INT64:
                              // clumsy way - TODO - Subclass SimpleGroup or something like that
                              currentGenerator.writeNumberField(field, Long.parseLong(grp.getValueToString(i, 0)));
                              break;
                          case DOUBLE:
                          case FLOAT:
                              currentGenerator.writeNumberField(field, Double.parseDouble(grp.getValueToString(i, 0)));
                              break;
                          case BOOLEAN:
                              currentGenerator.writeBooleanField(field, grp.getBoolean(i, 0));
                              break;
                          default:
                              throw new RuntimeException("Can't handle type " + gt.getType(i));
                      }
                  } else {
                      currentGenerator.writeFieldName(field);
                      groupToJson(currentGenerator, (SimpleGroup) grp.getGroup(i, 0));
                  }
              }
              catch (Exception e) {
                  if (e.getMessage().startsWith("not found") && gt.getType(i).getRepetition() == Type.Repetition.OPTIONAL)
                      currentGenerator.writeNullField(field);
                  else
                      throw new RuntimeException(e);
              }
          }
          currentGenerator.writeEndObject();
      }

      @Override
      protected Text fetchValue() {
          return new Text();
      }

      @Override
      protected List<String> groupToStrings(SimpleGroup grp) {

          JsonGenerator generator;
          CharArrayWriter jWriter;

          List<String> jls = new ArrayList<String>();
          jWriter = new CharArrayWriter();
          JsonFactory jFactory = new JsonFactory();
          try {
              generator = jFactory.createJsonGenerator(jWriter);
              groupToJson(generator,grp);
              generator.close();
          }
          catch (Exception e) {
              e.printStackTrace();
              return null;
          }

          jls.add(jWriter.toString());
          return jls;
      }
  }
}

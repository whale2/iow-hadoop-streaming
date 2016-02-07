/**
 * Copyright 2012 Twitter, Inc.
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
 *
 */

package net.iponweb.hadoop.streaming.parquet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public class GroupReadSupport extends ReadSupport<Group> {

  private static final Log LOG = LogFactory.getLog(GroupReadSupport.class);

  @Override
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema) {
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
    return new ReadContext(getSchemaForRead(fileSchema, partialSchemaString));
  }

  @Override
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {

      return init(context.getConfiguration(), context.getMergedKeyValueMetaData(), context.getFileSchema());
  }

  @Override
  public RecordMaterializer<Group> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    return new GroupRecordConverter(readContext.getRequestedSchema());
  }
}

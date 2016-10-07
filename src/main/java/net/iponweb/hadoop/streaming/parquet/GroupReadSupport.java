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
 * This file is a modified version of GroupReadSupport.java from original parquet source code
 */

package net.iponweb.hadoop.streaming.parquet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

public class GroupReadSupport extends ReadSupport<Group> {

    public static final String PARQUET_READ_SCHEMA_FILE = "parquet.read.schema.file";
    private static final Log LOG = LogFactory.getLog(GroupReadSupport.class);

    @Override
    public ReadContext init(
            Configuration configuration, Map<String, String> keyValueMetaData,
            MessageType fileSchema) {

        String partialSchemaString;
        String partialSchemaFile = configuration.get(PARQUET_READ_SCHEMA_FILE, "");
        if (!partialSchemaFile.isEmpty()) {
            StringBuilder r = new StringBuilder();
            try {
                BufferedReader br = new BufferedReader(new FileReader(new File(partialSchemaFile)));
                String line;
                while ((line = br.readLine()) != null)
                    r.append(line);
            } catch (Exception e) {
                throw new RuntimeException("Can't read schema from file " + partialSchemaFile + ": " + e.getMessage());
            }

            partialSchemaString = r.toString();
        }
        else
            partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);

        return new ReadContext(getSchemaForRead(fileSchema, partialSchemaString));
    }

    @Override
    public RecordMaterializer<Group> prepareForRead(Configuration configuration,
                                                    Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new GroupRecordConverter(readContext.getRequestedSchema());
    }
}

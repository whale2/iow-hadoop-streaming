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

import parquet.schema.PrimitiveType;
import parquet.schema.Type;

public class PathAction {

    private ActionType t;
    private String name;
    private PrimitiveType.PrimitiveTypeName type;
    private Type.Repetition repetition;

    PathAction(ActionType t) {
        this.t = t;
    }

    public ActionType getAction() {
        return this.t;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public PrimitiveType.PrimitiveTypeName getType() {
        return this.type;
    }

    public void setType(PrimitiveType.PrimitiveTypeName type) {
        this.type = type;
    }

    public Type.Repetition getRepetition() {
        return this.repetition;
    }

    public void setRepetition(Type.Repetition repetition) {
        this.repetition = repetition;
    }

    public String toString() {
        switch (this.t) {
            case FIELD:
                return "FIELD: " + this.name + ", type: " + this.type.toString();
            case GROUPEND:
                return "GROUPEND";
            case GROUPSTART:
                return "GROUPSTART: " + this.name;
        }
        return null;
        }

    protected enum ActionType {

        FIELD, GROUPSTART, GROUPEND
    }
}

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

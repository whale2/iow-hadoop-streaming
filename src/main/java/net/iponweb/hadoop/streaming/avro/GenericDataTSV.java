package net.iponweb.hadoop.streaming.avro;

import com.google.common.base.Joiner;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GenericDataTSV extends GenericData {

    GenericData.Record getDatum(String tsv, Schema s) throws IOException, JsonProcessingException {
        List<String> tsvStrings = Arrays.asList(tsv.split("\t",-1));
        return getDatum(tsvStrings.iterator(), s);
    }

    GenericData.Record getDatum(Iterator<String> tsvi, Schema s) throws IOException, JsonProcessingException {

        List<Schema.Field> fields = s.getFields();
        GenericData.Record innerDatum = new Record(s);
        Iterator<Schema.Field> it = fields.iterator();
        int m = 0;
        while(it.hasNext()) {

            Schema.Field f = it.next();
            String t;

            Schema type = f.schema();

            switch (type.getType()) {
                case UNION:

                    t = tsvi.hasNext() ? tsvi.next() : "";

                    // Get union branches, try to apply branch to value
                    List<Schema> branches = f.schema().getTypes();
                    Iterator<Schema> br = branches.iterator();
                    boolean hasNull = false;
                    boolean hasString = false;
                    boolean hasArray = false;
                    Schema payload = null;
                    while (br.hasNext()) {
                        Schema bs = br.next();
                        switch (bs.getType()) {
                            case ARRAY:
                                hasArray = true; // Currently we only support [array,null] case
                                payload = bs.getElementType();
                                break;
                            case NULL:
                                hasNull = true;
                                break;
                            case STRING:
                                hasString = true;
                            default:
                                payload = bs;
                        }
                    }

                    // If we have string branch - put the data because string can handle everything
                    // If we haven't string, but have null and data is empty string - put null
                    // Otherwise try to put primitive as is

                    if (hasArray && !t.isEmpty()) { // assuming that array is already json-encoded
                        innerDatum.put(m, createArray(payload, t));
                    }
                    else if (hasString) {
                        innerDatum.put(m, t);
                    }
                    else if (hasNull && t.equals("")) {
                        innerDatum.put(m, null);
                    }
                    else {
                        if (payload != null)
                            putPrimitive(innerDatum, m, payload, t);
                    }
                    break;

                case RECORD:
                    Schema innerSchema = f.schema();
                    innerDatum.put(m, getDatum(tsvi, innerSchema));
                    break;
                case ARRAY:
                    t = tsvi.hasNext() ? tsvi.next() : "[]";
                    innerDatum.put(m, createArray(type.getElementType(), t.isEmpty() ? "[]" : t));
                    break;
                default:
                    t = tsvi.hasNext() ? tsvi.next() : "";
                    putPrimitive(innerDatum, m, type, t);
            }
            m ++;
        }

        return innerDatum;
    }

    private Array<java.io.Serializable> createArray(Schema type, String t)
            throws IOException, JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(t);
        Iterator <JsonNode> i = node.iterator();
        Array<java.io.Serializable> arr = new GenericData.Array<java.io.Serializable>(node.size(), Schema.createArray(type));
        while(i.hasNext()) {
            switch (type.getType()) {
                case INT:
                    arr.add(i.next().getIntValue());
                    break;
                case FLOAT:
                case DOUBLE:
                    arr.add(i.next().getDoubleValue());
                    break;
                default:
                    arr.add(i.next().getTextValue());  // No array-of-objects!
            }
        }

        return arr;
    }

    private void putPrimitive(GenericData.Record datum, int pos, Schema type, String t) {

        float fv;
        double dv;

        try {
            switch (type.getType()) {
                case BOOLEAN:
                    datum.put(pos, t.equals("true") || t.equals("1"));
                    break;
                case INT:
                    datum.put(pos, Integer.parseInt(t));
                    break;
                case LONG:
                    datum.put(pos, Long.parseLong(t));
                    break;
                case FLOAT:
                    if (t.equalsIgnoreCase("NaN"))
                        fv = Float.NaN;
                    else if (t.equalsIgnoreCase("-Inf"))
                        fv = Float.NEGATIVE_INFINITY;
                    else if (t.equalsIgnoreCase("+Inf") || t.equalsIgnoreCase("Inf"))
                        fv = Float.POSITIVE_INFINITY;
                    else
                        fv = Float.parseFloat(t);
                    datum.put(pos, fv);
                    break;
                case DOUBLE:
                    if (t.equalsIgnoreCase("NaN"))
                        dv = Double.NaN;
                    else if (t.equalsIgnoreCase("-Inf"))
                        dv = Double.NEGATIVE_INFINITY;
                    else if (t.equalsIgnoreCase("+Inf") || t.equalsIgnoreCase("Inf"))
                        dv = Double.POSITIVE_INFINITY;
                    else
                        dv = Double.parseDouble(t);
                    datum.put(pos, dv);
                    break;
                case ENUM:
                    datum.put(pos, new EnumSymbol(type,t));
                    break;
                default:
                    datum.put(pos, t);
            }
        }
        catch (NumberFormatException e) {
            datum.put(pos, null);
        }
    }

    @Override
    public String toString(Object datum) {

        StringBuilder sb = new StringBuilder();
        toString(datum, sb);
        return sb.toString();
    }

    @Override
    public void toString(Object datum, StringBuilder sb) {

        List<Schema.Field> fields = ((GenericDataTSV.Record)datum).getSchema().getFields();

        Iterator<Schema.Field> i = fields.iterator();
        int n = 0;
        while(i.hasNext()) {
            Schema.Field f = i.next();
            Object val = ((GenericDataTSV.Record) datum).get(n++);

            fieldToString(f, val, sb);

            if (i.hasNext())
                sb.append("\t");
        }
    }

    public String toString(Object datum,int start,int stop) {

        StringBuilder sb = new StringBuilder();
        ArrayList<Schema.Field> fields = (ArrayList<Schema.Field>)((GenericDataTSV.Record)datum).getSchema().getFields();

        if (stop == -1)
            stop += fields.size();
        for (int i = start; i <= stop; i ++) {
            Schema.Field f = fields.get(i);
            Object val = ((GenericDataTSV.Record) datum).get(i);

            fieldToString(f, val, sb);

            if (i != stop)
                sb.append("\t");
        }

        return sb.toString();
    }

    private void arrayToString(Object val, Schema schema, StringBuilder sb) {
        sb.append("[");
        ArrayList<String> arr = new ArrayList<String>();
        Schema.Type s = schema.getElementType().getType();
        Iterator it = ((GenericDataTSV.Array) val).iterator();
        while (it.hasNext())
            arr.add(s == Schema.Type.STRING ?
                            "\"" + it.next().toString() + "\"" : it.next().toString());

        sb.append(Joiner.on(", ").join(arr));
        sb.append("]");
    }

    private void fieldToString(Schema.Field f, Object val, StringBuilder sb) {

        switch (f.schema().getType()) {

            case RECORD:
                sb.append(toString(val));
                break;

            case NULL:
                break;

            case ARRAY:
                arrayToString(val, f.schema(), sb);
                break;

            case UNION:

                // all fields are wrapped into unions...

                boolean hasArray = false;
                List<Schema> tps = f.schema().getTypes();
                Iterator<Schema> tt = tps.iterator();
                Schema arraySchema = null;
                while(tt.hasNext())
                    if((arraySchema = tt.next()).getType() == Schema.Type.ARRAY) {
                        hasArray = true;
                        break;
                    }

                if (hasArray && !val.toString().equals("null")) {
                    arrayToString(val, arraySchema, sb);
                    break;
                }

            default:
                if (val != null)
                    sb.append(val.toString());
        }

    }

}

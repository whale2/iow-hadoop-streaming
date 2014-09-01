package net.iponweb.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class GenericDataTSV extends GenericData {

    GenericData.Record getDatum(String tsv, Schema s) {
        List<String> tsvStrings = Arrays.asList(tsv.split("\t",-1));
        return getDatum(tsvStrings.iterator(), s);
    }

    GenericData.Record getDatum(Iterator<String> tsvi, Schema s) {

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
                    Schema payload = null;
                    while (br.hasNext()) {
                        Schema bs = br.next();
                        switch (bs.getType()) {
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

                    if (hasString) {
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
                    innerDatum.put(m,getDatum(tsvi, innerSchema));
                    break;
                default:
                    t = tsvi.hasNext() ? tsvi.next() : "";
                    putPrimitive(innerDatum, m, type, t);
            }
            m ++;
        }

        return innerDatum;
    }

    private void putPrimitive(GenericData.Record datum, int pos, Schema type, String t) {
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
                    datum.put(pos, Float.parseFloat(t));
                    break;
                case DOUBLE:
                    datum.put(pos, Double.parseDouble(t));
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
    public void toString(Object datum, StringBuilder buffer) {
        boolean nan = (datum instanceof Float && ((Float) datum).isNaN())
                || (datum instanceof Double && ((Double) datum).isNaN());
        if (nan) {
            buffer.append("\"NaN\"");
            return;
        }
        super.toString(datum, buffer);
    }

    @Override
    public String toString(Object datum) {

        StringBuilder sb = new StringBuilder();
        List<Schema.Field> fields = ((GenericDataTSV.Record)datum).getSchema().getFields();

        Iterator<Schema.Field> i = fields.iterator();
        int n = 0;
        while(i.hasNext()) {
            Schema.Field f = i.next();
            Object val = ((GenericDataTSV.Record) datum).get(n ++);
            switch (f.schema().getType()) {

                case RECORD:
                    sb.append(toString(val));
                    break;
                case NULL:
                    break;
                default:
                    if (val != null)
                        sb.append(val.toString());
            }

            if (i.hasNext())
                sb.append("\t");
        }

        return new String(sb.toString());
    }

    public String toString(Object datum,int start,int stop) {

        StringBuilder sb = new StringBuilder();
        ArrayList<Schema.Field> fields = (ArrayList)((GenericDataTSV.Record)datum).getSchema().getFields();

        if (stop == -1)
            stop += fields.size();
        for (int i = start; i <= stop; i ++) {
            Schema.Field f = fields.get(i);
            Object val = ((GenericDataTSV.Record) datum).get(i);
            switch (f.schema().getType()) {

                case RECORD:
                    sb.append(toString(val));
                    break;
                case NULL:
                    break;
                default:
                    if (val != null)
                        sb.append(val.toString());
            }

            if (i != stop)
                sb.append("\t");
        }

        return new String(sb.toString());
    }

}

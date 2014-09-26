/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified in IPONWEB, 2014
 *
 */

package net.iponweb.hadoop.streaming.avro;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.*;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/* This class is based on original JsonDecoder from Avro with attempt to
 * handle complex schemas. With somewhat limited success it can. Known limitation
 * is schema with union of two or more named records or other named types.
 * Simpler unions seem to work well based on experience of running this on prod servers
 * And we can convert "NaN" back to NaN if it is placed in double or float fields
 */

public class IOWJsonDecoder extends ParsingDecoder implements Parser.ActionHandler  {

    private JsonParser in;
    private static JsonFactory jsonFactory = new JsonFactory();
    static final String CHARSET = "ISO-8859-1";

    ReorderBuffer currentReorderBuffer;
    Stack<ReorderBuffer> reorderBuffers = new Stack<ReorderBuffer>();

    private static final Log LOG = LogFactory.getLog(IOWJsonDecoder.class);

    private static class ReorderBuffer {
        public Map<String, List<JsonElement>> savedFields = new HashMap<String, List<JsonElement>>();
        public JsonParser origParser = null;
    }

    public IOWJsonDecoder(Symbol s, String str) throws IOException {

        super(s);
        in = jsonFactory.createJsonParser(str);
        in.nextToken();
    }

    public IOWJsonDecoder(Schema s, String str) throws IOException {
        this(getSymbol(s), str);
    }

    private static Symbol getSymbol(Schema schema) {
        if (null == schema) {
            throw new NullPointerException("Schema cannot be null!");
        }
        return new JsonGrammarGenerator().generate(schema);
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        advance(Symbol.BYTES);
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            byte[] result = readByteArray();
            in.nextToken();
            return ByteBuffer.wrap(result);
        } else {
            throw error("bytes");
        }
    }

    private void advance(Symbol symbol) throws IOException {
        this.parser.processTrailingImplicitActions();
        if (in.getCurrentToken() == null && this.parser.depth() == 1)
            throw new EOFException();
        parser.advance(symbol);
    }

    @Override
    public void readNull() throws IOException {
        advance(Symbol.NULL);
        if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
            in.nextToken();
        } else {
            throw error("null");
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        advance(Symbol.BOOLEAN);
        JsonToken t = in.getCurrentToken();
          if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
            in.nextToken();
            return t == JsonToken.VALUE_TRUE;
        } else {
            String s = in.getText();
            if(s.equals("false") || s.equals("FALSE") || s.equals("0")) {
                in.nextToken();
                return false;
            }
            else if(s.equals("true") || s.equals("TRUE") || s.equals("1")) {
                in.nextToken();
                return true;
            }
            throw error("boolean");
        }
    }

    @Override
    public int readInt() throws IOException {
        advance(Symbol.INT);
        if (in.getCurrentToken().isNumeric()) {
            int result = in.getIntValue();
            in.nextToken();
            return result;
        } else {
            try {
                String s = in.getText();
                in.nextToken();
                return Integer.parseInt(s);
            }
            catch(Exception e) {
                throw error("int (" + e.getMessage() + ")");
            }
        }
    }

    @Override
    public long readLong() throws IOException {
        advance(Symbol.LONG);
        if (in.getCurrentToken().isNumeric()) {
            long result = in.getLongValue();
            in.nextToken();
            return result;
        } else {
            try {
                String s = in.getText();
                in.nextToken();
                return Long.parseLong(s);
            }
            catch(Exception e) {
                throw error("long (" + e.getMessage() + ")");
            }
        }
    }

    @Override
    public float readFloat() throws IOException {
        advance(Symbol.FLOAT);
        if (in.getCurrentToken().isNumeric()) {
            float result = in.getFloatValue();
            in.nextToken();
            return result;
        } else {
            try {
                String s = in.getText();
                in.nextToken();
                if (s.equals("NaN")) {
                    return Float.NaN;
                }
                else {
                    return Float.parseFloat(s);
                }
            }
            catch (Exception e) {
                throw error("float (" + e.getMessage() + ")");
            }
        }
    }

    @Override
    public double readDouble() throws IOException {
        advance(Symbol.DOUBLE);
        if (in.getCurrentToken().isNumeric()) {
            double result = in.getDoubleValue();
            in.nextToken();
            return result;
        } else {
            try {
                String s = in.getText();
                in.nextToken();
                if (s.equals("NaN")) {
                    return Double.NaN;
                }
                else {
                    return Double.parseDouble(s);
                }
            }
            catch (Exception e) {
                throw error("double (" + e.getMessage() + ")");
            }
        }
    }

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        return new Utf8(readString());
    }

    @Override
    public String readString() throws IOException {
        advance(Symbol.STRING);
        if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
            parser.advance(Symbol.MAP_KEY_MARKER);
                if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
                    throw error("map-key");
                }
            } else {
                if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
                throw error("string");
            }
        }
        String result = in.getText();
        in.nextToken();
        return result;
    }

    @Override
    public void skipString() throws IOException {
        advance(Symbol.STRING);
        if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
            parser.advance(Symbol.MAP_KEY_MARKER);
            if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
                throw error("map-key");
            }
        } else {
              if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
                  throw error("string");
              }
        }
        in.nextToken();
    }


    private byte[] readByteArray() throws IOException {
        byte[] result = in.getText().getBytes(CHARSET);
        return result;
    }

    @Override
    public void skipBytes() throws IOException {
        advance(Symbol.BYTES);
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            in.nextToken();
        } else {
            throw error("bytes");
        }
    }

    private void checkFixed(int size) throws IOException {
        advance(Symbol.FIXED);
        Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
        if (size != top.size) {
            throw new AvroTypeException(
                "Incorrect length for fixed binary: expected " +
            top.size + " but received " + size + " bytes.");
        }
    }

    @Override
    public void readFixed(byte[] bytes, int start, int len) throws IOException {
        checkFixed(len);
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            byte[] result = readByteArray();
            in.nextToken();
            if (result.length != len) {
                throw new AvroTypeException("Expected fixed length " + len
                    + ", but got" + result.length);
            }
            System.arraycopy(result, 0, bytes, start, len);
        } else {
            throw error("fixed");
        }
    }

    @Override
    public void skipFixed(int length) throws IOException {
        checkFixed(length);
        doSkipFixed(length);
    }

    private void doSkipFixed(int length) throws IOException {
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            byte[] result = readByteArray();
            in.nextToken();
            if (result.length != length) {
                throw new AvroTypeException("Expected fixed length " + length
                    + ", but got" + result.length);
            }
        } else {
            throw error("fixed");
        }
    }

    @Override
    protected void skipFixed() throws IOException {
        advance(Symbol.FIXED);
        Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
        doSkipFixed(top.size);
    }

    @Override
    public int readEnum() throws IOException {
        advance(Symbol.ENUM);
        Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
        if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
            in.getText();
            int n = top.findLabel(in.getText());
            if (n >= 0) {
                in.nextToken();
                return n;
            }
            throw new AvroTypeException("Unknown symbol in enum " + in.getText());
        } else {
            throw error("fixed");
        }
    }

    @Override
    public long readArrayStart() throws IOException {
        advance(Symbol.ARRAY_START);
        if (in.getCurrentToken() == JsonToken.START_ARRAY) {
            in.nextToken();
            return doArrayNext();
        } else {
            throw error("array-start");
        }
    }

    @Override
    public long arrayNext() throws IOException {
        advance(Symbol.ITEM_END);
        return doArrayNext();
    }


    private long doArrayNext() throws IOException {
        if (in.getCurrentToken() == JsonToken.END_ARRAY) {
            parser.advance(Symbol.ARRAY_END);
            in.nextToken();
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public long skipArray() throws IOException {
        advance(Symbol.ARRAY_START);
        if (in.getCurrentToken() == JsonToken.START_ARRAY) {
            in.skipChildren();
            in.nextToken();
            advance(Symbol.ARRAY_END);
        } else {
            throw error("array-start");
        }
        return 0;
    }

    @Override
    public long readMapStart() throws IOException {
        advance(Symbol.MAP_START);
        if (in.getCurrentToken() == JsonToken.START_OBJECT) {
            in.nextToken();
            return doMapNext();
        } else {
            throw error("map-start");
        }
    }

    @Override
    public long mapNext() throws IOException {
        advance(Symbol.ITEM_END);
        return doMapNext();
    }

    private long doMapNext() throws IOException {
        if (in.getCurrentToken() == JsonToken.END_OBJECT) {
            in.nextToken();
            advance(Symbol.MAP_END);
        return 0;
        } else {
            return 1;
        }
    }

    @Override
    public long skipMap() throws IOException {
        advance(Symbol.MAP_START);
        if (in.getCurrentToken() == JsonToken.START_OBJECT) {
            in.skipChildren();
            in.nextToken();
            advance(Symbol.MAP_END);
        } else {
            throw error("map-start");
        }
        return 0;
    }

    @Override
    public int readIndex() throws IOException {

        advance(Symbol.UNION);
        Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

        int n;
        JsonToken token = in.getCurrentToken();
        if (token == JsonToken.VALUE_NULL) {
            n = a.findLabel("null");
            if (n < 0)
                throw error("null");
            parser.pushSymbol(a.getSymbol(n));
            return n;

        }

        if (token == JsonToken.START_OBJECT) {

            // Iterate over union branches. Assume that first SEQUENCE symbol is our guessed branch
            n = 0;
            for (Symbol s : a.symbols) {
                if (s.kind == Symbol.Kind.SEQUENCE)
                    break;
                n ++;
            }

            if (n == a.symbols.length)
                throw new AvroTypeException("Union has no branch of type 'record' or 'array'");

            parser.pushSymbol(a.getSymbol(n));
            return n;

        }
        else {

            // Token is not an object
            // Check if we have enum somewhere in our symbols. If so, check our value against enum values
            // If found, push that enum into parser

            n = 0;
            for (Symbol s : a.symbols) {

                if(s.kind == Symbol.Kind.SEQUENCE)
                    for(Symbol seq : s.production) {
                        if (seq.kind == Symbol.Kind.EXPLICIT_ACTION
                                && seq instanceof org.apache.avro.io.parsing.Symbol.EnumLabelsAction) {
                            Symbol.EnumLabelsAction en = (Symbol.EnumLabelsAction)seq;
                            if (en.findLabel(in.getText()) >= 0) {
                                parser.pushSymbol(a.getSymbol(n));
                                return n;
                            }
                    }
                }
                n ++;
            }

            // Trying to guess correct branch using token type
            GUESSING: switch(token) {
                case START_ARRAY:

                    n = a.findLabel("array");
                    if (n < 0)
                        throw error("array");

                    parser.pushSymbol(a.getSymbol(n));
                    break;

                case VALUE_NUMBER_FLOAT:

                    final String floats[] = { "float", "double" };
                    for(String b : floats) {
                        n = a.findLabel(b);
                        if (n >= 0) {
                            parser.pushSymbol(a.getSymbol(n));
                            break GUESSING;
                        }
                    }

                    throw error("float or double");

                case VALUE_NUMBER_INT:

                    final String integers[] = { "int","long","float","double" };
                    for(String b : integers) {
                        n = a.findLabel(b);
                        if (n >= 0) {
                            parser.pushSymbol(a.getSymbol(n));
                            break GUESSING;
                        }
                    }

                    throw error("int, long, float or double");

                case VALUE_FALSE:
                case VALUE_TRUE:

                    n = a.findLabel("boolean");
                    if (n < 0)
                        throw error("boolean");

                    parser.pushSymbol(a.getSymbol(n));
                    break;

                case VALUE_STRING:

                    final String nanCandidates[] = { "float", "double" };
                    n = a.findLabel("string");
                    if (n < 0 && in.getText().equals("NaN"))  {
                        // Try to substitute NaN

                        for(String nn : nanCandidates) {
                            if ((n = a.findLabel(nn)) >= 0) {
                                parser.pushSymbol(a.getSymbol(n));
                                break GUESSING;
                            }
                        }
                        throw error("string (is NaN, but no string, double or float branches found)");
                    }
                    else if (n < 0) {
                        // Try to make double of this string
                        String s = in.getText();
                        try {
                            Double.parseDouble(s);
                        } catch (NumberFormatException e) {
                            throw error("string (not looks like number) and no string branch found");
                        }

                        // String could be converted to double; Try integer as well
                        try {
                            Integer.parseInt(s);
                            if ((n = a.findLabel("int")) >= 0) {
                                parser.pushSymbol(a.getSymbol(n));
                                break;
                            }
                        } catch (NumberFormatException e) {}

                        // Now try to find float or double (same as nanCandidates just by coincident
                        for(String nn : nanCandidates) {
                            if ((n = a.findLabel(nn)) >= 0) {
                                parser.pushSymbol(a.getSymbol(n));
                                break GUESSING;
                            }
                        }
                        throw error("string and also looks like number but no string nor numeric branches found");
                    }
                    else
                        parser.pushSymbol(a.getSymbol(n));

                    break;

                default:
                    throw error("start-union");
            }
            return n;
        }
    }

    @Override
    public Symbol doAction(Symbol input, Symbol top) throws IOException {


        if (top instanceof Symbol.FieldAdjustAction) {
            Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
            String name = fa.fname;
            if (currentReorderBuffer != null) {
                List<JsonElement> node = currentReorderBuffer.savedFields.get(name);
                if (node != null) {
                    currentReorderBuffer.savedFields.remove(name);
                    currentReorderBuffer.origParser = in;
                    in = makeParser(node);
                    return null;
                }
            }
            if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
                do {
                    String fn = in.getText();
                    in.nextToken();
                    if (name.equals(fn)) {
                        return null;
                    } else {
                        if (currentReorderBuffer == null) {
                            currentReorderBuffer = new ReorderBuffer();
                        }
                        currentReorderBuffer.savedFields.put(fn, getVaueAsTree(in));
                    }
                } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
                throw new AvroTypeException("Expected field name not found: " + fa.fname);
            }
        } else if (top == Symbol.FIELD_END) {
            if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
                in = currentReorderBuffer.origParser;
                currentReorderBuffer.origParser = null;
            }
        } else if (top == Symbol.RECORD_START) {
            if (in.getCurrentToken() == JsonToken.START_OBJECT) {
                in.nextToken();
                reorderBuffers.push(currentReorderBuffer);
                currentReorderBuffer = null;
            } else {
                throw error("record-start");
            }
        } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
            if (in.getCurrentToken() == JsonToken.END_OBJECT) {
                in.nextToken();
                if (top == Symbol.RECORD_END) {
                    if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
                        throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
                    }
                    currentReorderBuffer = reorderBuffers.pop();
                }
            } else {
                throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
            }
        } else {
            throw new AvroTypeException("Unknown action symbol " + top);
        }
        return null;
    }

    private static class JsonElement {
        public final JsonToken token;
        public final String value;
        public JsonElement(JsonToken t, String value) {
            this.token = t;
            this.value = value;
        }

        public JsonElement(JsonToken t) {
            this(t, null);
        }
    }


    private static List<JsonElement> getVaueAsTree(JsonParser in) throws IOException {
        int level = 0;
        List<JsonElement> result = new ArrayList<JsonElement>();
        do {
            JsonToken t = in.getCurrentToken();
            switch (t) {
                case START_OBJECT:
                case START_ARRAY:
                    level++;
                    result.add(new JsonElement(t));
                    break;
                case END_OBJECT:
                case END_ARRAY:
                    level--;
                    result.add(new JsonElement(t));
                    break;
                case FIELD_NAME:
                case VALUE_STRING:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT:
                case VALUE_TRUE:
                case VALUE_FALSE:
                case VALUE_NULL:
                    result.add(new JsonElement(t, in.getText()));
                    break;
            }
            in.nextToken();
        } while (level != 0);
        result.add(new JsonElement(null));
        return result;
    }

    private JsonParser makeParser(final List<JsonElement> elements) throws IOException {
        return new JsonParser() {
            int pos = 0;

            @Override
            public ObjectCodec getCodec() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setCodec(ObjectCodec c) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public JsonToken nextToken() throws IOException {
                pos++;
                return elements.get(pos).token;
            }

            @Override
            public JsonParser skipChildren() throws IOException {
                int level = 0;
                do {
                    switch(elements.get(pos++).token) {
                        case START_ARRAY:
                        case START_OBJECT:
                            level++;
                            break;
                        case END_ARRAY:
                        case END_OBJECT:
                            level--;
                            break;
                    }
                } while (level > 0);
                return this;
            }

            @Override
            public boolean isClosed() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getCurrentName() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public JsonStreamContext getParsingContext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public JsonLocation getTokenLocation() {
                throw new UnsupportedOperationException();
            }

            @Override
            public JsonLocation getCurrentLocation() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getText() throws IOException {
                return elements.get(pos).value;
            }

            @Override
            public char[] getTextCharacters() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getTextLength() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getTextOffset() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public Number getNumberValue() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public NumberType getNumberType() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getIntValue() throws IOException {
                return Integer.parseInt(getText());
            }

            @Override
            public long getLongValue() throws IOException {
                return Long.parseLong(getText());
            }

            @Override
            public BigInteger getBigIntegerValue() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public float getFloatValue() throws IOException {
                return Float.parseFloat(getText());
            }

            @Override
            public double getDoubleValue() throws IOException {
                return Double.parseDouble(getText());
            }

            @Override
            public BigDecimal getDecimalValue() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] getBinaryValue(Base64Variant b64variant)
                throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public JsonToken getCurrentToken() {
                return elements.get(pos).token;
            }
        };
    }

    private AvroTypeException error(String type) {

        String val;
        String fld;
        String loc;
        String cntx;
        String lastToken;
        try {
            val = in.getText();
        }
        catch (Exception e) {
            val = "*UNKNOWN*";
        }

        try {
            fld = in.getCurrentName();
        }
        catch (Exception e) {
            fld = "*UNKNOWN*";
        }

        try {
            loc = in.getCurrentLocation().toString();
        }
        catch (Exception e) {
            loc = "*UNKNOWN*";
        }

        try {
            lastToken = in.getLastClearedToken().toString();
        }
        catch (Exception e) {
            lastToken = "*UNKNOWN*";
        }

        try {
            cntx = in.getParsingContext().getCurrentName();
        }
        catch (Exception e) {
            cntx = "*UNKNOWN*";
        }

        return new AvroTypeException("Expected " + type +
            ". Got " + in.getCurrentToken() + " (" + val + ") at field '" + fld + "'" +
            ". Location: '" + loc + "', last token = '" + lastToken + "', context: '" + cntx + "'");
    }
}

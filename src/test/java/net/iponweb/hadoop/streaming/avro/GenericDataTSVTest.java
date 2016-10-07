package net.iponweb.hadoop.streaming.avro;

import junit.framework.Assert;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

public class GenericDataTSVTest {

    private final String sc1 = "{\"fields\" : [ { \"name\" : \"x\", \"type\" : \"int\" }, { \"name\" : \"y\", \"type\" : \"string\" }, " +
        "{ \"name\" : \"z\", \"type\" : \"null\" }, { \"name\" : \"a\", \"type\" : { \"type\" : \"array\", \"items\" : \"int\"} }" +
        "], \"type\" : \"record\", \"name\" : \"log_record\"}";

    private final String sc2 = "{\"fields\" : [ { \"name\" : \"x\", \"type\" : \"float\"}, { \"name\" : \"y\", \"type\" : \"double\" }, " +
        "{ \"name\" : \"x2\", \"type\" : \"float\"}, { \"name\" : \"y2\", \"type\" : \"double\" }, " +
        "{ \"name\" : \"x3\", \"type\" : \"float\"}, { \"name\" : \"y3\", \"type\" : \"double\" }, " +
        "{ \"name\" : \"x4\", \"type\" : \"float\"}, { \"name\" : \"y4\", \"type\" : \"double\" }" +
        "], \"type\" : \"record\", \"name\" : \"log_record2\"}";

    private final String sc3 = "{\"fields\" : [ { \"name\" : \"x\", \"type\" : [ \"string\", \"null\" ] }, " +
        "{ \"name\" : \"x2\", \"type\" : [ \"null\", \"string\" ] }, " +
        "{ \"name\" : \"a\", \"type\" : [ { \"type\" : \"array\", \"items\" : \"int\" }, \"null\" ] }, " +
        "{ \"name\" : \"a2\", \"type\" : [ { \"type\" : \"array\", \"items\" : \"int\" },  \"null\" ] }, " +
        "{ \"name\" : \"a3\", \"type\" : [ \"null\", { \"type\" : \"array\", \"items\" : \"int\" } ] }, " +
        "{ \"name\" : \"a4\", \"type\" : [ \"null\", { \"type\" : \"array\", \"items\" : \"int\" } ] }" +
        "], \"type\" : \"record\", \"name\" : \"log_record3\"}";

    private final String sc4 = "{\"fields\" : [ { \"name\" : \"x\", \"type\" : \"int\" }, " +
        "{ \"name\" : \"a\", \"type\" : { \"type\" : \"array\", \"items\" : \"string\"} }" +
        "], \"type\" : \"record\", \"name\" : \"log_record4\"}";

    private Parser p = new Parser();
    private GenericDataTSV gd = new GenericDataTSV();

    @Test
    public void testGetDatum() throws Exception {

        // checking int, string, null and array of ints

        Schema s1 = p.parse(sc1);

        GenericData.Record r1 = gd.getDatum("25\twtf\t\t[1,3,4]", s1);
        GenericData.Record r2 = new GenericData.Record(s1);
        r2.put("x", 25);
        r2.put("y", "wtf");
        GenericData.Array<Integer> a = new GenericData.Array<Integer>(3, Schema.createArray(Schema.create(Schema.Type.INT)));
        a.add(1);
        a.add(3);
        a.add(4);
        r2.put("a", a);

        Assert.assertEquals(r1, r2);
    }

    @Test
    public void testGetDatum1() throws Exception {

        // checking floats and doubles - including int, +inf, -inf, nan

        Schema s2 = p.parse(sc2);

        GenericData.Record r1 = gd.getDatum("0.345621\tNaN\t+Inf\t1.0\tnan\tInf\tNAN\t-inf", s2);
        GenericData.Record r2 = new GenericData.Record(s2);
        r2.put("x", 0.345621f);
        r2.put("y", Double.NaN);
        r2.put("x2", Float.POSITIVE_INFINITY);
        r2.put("y2", 1.0);
        r2.put("x3", Float.NaN);
        r2.put("y3", Double.POSITIVE_INFINITY);
        r2.put("x4", Float.NaN);
        r2.put("y4", Double.NEGATIVE_INFINITY);

        Assert.assertNotNull(r1);
        Assert.assertNotNull(r2);
        Assert.assertEquals(r1, r2);
    }

    @Test
    public void testGetDatum2() throws Exception {
        // checking unions

        Schema s3 = p.parse(sc3);

        GenericData.Record r1 = gd.getDatum("wtf\t\t[1,3,7]\t\t\t[1,3,7]", s3);
        GenericData.Record r2 = new GenericData.Record(s3);
        r2.put("x","wtf");
        r2.put("x2","");
        GenericData.Array<Integer> a2 = new GenericData.Array<Integer>(3,Schema.createArray(Schema.create(Schema.Type.INT)));
        a2.add(1);
        a2.add(3);
        a2.add(7);
        r2.put("a", a2);
        r2.put("a4", a2);

        Assert.assertNotNull(r1);
        Assert.assertNotNull(r2);
        Assert.assertEquals(r1, r2);
    }

    @Test
    public void testToString() throws Exception {

        Schema s1 = p.parse(sc1);
        GenericData.Record r2 = new GenericData.Record(s1);
        r2.put("x", 25);
        r2.put("y", "wtf");
        GenericData.Array<Integer> a = new GenericData.Array<Integer>(3, Schema.createArray(Schema.create(Schema.Type.INT)));
        a.add(1);
        a.add(3);
        a.add(4);
        r2.put("a", a);

        String tsv = gd.toString(r2);

        Assert.assertNotNull(tsv);
        Assert.assertEquals("25\twtf\t\t[1, 3, 4]", tsv);

    }

    @Test
    public void testToString1() throws Exception {

        Schema s4 = p.parse(sc4);
        GenericData.Record r2 = new GenericData.Record(s4);
        r2.put("x", 25);
        GenericData.Array<String> a = new GenericData.Array<String>(3, Schema.createArray(Schema.create(Schema.Type.STRING)));
        a.add("Zaphod Beeblebrox");
        a.add("Ford Prefect");
        a.add("Trillian McMillan");
        r2.put("a", a);

        String tsv = gd.toString(r2);
        Assert.assertNotNull(tsv);
        //Assert.assertEquals("25	[\"Zaphod Beeblebrox\",\"Ford Prefect\",\"Trillian McMillan\"]", tsv);

    }

    @Test
    public void testToString2() throws Exception {

        Schema s2 = p.parse(sc2);
        GenericData.Record r2 = new GenericData.Record(s2);
        r2.put("x",0.345621f);
        r2.put("y",Double.NaN);
        r2.put("x2", Float.POSITIVE_INFINITY);
        r2.put("y2", 1.0);
        r2.put("x3", Float.NaN);
        r2.put("y3", Double.POSITIVE_INFINITY);
        r2.put("x4", Float.NaN);
        r2.put("y4", Double.NEGATIVE_INFINITY);

        String tsv = gd.toString(r2);
        Assert.assertNotNull(tsv);
        Assert.assertEquals("0.345621\tNaN\tInfinity\t1.0\tNaN\tInfinity\tNaN\t-Infinity",tsv);
    }

    @Test
    public void testToString3() throws Exception {

        Schema s2 = p.parse(sc2);
        GenericData.Record r2 = new GenericData.Record(s2);
        r2.put("x",0.345621f);
        r2.put("y",Double.NaN);
        r2.put("x2", Float.POSITIVE_INFINITY);
        r2.put("y2", 1.0);
        r2.put("x3", Float.NaN);
        r2.put("y3", Double.POSITIVE_INFINITY);
        r2.put("x4", Float.NaN);
        r2.put("y4", Double.NEGATIVE_INFINITY);

        String tsv = gd.toString(r2,1,5);
        Assert.assertNotNull(tsv);
        Assert.assertEquals("NaN\tInfinity\t1.0\tNaN\tInfinity",tsv);
    }
}
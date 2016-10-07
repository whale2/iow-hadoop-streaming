package net.iponweb.hadoop.streaming.avro;


import junit.framework.Assert;
import net.iponweb.hadoop.streaming.dummyReporter;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroInOutFormatsTest {

    private final String schema = "{\"fields\" : [ { \"name\" : \"x\", \"type\" : \"int\" }, { \"name\" : \"y\", \"type\" : \"string\" }, " +
        "{ \"name\" : \"z\", \"type\" : \"null\" }, { \"name\" : \"a\", \"type\" : { \"type\" : \"array\", \"items\" : \"int\"} }" +
        "], \"type\" : \"record\", \"name\" : \"log_record\"}";

    private Schema.Parser p = new Schema.Parser();
    private GenericDataTSV gd = new GenericDataTSV();
    private static Path workDir = new Path("file:///tmp/iow-hadoop-streaming-" + Thread.currentThread().getId());
    private static JobConf defaultConf = new JobConf();
    private static String fname = "avroastexttest";
    private static String fname2 = "avroasjsontest";
    private static Path file = new Path(workDir, fname);
    private static Path file2 = new Path(workDir, fname2);

    private static String tsv = "25\twtf\t\t[1, 3, 4]";
    private static String json = "{\"x\": 25, \"y\": \"wtf\", \"z\": null, \"a\": [1, 3, 4]}";

    @Before
    public void setup() {

        defaultConf.set("iow.streaming.output.schema", schema);
        defaultConf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_000005_0");
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(workDir.toUri()));
    }

    @Test
    public void testAvroAsTextFmt() throws IOException {

        AvroAsTextOutputFormat outfmt = new AvroAsTextOutputFormat();
        FileOutputFormat.setOutputPath(defaultConf, file);
        RecordWriter<Text, NullWritable> writer = outfmt.getRecordWriter(file.getFileSystem(defaultConf),
                defaultConf, fname, new dummyReporter());

        writer.write(new Text(tsv), NullWritable.get());
        writer.close(null);

        FileInputFormat.setInputPaths(defaultConf, FileOutputFormat.getTaskOutputPath(defaultConf, fname +
            AvroOutputFormat.EXT));
        AvroAsTextInputFormat informat = new AvroAsTextInputFormat();
        RecordReader<Text, Text> reader = informat.getRecordReader(informat.getSplits(defaultConf, 1)[0],
                defaultConf, new dummyReporter());

        Text k = new Text();
        Text v = new Text();

        reader.next(k, v);
        Assert.assertEquals("read back tsv", tsv, k.toString() + "\t" + v.toString());
    }

    @Test
    public void testAvroAsJsonFmt() throws IOException {

        AvroAsJsonOutputFormat outfmt = new AvroAsJsonOutputFormat();
        FileOutputFormat.setOutputPath(defaultConf, file2);
                RecordWriter<Text, NullWritable> writer = outfmt.getRecordWriter(file2.getFileSystem(defaultConf),
                defaultConf, fname2, new dummyReporter());

        writer.write(new Text(json), NullWritable.get());
        writer.close(null);

        FileInputFormat.setInputPaths(defaultConf, FileOutputFormat.getTaskOutputPath(defaultConf, fname2 +
            AvroOutputFormat.EXT));
        AvroAsJsonInputFormat informat = new AvroAsJsonInputFormat();
        RecordReader<Text, Text> reader = informat.getRecordReader(informat.getSplits(defaultConf, 1)[0],
                defaultConf, new dummyReporter());

        Text k = new Text();
        Text v = new Text();

        reader.next(k, v);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode n0 = mapper.readTree(k.toString());
        JsonNode n1 = mapper.readTree(json);
        Assert.assertEquals("read back json", n0, n1);
    }

}

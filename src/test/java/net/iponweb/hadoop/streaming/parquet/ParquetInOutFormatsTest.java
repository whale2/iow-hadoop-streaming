package net.iponweb.hadoop.streaming.parquet;

import net.iponweb.hadoop.streaming.dummyReporter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ParquetInOutFormatsTest {

    private final String schema = "message test { required int32 x; required binary y; optional binary z; repeated int32 a; }";

    private static String tsv = "25\twtf\t\t[1, 3, 4]";
    private static String json = "{\"x\": 25, \"y\": \"wtf\", \"z\": null, \"a\": [1, 3, 4]}";

    private static JobConf defaultConf = new JobConf();
    private static Path workDir = new Path("file:///tmp/iow-hadoop-streaming-" + Thread.currentThread().getId());
    private static String fname = "parquetastexttest";
    private static String fname2 = "parquetasjsontest";
    private static Path file = new Path(workDir, fname);
    private static Path file2 = new Path(workDir, fname2);


    @Before
    public void setup() {

        defaultConf.set("iow.streaming.output.schema", schema);
        defaultConf.set("mapreduce.task.partition", "0");
        defaultConf.set("mapreduce.task.attempt.id", "attempt_200707121733_0003_m_000005_0");
        defaultConf.set("parquet.read.support.class","net.iponweb.hadoop.streaming.parquet.GroupReadSupport");
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.deleteDirectory(new File(workDir.toUri()));
    }

    @Test
    public void testParquetAsTextFmt() throws IOException {

        ParquetAsTextOutputFormat outfmt = new ParquetAsTextOutputFormat();
        FileOutputFormat.setOutputPath(defaultConf, file);
        String outpath = FileOutputFormat.getTaskOutputPath(defaultConf, "wtf").toString();
        defaultConf.set("mapreduce.task.output.dir", outpath);
        RecordWriter<Text, Text> writer = outfmt.getRecordWriter(file.getFileSystem(defaultConf),
                defaultConf, fname, new dummyReporter());

        writer.write(new Text(tsv), null);
        writer.close(null);

        FileInputFormat.setInputPaths(defaultConf, outpath + "/" + fname + "-m-00000.parquet");
        ParquetAsTextInputFormat informat = new ParquetAsTextInputFormat();
        RecordReader<Text, Text> reader = informat.getRecordReader(informat.getSplits(defaultConf, 1)[0],
                defaultConf, new dummyReporter());

        Text k = new Text();
        Text v = new Text();

        reader.next(k, v);
        Assert.assertEquals("read back tsv", tsv, k.toString() + "\t" + v.toString());
    }


    @Test
    public void testParquetAsJsonFmt() throws IOException {

        ParquetAsJsonOutputFormat outfmt = new ParquetAsJsonOutputFormat();
        FileOutputFormat.setOutputPath(defaultConf, file2);
        String outpath = FileOutputFormat.getTaskOutputPath(defaultConf, "wtf").toString();
        defaultConf.set("mapreduce.task.output.dir", outpath);
        RecordWriter<Text, Text> writer = outfmt.getRecordWriter(file.getFileSystem(defaultConf),
                defaultConf, fname2, new dummyReporter());

        writer.write(new Text(json), null);
        writer.close(null);

        FileInputFormat.setInputPaths(defaultConf, outpath + "/" + fname2 + "-m-00000.parquet");
        ParquetAsJsonInputFormat informat = new ParquetAsJsonInputFormat();
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

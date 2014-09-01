iow-hadoop-streaming
====================

Set of hadoop input/output formats for use in combination with hadoop streaming
(http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html)

Input formats that read Avro or Parquet files and convert them to text or json and 
then feed into streaming MapRed job as input.
Output formats for converting text or json output of streaming MapRed jobs and store it in Avro or Parquet.
Output format that can write to many files based on record prefix and can be combined with above output formats



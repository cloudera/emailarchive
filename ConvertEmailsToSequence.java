
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConvertEmailsToSequence extends Configured
  implements Tool {
  
  static class SequenceFileMapper extends MapReduceBase
      implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    
    private JobConf conf;
    
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    @Override
    public void map(NullWritable key, BytesWritable value,
        OutputCollector<Text, BytesWritable> output, Reporter reporter)
        throws IOException {
      
      String filename = conf.get("map.input.file");
      output.collect(new Text(filename), value);
    }
    
  }
 
  @Override
  public int run(String[] args) throws IOException {
	  JobConf jobConf = new JobConf(ConvertEmailsToSequence.class);
		jobConf.setJobName("SequenceCreator");
		FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    jobConf.setInputFormat(WholeFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
//    SequenceFileOutputFormat.setOutputCompressionType(jobConf, CompressionType.BLOCK);
//    SequenceFileOutputFormat.setCompressOutput(jobConf, true); 
//    SequenceFileOutputFormat.setOutputCompressorClass(jobConf, GzipCodec.class); 
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(BytesWritable.class);

    jobConf.setMapperClass(SequenceFileMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);

    JobClient.runJob(jobConf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ConvertEmailsToSequence(), args);
    System.exit(exitCode);
  }
}

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.poi.hsmf.MAPIMessage;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;

	
public class SearchEmail extends Configured implements Tool{
	
	public static String keyToSearch = null;
	
	
	public static class Map extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> {
		public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String keyText = key.toString();
			InputStream input = new ByteArrayInputStream(value.getBytes());
			MAPIMessage msg = new MAPIMessage(input);
			try {
				if (msg.getRecipientEmailAddress().contains("sales")) {
					//write the file directly to local file system NFS mount seems to be the best option
					String fileName = keyText.substring(keyText.lastIndexOf("/")+1);
				    FileOutputStream fos = new FileOutputStream("/tmp/" + fileName);
				    fos.write(value.getBytes());  
				    fos.close();
				}
					  
			} catch (ChunkNotFoundException e) {
				e.printStackTrace();
			} catch (IOException ioe) { 
				
			} 
			
		}
	
	}
	
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(SearchEmail.class);
		conf.setJobName("SearchEmail");
		conf.setMapperClass(Map.class);
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(NullOutputFormat.class);
		conf.setNumMapTasks(1);
		conf.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SearchEmail(), args);
		System.exit(exitCode);  
		}
	
	
	

}

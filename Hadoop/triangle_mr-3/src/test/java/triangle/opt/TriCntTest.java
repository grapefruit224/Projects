package triangle.opt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class TriCntTest {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] params = {"src/test/resources/wiki-task2.txt"};
		
		ToolRunner.run(conf, new TriCnt(), params);
	}
}

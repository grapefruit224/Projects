package triangle.task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import triangle.task1.TriCnt;

public class TriCntTest {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.job.reduces", 3);
		
		String[] params = {"src/test/resources/test.txt"};
		
		ToolRunner.run(conf, new TriCnt(), params);
	}
}

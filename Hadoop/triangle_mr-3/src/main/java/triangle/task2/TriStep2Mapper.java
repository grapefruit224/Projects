package triangle.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TriStep2Mapper extends Mapper<Object, Text, Text, IntPairWritable>{
	
	Text ok = new Text();
	IntPairWritable ov = new IntPairWritable();
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());
		int u_count = Integer.parseInt(st.nextToken());
		int v_count = Integer.parseInt(st.nextToken());
		
		ok.set(u + "\t" + v);
		ov.set(u_count, v_count);
		
		context.write(ok, ov);
		
	}
}

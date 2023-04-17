package triangle.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import triangle.task2.IntPairWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class TriStep1Mapper extends Mapper<Object, Text, IntWritable, IntPairWritable> {
	
	IntWritable ok = new IntWritable();
	IntPairWritable ov = new IntPairWritable();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer st = new StringTokenizer(value.toString());
		
		int u = Integer.parseInt(st.nextToken());
		int v = Integer.parseInt(st.nextToken());

		ok.set(u);
		ov.set(u, v);
		context.write(ok, ov);
		ok.set(v);
		context.write(ok, ov);

	}
}

package triangle.opt_2.opt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TriStep2MapperForWedges extends Mapper<IntPairWritable, IntWritable, IntPairWritable, IntWritable>{
	
	@Override
	protected void map(IntPairWritable key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}

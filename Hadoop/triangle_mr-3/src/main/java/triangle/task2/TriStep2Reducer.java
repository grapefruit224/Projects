package triangle.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TriStep2Reducer extends Reducer<Text, IntPairWritable, IntWritable, IntWritable>{

	IntWritable ok = new IntWritable();
	IntWritable ov = new IntWritable();

	
	@Override
	protected void reduce(Text key, Iterable<IntPairWritable> values,
			Context context) throws IOException, InterruptedException {
		String keypair = key.toString();
		int u = Integer.parseInt(keypair.split("\t")[0]);
		int v = Integer.parseInt(keypair.split("\t")[1]);
		int u_count = 0;
		int v_count = 0;

		for(IntPairWritable v1 : values){
			String value1pair = v1.toString();

			int u1_count = Integer.parseInt(value1pair.split("\t")[0]);
			int v1_count = Integer.parseInt(value1pair.split("\t")[1]);
			for(IntPairWritable v2 : values) {
				String value2pair = v2.toString();

				int u2_count = Integer.parseInt(value2pair.split("\t")[0]);
				int v2_count = Integer.parseInt(value2pair.split("\t")[1]);
				if(u1_count == 0) {
					u_count = u2_count;
					v_count = v1_count;
				}
				if(v1_count == 0) {
					u_count = u1_count;
					v_count = v2_count;
				}
			}

		}


		if (u_count > v_count || (u_count == v_count && u > v))
		{
			ok.set(v);
			ov.set(u);
		}
		else{
			ok.set(u);
			ov.set(v);
		}

		context.write(ok, ov);




	}
}

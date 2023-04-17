package triangle.opt_2.opt;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TriStep1Reducer extends Reducer<IntWritable, IntWritable, IntPairWritable, IntWritable>{
	IntPairWritable ok = new IntPairWritable();

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		ArrayList<Integer> neighbors = new ArrayList<Integer>();
		for(IntWritable v : values) {
			neighbors.add(v.get());
		}
		
		for(int u : neighbors) {
			for(int v : neighbors) {
				if (u < v) {
					ok.set(u, v);
					context.write(ok, key);
				}
			}
		}
		/*
		System.out.println("Start!");
		for(IntWritable u : values){
			int uu = u.get();
			System.out.println(uu);
			System.out.println();
			for(IntWritable v : values){
				int vv = v.get();
				System.out.println(vv);
				if(uu < vv){
					ok.set(uu, vv);
					context.write(ok, key);
				}
			}
			System.out.println();
		}
		 */
	}
}

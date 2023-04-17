package triangle.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TriStep1Reducer extends Reducer<IntWritable, IntPairWritable, Text, Text>{
	Text ok = new Text();
	Text ov = new Text();
	IntWritable first = new IntWritable();
	IntWritable last = new IntWritable();
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntPairWritable> values,
			Context context) throws IOException, InterruptedException {
		
		ArrayList<String> neighbors = new ArrayList<String>();
		for(IntPairWritable v : values) {
			neighbors.add(v.toString());
		}

		for(String v : neighbors){
			first.set(Integer.parseInt(v.split("\t")[0]));
			last.set(Integer.parseInt(v.split("\t")[1]));
			ok.set(v);

			if(key.compareTo(first) == 0){ // pair 중 앞에 있는 것의 degree이면 앞에 저장
				ov.set(neighbors.size() + "\t" + "0");
				context.write(ok, ov);
			}
			if(key.compareTo(last) == 0){ // pair 중 뒤에 있는 것의 degree이면 뒤에 저장
				ov.set("0" + "\t" + neighbors.size());
				context.write(ok, ov);
			}
		}
		
	}
}

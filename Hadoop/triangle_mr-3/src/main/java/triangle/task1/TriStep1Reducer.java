package triangle.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeSet;

public class TriStep1Reducer extends Reducer<IntWritable, IntWritable, Text, Text>{
	Text ok = new Text();
	Text ov = new Text();
	
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		TreeSet<Integer> treeSet = new TreeSet<Integer>(); // 중복이 없는 집합

		ok.set("" + key.get());

		for(IntWritable v : values) {
			treeSet.add(v.get()); // 집합에 넣어 중복값을 없앤다
		}

		Iterator<Integer> iter = treeSet.iterator();
		while(iter.hasNext()){
			ov.set(""+iter.next()); // 그 set을 write함!
			context.write(ok, ov);
		}
		
	}
}

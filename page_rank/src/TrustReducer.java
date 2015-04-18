import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class TrustReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)
	throws IOException, InterruptedException {
	//Implement
		Node M = null;
		double sum = 0.0;
		for(NodeOrDouble items: values){
			if(items.isNode()){
				//if item is node case
				M = items.getNode();
			}
			else{
				//if item is double case
				sum = sum + items.getDouble();
			}
		}
		M.setPageRank(sum);
		context.write(key,M);
    }
}

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class TrustMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {

	//Implement
		//For every node parsed from input file, increment NODECOUNTER 
    	context.getCounter(Counters.NODECOUNT).increment(1);
    	context.write(new IntWritable(value.nodeid),new NodeOrDouble(value));

    	if(value.outgoingSize() == 0){
    		//handle dead end node case here
    		long incrementValue = (long)(value.getPageRank() * 1000000000);
    		context.getCounter(Counters.MASSCOUNT).increment(incrementValue);
    	}
    	else{
    		//handle regular case here
    		double p_massperlink = value.getPageRank() / value.outgoingSize();
    		for(Integer outlink : value){
			context.write(new IntWritable(outlink),new NodeOrDouble(p_massperlink));
		}

    	}

		
    }
}

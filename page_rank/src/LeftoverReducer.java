import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class LeftoverReducer extends Reducer<IntWritable, Node, IntWritable, Node> {
    public static double alpha = 0.85;
    public void reduce(IntWritable nid, Iterable<Node> Ns, Context context) throws IOException, InterruptedException {
	//Implement
	Configuration conf = context.getConfiguration();
    long leftover = Long.parseLong(conf.get("leftover"));
    System.out.println("##*##alue of leftover: "+leftover);
    long size = Long.parseLong(conf.get("size"));
    System.out.println("##*##alue of size: "+size);
    Iterator<Node> it = Ns.iterator();
    Node N = it.next();
    double oldrank = N.getPageRank();
    double newrank = (alpha/size) + ((1-alpha)*((((double)leftover/1000000000.0)/size) + oldrank ));
    N.setPageRank(newrank);
    context.write(nid,N);
    }
}

package net.sf.hadoopconnector.treasury;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mongodb.hadoop.util.MongoConfigUtil;

/**
 * 
 * 
 * 
 * To run on server,
 * at command line, issue : hadoop jar MongoDBHadoopDemo-1.0-SNAPSHOT.jar
 * net.sf.hadoopconnector.treasury.TreasuryYieldRunner
 * 
 * 
 */
public class TreasuryYieldRunner extends Configured implements Tool {

	static {

		// To run on in standalone mode, uncomment the line below
		//Configuration.addDefaultResource("hadoop-local.xml");
		Configuration.addDefaultResource("mongo-treasury_yield.xml");
	}

	public int run(String args[]) throws Exception {
		Configuration conf = getConf();

		// Temporary to get of the following error.
		// Exception in thread "main" java.lang.IllegalArgumentException: Unable
		// to calculate input splits: need to login
		// at
		// com.mongodb.hadoop.util.MongoSplitter.calculateUnshardedSplits(MongoSplitter.java:106)
		conf.setBoolean("mongo.input.split.create_input_splits", false);

		Job job = new Job(conf, "Treasury Yield");
		job.setJarByClass(getClass());
		job.setReducerClass(MongoConfigUtil.getReducer(conf));
		job.setMapperClass(MongoConfigUtil.getMapper(conf));
		job.setOutputFormatClass(MongoConfigUtil.getOutputFormat(conf));
		job.setOutputKeyClass(MongoConfigUtil.getOutputKey(conf));
		job.setMapOutputKeyClass(MongoConfigUtil.getMapperOutputKey(conf));

		job.setMapOutputValueClass(MongoConfigUtil.getMapperOutputValue(conf));
		job.setInputFormatClass(MongoConfigUtil.getInputFormat(conf));
		// submit job and wait for completion.
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new TreasuryYieldRunner(), args);
	}

	@SuppressWarnings("unused")
	private static final Log LOG = LogFactory
			.getLog(TreasuryYieldRunner.class);

}

package com.xxx.algo.ad.mr;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.xxx.hadoop.rcfile.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Hadoop Task Base Processor
 */

public abstract class AbstractProcessor implements Tool {
	protected static final Log LOG = LogFactory.getLog(AbstractProcessor.class
			.getName());

	public AbstractProcessor() {
		setupOptions();
	}

	public Configuration getConf() {
		return _conf;
	}

	public void setConf(Configuration conf) {
		_conf = conf;
	}

	public int run(String[] args) throws Exception {
		parseArgv(args);

		Configuration conf = getConf();
		if (_columnNum != null) {
			int cn = Integer.parseInt(_columnNum);
			RCFileOutputFormat.setColumnNumber(conf, cn);
		}
		if (_compressed) { //
			conf.set("mapred.compress.map.output", "true");
			conf.set("mapred.output.compress", "true");
			if (_columnNum != null) { // rcfile + gzip
				conf.set("mapred.map.output.compression.codec",	"org.apache.hadoop.io.compress.GzipCodec");
				conf.set("mapred.output.compression.codec",	"org.apache.hadoop.io.compress.GzipCodec");
			} else {  // text + lzo
				conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzoCodec");
		    	conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
			}
		}

		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(job, _input);
		FileOutputFormat.setOutputPath(job, new Path(_output));

		if (_numReduce != null) {
			int numReduce = Integer.parseInt(_numReduce);
			job.setNumReduceTasks(numReduce);
		} else {
			job.setNumReduceTasks(0);
		}

		configJob(job);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Implement this function to specify your task's unique configuration
	 * 
	 * @param job
	 */
	protected abstract void configJob(Job job);

	@SuppressWarnings("static-access")
	private Option createOption(String name, String desc, String argName,
			int max, boolean required) {
		return OptionBuilder.withArgName(argName).hasArgs(max)
				.withDescription(desc).isRequired(required).create(name);
	}

	@SuppressWarnings("static-access")
	private Option createBoolOption(String name, String desc) {
		return OptionBuilder.withDescription(desc).create(name);
	}

	protected void setupOptions() {
		Option input = createOption("input", "HDFS path for input files", "path",
				Integer.MAX_VALUE, true);
		Option output = createOption("output",
				"HDFS path for output directory", "path", 1, true);
		Option numReduce = createOption("numReduce", "Optional.", "spec", 1, false);
		Option rcfile = createOption("columns", "columns for rcfile output", "spec", 1, false);
		Option compressed = createBoolOption("compressed", "should output gzip compressed"); //
		Option help = createBoolOption("help", "print this help message");

		_options.addOption(input).addOption(output).addOption(numReduce)
				.addOption(rcfile).addOption(compressed).addOption(help);
	}

	protected void parseArgv(String[] args) {
		CommandLine cmdLine = null;
		try {
			cmdLine = new BasicParser().parse(_options, args);
		} catch (Exception oe) {
			exitUsage(true);
		}

		if (cmdLine != null) {
			if (cmdLine.hasOption("help")) {
				exitUsage(true);
			}
			_input = cmdLine.getOptionValue("input");
			_output = cmdLine.getOptionValue("output");
			_numReduce = cmdLine.getOptionValue("numReduce");
			_columnNum = cmdLine.getOptionValue("columns");
			_compressed = cmdLine.hasOption("compressed") ? true : false; //
		} else {
			exitUsage(true);
		}

		if (_input == null) {
			fail("Required argument: -input <path>");
		}
		if (_output == null) {
			fail("Required argument: -output <path>");
		}
	}

	protected static void exitUsage(boolean generic) {
		System.out
				.println("Usage: $HADOOP_HOME/bin/hadoop jar jarFile Launcher Processor [Options]");
		System.out.println("Options:");
		System.out.println("  -input    <path>     inputs, seperated by comma");
		System.out.println("  -output   <path>     output directory");
		System.out.println("  -numReduce <num>  optional");
		System.out.println("  -columns   <num>  optional column number of output rcfile");
		System.out.println("  -compressed , rcfile+gzip or text+lzo, default=false"); //
		System.out.println("  -help");
		System.out.println();
		if (generic) {
			GenericOptionsParser.printGenericCommandUsage(System.out);
		}
		fail("");
	}

	private static void fail(String message) {
		System.err.println(message);
		System.exit(-1);
	}

	protected Configuration _conf = new Configuration();
	protected Options _options = new Options();

	protected String _input = null;
	protected String _output = null;
	protected String _columnNum = null;
	protected String _numReduce = null;
	protected Boolean _compressed = false;
}

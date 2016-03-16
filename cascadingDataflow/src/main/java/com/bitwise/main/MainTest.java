package com.bitwise.main;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.bitwise.functions.SampleAggregator;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.text.DateParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

public class MainTest {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Properties properties = appplyGenericOptionParser(args, conf);

		AppProps.setApplicationJarClass(properties, Main.class);
		AppProps.addApplicationTag(properties, "CascadingDataFlow");
		AppProps.addApplicationTag(properties, "project2");
		AppProps.addApplicationTag(properties, "Cascading-Dataflow");

		String inputPath = args[0];
		String intermediateOutputPath = args[1] + "inter";
		String finalOutputPath = args[2];

		// create source scheme and tap
		DateType dateType = new DateType("yyyyMMdd");
		Fields inputFields = new Fields("store", "product", "sold", "returned", "date").applyTypes(String.class,
				String.class, Integer.class, Integer.class, dateType);

		Scheme inputScheme = new TextDelimited(inputFields, true, ",");
		Hfs inTap = new Hfs(inputScheme, inputPath);

		Pipe mainPipe = new Pipe("main");
		
		Fields groupFields = new Fields("store", "product");
		Pipe intermediate = new GroupBy(mainPipe, groupFields);
		intermediate = new Each(intermediate,DebugLevel.VERBOSE ,new Debug(true));
		//Pipe output = new SumBy(mainPipe,groupFields,new Fields("sold"),new Fields("sum"),Integer.class);

		// create sink scheme and tap
		Fields finalOutputFields = new Fields("store", "product", "sold", "returned", "date").applyTypes(String.class,
				String.class, Integer.class, Integer.class, dateType);
		Scheme finalOutputScheme = new TextDelimited(finalOutputFields, true, ",");
		Hfs finalOutTap = new Hfs(finalOutputScheme, finalOutputPath, SinkMode.REPLACE);

		FlowDef flowDef = FlowDef.flowDef();
		flowDef.setName("Cascading Dataflow");
		flowDef.addSource(mainPipe, inTap);
		// flowDef.addTailSink(intermediate, intermediateOutTap);
		flowDef.addTailSink(intermediate, finalOutTap);
		flowDef.setDebugLevel(DebugLevel.VERBOSE);

		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		Flow wcFlow = flowConnector.connect(flowDef);
		wcFlow.complete();

	}

	private static Properties appplyGenericOptionParser(String[] args, Configuration conf) throws IOException {
		String[] otherArgs = { "" };
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String argsString = "";

		for (String arg : otherArgs) {
			argsString = argsString + " " + arg;
		}
		System.out.println("After processing arguments are:" + argsString);
		Properties properties = new Properties();
		properties.putAll(conf.getValByRegex(".*"));
		return properties;
	}
}

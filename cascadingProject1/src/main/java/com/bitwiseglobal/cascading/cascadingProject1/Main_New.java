package com.bitwiseglobal.cascading.cascadingProject1;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main_New {
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		String[] otherArgs={""};
		otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		// print other args
		String argsString = "";
		for (String arg : otherArgs) {
		argsString = argsString + " " + arg;
		}
		System.out.println("After processing arguments are:" + argsString);
		Properties properties = new Properties();
		properties.putAll(conf.getValByRegex(".*"));
		
		//Properties properties = new Properties();
		
		FlowConnector flowConnector  = new Hadoop2MR1FlowConnector(properties);
		AppProps.setApplicationJarClass(properties, Main_New.class);
		AppProps.addApplicationTag(properties, "cascading-propject");
		AppProps.addApplicationTag(properties, "project1");
		AppProps.setApplicationName(properties, "FileFormatting");
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		//Use Schemes
		Fields inputFields = new Fields("KeyField","SplitStringField","DateField","NumericField","DateField1")
								.applyTypes(String.class,String.class,String.class,Double.class,String.class);
		
		Scheme inputScheme  = new TextDelimited(inputFields,true,",","\"");
		
		Fields outputFields = new Fields("KeyField","SplitString1","SplitString2","DateField","NumericField","DateField1")
								.applyTypes(String.class,String.class,String.class,String.class,Double.class,String.class);
		
		Scheme outputScheme = new TextDelimited(outputFields,true,"|");
		
		Hfs inTap = new Hfs(inputScheme,inputPath);
		Hfs outTap = new Hfs(outputScheme,outputPath,SinkMode.REPLACE);
		Hfs trapTap = new Hfs(new TextDelimited(true,"|"),outputPath+"/trap",SinkMode.REPLACE);
		
		//Hfs intermediateTap = new Hfs(new TextDelimited(true,"|"),outputPath+"/intermediate",SinkMode.REPLACE);
		Pipe mainPipe = new Pipe("main");
		
		//Assertion to trap null values for date
		//mainPipe = new Each(mainPipe,new Fields("DateField"),AssertionLevel.STRICT,new AssertNotNull());
		//Assertion to check format of input date
		//mainPipe = new Each(mainPipe, new Fields("DateField"),AssertionLevel.STRICT,new AssertMatches("([0-9]{4})-([0-9]{2})-([0-9]{2})"));
		
		//Assertion to check valid double
		mainPipe = new Each( mainPipe,AssertionLevel.VALID, new DoubleValidator());
		
		Fields splitStringField = new Fields("SplitStringField");
		Fields declaredFields = new Fields("SplitString1","SplitString2");
		String inputFormat = "MM/dd/yyyy";
		String outputFormat = "yyyy-MM-dd";
		
		mainPipe = new Each(mainPipe, splitStringField, new SplitString(declaredFields),Fields.SWAP);
		Pipe intermediatePipe = new Pipe("intermediate",mainPipe);
		
		//DateFormatter: Class to convert date in another format
		Fields fieldSelector = new Fields("DateField","DateField1");
		mainPipe = new Each(mainPipe,fieldSelector,AssertionLevel.STRICT,new DateFormatAssertion(inputFormat));
		mainPipe = new Each(mainPipe,fieldSelector,
				new DateFormatter(fieldSelector,inputFormat,outputFormat),
				Fields.REPLACE);
		//Connect taps and pipes
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.setName("cascadingProject");
		flowDef.addSource(mainPipe, inTap);
		flowDef.addTailSink(mainPipe, outTap);
		flowDef.addTrap(mainPipe, trapTap);
		
		//flowDef.addTailSink(intermediatePipe, intermediateTap);
		
		Flow wcFlow = flowConnector.connect(flowDef);
		wcFlow.complete();

	}
}

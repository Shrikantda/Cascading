package com.bitwise.main;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.bitwise.functions.MinMaxAggregator;
import com.bitwise.functions.NormalizeAssembly;
import com.bitwise.functions.ScanAssembly;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = { "" };
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		String argsString = "";

		for (String arg : otherArgs) {
			argsString = argsString + " " + arg;
		}
		System.out.println("After processing arguments are:" + argsString);
		Properties properties = new Properties();
		properties.putAll(conf.getValByRegex(".*"));

		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
		AppProps.setApplicationJarClass(properties, Main.class);
		AppProps.addApplicationTag(properties, "CascadingDataFlow");
		AppProps.addApplicationTag(properties, "project2");
		AppProps.addApplicationTag(properties, "Cascading-Dataflow");

		String inputPath = args[0];
		String intermediateOutputPath = args[1];
		String finalOutputPath = args[2];

		String scanOutputPath = finalOutputPath;
		// Create input scheme and taps
		Fields inputFields = new Fields("acct_id", "sor_id", "snap_dt", "sor_src_id", "dept_grp_cd", "dept_id",
				"acct_ofcr_cd", "ent_prod_cd", "sor_prod_cd", "cmtmt_rpmt_src_cnctrn_cd", "sic_cd",
				"rcvry_stat_type_cd", "intl_agt_id", "extrnl_agt_id", "fin_agt_id", "orig_sor_id", "orig_acct_id",
				"acct_rcvry_file_num", "acct_recvd_dt", "extrnl_agt_asgnmt_dt", "acct_st_ind", "debt_sales_btch_num",
				"acct_inactivation_dt", "acct_int_rt", "corrnc_rcpnt_inhibit_ind", "last_cntct_dt", "next_cntct_dt",
				"acct_last_pmt_dt", "acct_last_pmt_amt", "acct_last_int_calcn_dt", "form_1098_acct_sttlmt_cd",
				"snap_end_dt", "bdw_publn_id").applyTypes(String.class, String.class, String.class, String.class,
						String.class, String.class, String.class, String.class, String.class, String.class,
						String.class, String.class, String.class, String.class, String.class, String.class,
						String.class, String.class, String.class, String.class, String.class, String.class,
						String.class, String.class, String.class, String.class, String.class, String.class,
						String.class, String.class, String.class, String.class, String.class);

		Scheme inputScheme = new TextDelimited(inputFields, true, ",");
		Hfs inTap = new Hfs(inputScheme, inputPath);

		Pipe mainPipe = new Pipe("main");

		// Part 1: Retain
		Fields retainFields = new Fields("acct_id", "sor_id", "extrnl_agt_id", "extrnl_agt_asgnmt_dt", "snap_dt",
				"snap_end_dt", "sor_src_id");
		Pipe retainPipe = new Retain(mainPipe, retainFields);

		// Part 2: Sort (GroupBy)
		Fields groupingFields = new Fields("acct_id", "sor_id", "extrnl_agt_id", "extrnl_agt_asgnmt_dt");
		Pipe sortPipe = new GroupBy(retainPipe, groupingFields);
		
		//Part 3: Rollup (Min and Max)
		sortPipe = new Every(sortPipe, Fields.ALL, new MinMaxAggregator(retainFields), Fields.RESULTS);
		Pipe secondPart = new Pipe("second",sortPipe);
		//Part 4: Sort (GroupBy and Sort)
		groupingFields = new Fields("acct_id","sor_id");
		Fields sortFields = new Fields("snap_dt");
		Fields subAssemblyInputFields = new Fields("extrnl_agt_id");
		Fields subAssemblyOutputFields = new Fields("extrnl_agt_id_prev","extrnl_agt_id_orig");
		Fields passThroughFields = new Fields("acct_id","sor_id","extrnl_agt_asgnmt_dt","snap_dt","snap_end_dt","sor_src_id");
		
		SubAssembly outputPipes = new ScanAssembly(secondPart,groupingFields,sortFields,
				subAssemblyInputFields,subAssemblyOutputFields,passThroughFields);
		//Pipe intermediateOutputPipe = new Pipe("first",outputPipes.getTails()[0]);
		
		Pipe scanOutputPipe = new Pipe("ss",outputPipes.getTails()[0]);
		Pipe normalizedPipe = normalize(scanOutputPipe);
		
		//Part 5:  Create intermediate scheme and taps and write intermediate output
		Fields intermediateOutputFields = new Fields("acct_id", "sor_id","extrnl_agt_id_prev","extrnl_agt_id_orig", 
				"extrnl_agt_asgnmt_dt","snap_dt", "snap_end_dt","sor_src_id","extrnl_agt_id",
				"adjd_extrnl_agt_id","bdw_publn_id");
		Scheme intermediateOutputScheme = new TextDelimited(intermediateOutputFields, true, ",");
		Hfs intermediateOutTap = new Hfs(intermediateOutputScheme, intermediateOutputPath, SinkMode.REPLACE);
		
		
		Fields scanOutputFields = new Fields("acct_id", "sor_id","extrnl_agt_id_prev","extrnl_agt_id_orig", "extrnl_agt_asgnmt_dt",
				"snap_dt", "snap_end_dt","sor_src_id").applyTypes(String.class, String.class, String.class, String.class,
						String.class, String.class,String.class,String.class);
		Scheme scanOutputScheme = new TextDelimited(scanOutputFields, true, ",");
		Hfs scanOutTap = new Hfs(scanOutputScheme, scanOutputPath, SinkMode.REPLACE);
		
		
		
		
		//Part 6:
		
		// Create output scheme and taps
		// Fields finalOutputFields = new Fields("sor_id", "acct_id", "snap_dt",
		// "extrnl_agt_id", "adjd_extrnl_agt_id",
		// "snap_end_dt", "bdw_publn_id").applyTypes(String.class, String.class,
		// String.class, String.class,
		// String.class, String.class, String.class);
		// Scheme finalOutputScheme = new TextDelimited(finalOutputFields, true,
		// ",");
		// Hfs finalOutTap = new Hfs(finalOutputScheme, finalOutputPath,
		// SinkMode.REPLACE);

		// Create flow and execute
		FlowDef flowDef = FlowDef.flowDef();
		flowDef.setName("Cascading Dataflow");
		flowDef.addSource(mainPipe, inTap);
		flowDef.addTailSink(normalizedPipe, intermediateOutTap);
		//flowDef.addTailSink(scanOutputPipe, scanOutTap);

		Flow wcFlow = flowConnector.connect(flowDef);
		wcFlow.complete();

	}
	
	public static Pipe normalize(Pipe scanPipe){
		Fields inputFields = new Fields("extrnl_agt_id_prev","extrnl_agt_id_orig","extrnl_agt_asgnmt_dt",
				"snap_dt");
		Fields outputFields = new Fields("acct_id", "sor_id","extrnl_agt_id_prev","extrnl_agt_id_orig", 
				"extrnl_agt_asgnmt_dt","snap_dt", "snap_end_dt","sor_src_id","extrnl_agt_id",
				"adjd_extrnl_agt_id","bdw_publn_id");
		
		Pipe normalizedPipe = new NormalizeAssembly(scanPipe,inputFields,outputFields); 
		return normalizedPipe;
	}
}

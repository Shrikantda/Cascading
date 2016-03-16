package com.bitwise.main;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class AggregateEx1 {

	public static void main(String[] args) {

		// create source Tap
		Fields inputFields = new Fields("acct_id", "sor_id", "snap_dt", "sor_src_id", "dept_grp_cd", "dept_id",
				"acct_ofcr_cd", "ent_prod_cd", "sor_prod_cd", "cmtmt_rpmt_src_cnctrn_cd", "sic_cd",
				"rcvry_stat_type_cd", "intl_agt_id", "extrnl_agt_id", "fin_agt_id", "orig_sor_id", "orig_acct_id",
				"acct_rcvry_file_num", "acct_recvd_dt", "extrnl_agt_asgnmt_dt", "acct_st_ind", "debt_sales_btch_num",
				"acct_inactivation_dt", "acct_int_rt", "corrnc_rcpnt_inhibit_ind", "last_cntct_dt", "next_cntct_dt",
				"acct_last_pmt_dt", "acct_last_pmt_amt", "acct_last_int_calcn_dt", "form_1098_acct_sttlmt_cd",
				"snap_end_dt", "bdw_publn_id");
		Class[] types = new Class[] { String.class, String.class, String.class, String.class, String.class,
				String.class, String.class, String.class, String.class, String.class, String.class, String.class,
				String.class, String.class, String.class, String.class, String.class, String.class, String.class,
				String.class, String.class, String.class, String.class, String.class, String.class, String.class,
				String.class, String.class, String.class, String.class, String.class, String.class, String.class };
		DelimitedParser delimitedParser = new DelimitedParser(",", null, types, true, false);
		Scheme sourceScheme = new TextDelimited(inputFields, true, delimitedParser);
		Tap sourceTap = new Hfs(sourceScheme, "config/resources/input/small.txt");

		// create Pipe
		Pipe pipe = new Pipe("pipe");

		// retain some fields
		Fields retainFields = new Fields("acct_id", "sor_id", "snap_dt", "sor_src_id", "extrnl_agt_id",
				"extrnl_agt_asgnmt_dt", "snap_end_dt");
		pipe = new Retain(pipe, retainFields);

		// group and perform custom aggregation
		Fields groupFields = new Fields("acct_id", "sor_id", "extrnl_agt_id", "extrnl_agt_asgnmt_dt");
		pipe = new GroupBy(pipe, groupFields);
		pipe = new Every(pipe, new Fields("snap_dt","snap_end_dt"), new MyAggregators(new Fields("snap_dt","snap_end_dt")), Fields.ALL);

		// create Sink Tap
		Fields outputFields = retainFields;
		Class[] sinkTypes = new Class[] { String.class, String.class, String.class, String.class,
				String.class, String.class };
		DelimitedParser delimitedParser1 = new DelimitedParser(",", null, sinkTypes, true, false);
		Scheme sinkScheme = new TextDelimited( new Fields("acct_id", "sor_id", "snap_dt", "extrnl_agt_id",
				"extrnl_agt_asgnmt_dt", "snap_end_dt"), false, delimitedParser1);
		Tap sinkTap = new Hfs(sinkScheme, "config/resources/output/aggr_out", SinkMode.REPLACE);

		// create Flow
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, AggregateEx1.class);
		FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

		FlowDef flowDef = FlowDef.flowDef().setName("AggregateEx1");
		flowDef.addSource(pipe, sourceTap);
		flowDef.addTailSink(pipe, sinkTap);

		Flow flow = flowConnector.connect(flowDef);
		flow.complete();
	}

}

package com.bitwise.main;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class MyAggregators extends BaseOperation implements Aggregator {

	static String minDate = null;
	static String maxDate = null;
	static boolean checkEmpty = true;
	String acct_id, sor_id, sor_src_id, extrnl_agt_id, extrnl_agt_asgnmt_dt;

	ArrayList<String> al = new ArrayList<String>();

	public MyAggregators(Fields retainFields) {
		super(retainFields.size(), retainFields);
	}

	@Override
	public void start(FlowProcess arg0, AggregatorCall arg1) {
		minDate = null;
		maxDate = null;
	}

	@Override
	public void aggregate(FlowProcess arg0, AggregatorCall arg1) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyymmdd");
		Date currentForMaxCheck, currentForMinCheck, min, max;
		TupleEntry tuple = arg1.getArguments();
		String formin = tuple.getString("snap_dt");
		String formax = tuple.getString("snap_end_dt");

		try {
			if (checkEmpty == true) {

				minDate = formin;
				maxDate = formax;
				checkEmpty = false;
			} else {
				currentForMinCheck = formatter.parse(formin);
				currentForMaxCheck = formatter.parse(formax);
				min = formatter.parse(minDate);
				max = formatter.parse(maxDate);

				if (currentForMinCheck.compareTo(min) <= 0)
					minDate = formin;
				if (currentForMaxCheck.compareTo(max) >= 0)
					maxDate = formax;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		acct_id = tuple.getString("acct_id");
		sor_id = tuple.getString("sor_id");
		sor_src_id = tuple.getString("sor_src_id");
		extrnl_agt_id = tuple.getString("extrnl_agt_id");
		extrnl_agt_asgnmt_dt = tuple.getString("extrnl_agt_asgnmt_dt");

//		System.out.println(acct_id + "   " + sor_id + "  " + al + "   " + extrnl_agt_id + "  " + extrnl_agt_asgnmt_dt);
	}

	@Override
	public void complete(FlowProcess arg0, AggregatorCall arg1) {

		Tuple result = new Tuple();
		result.add(acct_id);
		result.add(sor_id);
		result.add(minDate);
		result.add(sor_src_id);
		result.add(extrnl_agt_id);
		result.add(extrnl_agt_asgnmt_dt);
		result.add(maxDate);

		arg1.getOutputCollector().add(result);

		minDate = null;
		maxDate = null;
		checkEmpty = true;
		al.clear();

	}

}

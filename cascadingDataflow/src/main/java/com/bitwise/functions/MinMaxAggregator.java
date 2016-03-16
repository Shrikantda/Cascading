package com.bitwise.functions;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class MinMaxAggregator extends BaseOperation implements Aggregator {

	static String minDate = null;
	static String maxDate = null;
	static boolean checkEmpty = true;
	String acct_id,sor_id,extrnl_agt_id,extrnl_agt_asgnmt_dt,sor_src_id;
	
	public MinMaxAggregator() {
		
	}
	
	public MinMaxAggregator(Fields retainFields) {
		super(retainFields.size(), retainFields);
	}
	@Override
	public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		
		
		acct_id=null;
		sor_id=null;
		extrnl_agt_id=null;
		extrnl_agt_asgnmt_dt=null;
		sor_src_id=null;
		checkEmpty=true;
	}

	@Override
	public void aggregate(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyymmdd");
		Date currentForMaxCheck, currentForMinCheck, min, max;
		TupleEntry tuple = aggregatorCall.getArguments();
		String formin = tuple.getString("snap_dt");
		String formax = tuple.getString("snap_end_dt");

		acct_id=tuple.getString("acct_id");
		
		sor_id=tuple.getString("sor_id");
		extrnl_agt_id=tuple.getString("extrnl_agt_id");
		extrnl_agt_asgnmt_dt=tuple.getString("extrnl_agt_asgnmt_dt");
		sor_src_id=tuple.getString("sor_src_id");
		
		
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
			
			e.printStackTrace();
		}
	}

	@Override
	public void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		
		Tuple result = new Tuple();
		result.addString(acct_id);
		result.addString(sor_id);
		result.addString(extrnl_agt_id);
		result.addString(extrnl_agt_asgnmt_dt);
		result.addString(minDate);
		result.addString(maxDate);
		result.addString(sor_src_id);
		
		aggregatorCall.getOutputCollector().add(result);
		
		minDate = null;
		maxDate = null;
	}

}

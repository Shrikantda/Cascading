package com.bitwise.functions;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NormalizeFunction extends BaseOperation implements Function {

	public static final String DEFAULT_PUBLN_ID = "20131003111111";
	public NormalizeFunction(){
		
	}
	
	public NormalizeFunction(Fields declaredFields){
		super(4,declaredFields);
	}
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry entry = functionCall.getArguments();
		Tuple result = Tuple.size(11);
		String extrnl_agt_id_prev=entry.getString(0);
		String extrnl_agt_id_orig = entry.getString(1);
		String extrnl_agt_asgnmt_dt = entry.getString(2);
		String snap_dt = entry.getString(3);

				
		result.setString(8, extrnl_agt_id_orig);
		result.setString(9, extrnl_agt_id_orig);
		result.setString(10, DEFAULT_PUBLN_ID);
		result.setString(5, snap_dt);
		
		
		functionCall.getOutputCollector().add(result);
		Tuple tup = new Tuple();
		
		
		if(!snap_dt.equals(extrnl_agt_asgnmt_dt)){
			System.out.println("Previous and original:"+extrnl_agt_id_prev+" "+extrnl_agt_id_orig);
			Tuple tuple = Tuple.size(11);
			tuple.setString(8, extrnl_agt_id_prev);
			tuple.setString(9, extrnl_agt_id_orig);
			tuple.setString(10, DEFAULT_PUBLN_ID);
			tuple.setString(5, extrnl_agt_asgnmt_dt);
			functionCall.getOutputCollector().add(tuple);
		}
		
		
		
		
	}

}

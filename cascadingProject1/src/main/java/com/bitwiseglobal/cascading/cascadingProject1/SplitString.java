package com.bitwiseglobal.cascading.cascadingProject1;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class SplitString extends BaseOperation implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public SplitString(Fields fieldDeclaration){
		super(1,fieldDeclaration);
	}
	
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		TupleEntry arguments = functionCall.getArguments();
		Tuple result = new Tuple();
		String original = arguments.getString(0).trim();
		String[] fields = original.split(";");
		
		if(fields.length==0){
			result.add(null);
			result.add(null);
		}
		else if(fields.length==1){
			result.add(fields[0]);
			result.add(null);
		}
		else{
			result.add(fields[0]);
			result.add(fields[1]);
		}
		
		
		functionCall.getOutputCollector().add(result);
		
	}

}

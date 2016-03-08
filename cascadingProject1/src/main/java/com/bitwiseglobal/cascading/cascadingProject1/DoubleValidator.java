package com.bitwiseglobal.cascading.cascadingProject1;

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.operation.assertion.BaseAssertion;
import cascading.tuple.TupleEntry;

public class DoubleValidator extends BaseAssertion implements ValueAssertion {

	
	public DoubleValidator(){
		super("Argument '%s' is not a valid Double in tuple: %s");
	}
	@Override
	public void doAssert(FlowProcess flowProcess, ValueAssertionCall assertionCall) {
		
		TupleEntry entry = assertionCall.getArguments();
		String val = entry.getString(3);

		//String value = Double.toString(val);
		
		if(val!=null && val.length()>0){
			try{
				Double.parseDouble(val);
			}catch(NumberFormatException ex){
				fail(val,entry.getTuple().print());
			}
		}
		else{
			fail(val,entry.getTuple().print());
		}
		
		
	}

}

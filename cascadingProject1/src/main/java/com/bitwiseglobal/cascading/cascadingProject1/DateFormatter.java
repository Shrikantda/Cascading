package com.bitwiseglobal.cascading.cascadingProject1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DateFormatter extends BaseOperation implements Function {

	
	public DateFormatter(Fields fieldDeclaration){
		super(1,fieldDeclaration);
	}
	
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
		
		TupleEntry arguments = functionCall.getArguments();
		Tuple result = new Tuple();
		String resultDate="";
	  
		String oldFormat = "yyyy-MM-dd";
		String newFormat = "MM/dd/yyyy";
		
		String originalDate = arguments.getString(0);
		
		SimpleDateFormat sdfSource = new SimpleDateFormat(oldFormat);
	    try {
			Date date = sdfSource.parse(originalDate);
			SimpleDateFormat sdfDestination = new SimpleDateFormat(newFormat);
			resultDate = sdfDestination.format(date);
		} catch (ParseException e) {
			
			e.printStackTrace();
		}
	    
		result.add(resultDate);
		functionCall.getOutputCollector().add(result);
	}

}

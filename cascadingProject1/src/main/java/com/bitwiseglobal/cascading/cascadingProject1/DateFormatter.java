package com.bitwiseglobal.cascading.cascadingProject1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DateFormatter extends BaseOperation<Tuple> implements Function<Tuple> {

	private static final long serialVersionUID = 1L;
	String inputFormat;
	String outputFormat;
	public DateFormatter(Fields fieldDeclaration,String inputFormat,String outputFormat){
		super(2,fieldDeclaration);
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
	}
	
	@Override
	public void prepare(FlowProcess flowProcess, OperationCall<Tuple> call){
		call.setContext(Tuple.size(2));
	}
	@Override
	public void operate(FlowProcess flowProcess, FunctionCall<Tuple> functionCall) {
		
		TupleEntry arguments = functionCall.getArguments();
		Tuple result = new Tuple();
		String resultDate="";
		String originalDate = arguments.getString(0);
		String originalDate2 = arguments.getString(1);
		
		if(originalDate!=null && originalDate.length()>0){
			resultDate = convertDate(originalDate,inputFormat,outputFormat);
			result.add(resultDate);
		}
		else{
			result.add(null);
		}
	    
		if(originalDate2!=null && originalDate2.length()>0){
			resultDate = convertDate(originalDate2,inputFormat,outputFormat);
			result.add(resultDate);
		}
		else{
			result.add(null);
		}
		
		functionCall.getOutputCollector().add(result);
	}

	public String convertDate(String inputDate,String inputF, String outputF){
		String resultDate="";
		SimpleDateFormat sdfSource = new SimpleDateFormat(inputF,Locale.ENGLISH);
		sdfSource.setLenient(false);
	    try {
	    	
			Date date = sdfSource.parse(inputDate.trim());
			SimpleDateFormat sdfDestination = new SimpleDateFormat(outputF,Locale.ENGLISH);
			sdfDestination.setLenient(false);
			resultDate = sdfDestination.format(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return resultDate;	
	}
	
	@Override
	public void cleanup(FlowProcess flowProcess, OperationCall<Tuple> call){
		call.setContext(null);
	}
}

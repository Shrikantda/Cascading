package com.bitwiseglobal.cascading.cascadingProject1;

import java.util.Date;
import java.util.Locale;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.operation.assertion.BaseAssertion;
import cascading.tuple.TupleEntry;

public class DateFormatAssertion extends BaseAssertion implements ValueAssertion{

	
	String inputFormat;
	public DateFormatAssertion(){
		super(1);
	}
	
	public DateFormatAssertion(String inputFormat){
		super(1,"Argument %s failed Date Format Assertion, in tuple: '%s'");
		this.inputFormat = inputFormat;
	}
	
	@Override
	public void doAssert(FlowProcess flowProcess, ValueAssertionCall assertionCall) {
		
		TupleEntry input = assertionCall.getArguments();
		String dateInput1 = input.getString(0);
	
		String dateInput2 = input.getString(1);
		
		DateFormat formatter = new SimpleDateFormat(inputFormat);
		formatter.setLenient(false);
		if(dateInput1 != null && dateInput1.length()>0){
			
			try{
				Date date = formatter.parse(dateInput1);
			}catch(ParseException ex){
				fail(dateInput1,input.getTuple().print());
			}
		}
		
		if(dateInput2 != null && dateInput2.length()>0){
			
			try{		
				Date date = formatter.parse(dateInput2);
			}catch(ParseException ex){
				fail(dateInput1,input.getTuple().print());
			}
		}
		
	}
}

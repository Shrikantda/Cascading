package com.bitwiseglobal.cascading.cascadingProject1;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

public class DateFormatterTest extends CascadingTestCase{

	private static final long serialVersionUID = 1L;
	String inputFormat = "MM/dd/yyyy";
	String outputFormat = "yyyy-MM-dd";
	
	@Test
	public void testValidDateResults(){
		
		Fields fields = new Fields("DateField","DateField1").applyTypes(String.class,String.class);
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("1/14/2016","5/16/2000");
		arguments[1] = new Tuple("5/23/2006","7/21/2004");
		arguments[2] = new Tuple("6/19/2014","8/15/2009");
		
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("2016-01-14","2000-05-16"));
		expectedResults.add(new Tuple("2006-05-23","2004-07-21"));
		expectedResults.add(new Tuple("2014-06-19","2009-08-15"));
		
		TupleListCollector collector = invokeFunction(new DateFormatter(fields,inputFormat,outputFormat),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
	
	
	@Test
	public void testForNull_Empty_Dates(){
		Fields fields = new Fields("DateField","DateField1").applyTypes(String.class,String.class);
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("","1/14/2016");
		arguments[1] = new Tuple("12/12/1989",null);
		arguments[2] = new Tuple("",null);
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple(null,"2016-01-14"));
		expectedResults.add(new Tuple("1989-12-12",null));
		expectedResults.add(new Tuple(null,null));
		
		TupleListCollector collector = invokeFunction(new DateFormatter(fields,inputFormat,outputFormat),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
	
	@Test(expected = NullPointerException.class)
	public void testForWrongFormat(){
		Fields fields = new Fields("DateField","DateField1").applyTypes(String.class,String.class);
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("1-14-2016","1/14/2016");
		arguments[1] = new Tuple("1989/12/12","12-12-1989");
		TupleListCollector collector = invokeFunction(new DateFormatter(fields,inputFormat,outputFormat),arguments,Fields.ALL);
		
	}
}

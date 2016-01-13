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
	
	@Test
	public void testValidDateResults(){
		
		Fields fields = new Fields("DateField").applyTypes(String.class);
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("2015-11-20");
		arguments[1] = new Tuple("1989-5-26");
		arguments[2] = new Tuple("2000-6-15");
		
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("11/20/2015"));
		expectedResults.add(new Tuple("05/26/1989"));
		expectedResults.add(new Tuple("06/15/2000"));
		
		TupleListCollector collector = invokeFunction(new DateFormatter(fields),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
	
	
	@Test(expected = NullPointerException.class)
	public void testForNullPointerException(){
		Fields fields = new Fields("DateField").applyTypes(String.class);
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("");
		arguments[1] = new Tuple("12/12/1989");
		
		TupleListCollector collector = invokeFunction(new DateFormatter(fields),arguments,Fields.ALL);
		
	}
}

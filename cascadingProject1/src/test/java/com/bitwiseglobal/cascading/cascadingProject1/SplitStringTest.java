package com.bitwiseglobal.cascading.cascadingProject1;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Test;

public class SplitStringTest extends CascadingTestCase {

	@Test
	public void testValidStrings(){
		Fields fields = new Fields("SplitStringField").applyTypes(String.class);
		Fields declaredFields = new Fields("SplitString1","SplitString2").applyTypes(String.class,String.class);
		
		Tuple[] arguments = new Tuple[3];
		arguments[0] = new Tuple("Test1;Test2");
		arguments[1] = new Tuple("Value1;Value2");
		arguments[2] = new Tuple("Val3;val4");
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("Test1","Test2"));
		expectedResults.add(new Tuple("Value1","Value2"));
		expectedResults.add(new Tuple("Val3","val4"));
		
		TupleListCollector collector = invokeFunction(new SplitString(declaredFields),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
	
	@Test
	public void testQuotedStrings(){
		Fields fields = new Fields("SplitStringField").applyTypes(String.class);
		Fields declaredFields = new Fields("SplitString1","SplitString2").applyTypes(String.class,String.class);
		Tuple[] arguments = new Tuple[2];
		arguments[0] = new Tuple("Test1;Test2,Test3");
		arguments[1] = new Tuple("Value1,Value4;Value2");
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("Test1","Test2,Test3"));
		expectedResults.add(new Tuple("Value1,Value4","Value2"));
		
		TupleListCollector collector = invokeFunction(new SplitString(declaredFields),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
	
	@Test
	public void testEmptyStrings(){
		Fields fields = new Fields("SplitStringField").applyTypes(String.class);
		Fields declaredFields = new Fields("SplitString1","SplitString2").applyTypes(String.class,String.class);
		Tuple[] arguments = new Tuple[4];
		arguments[0] = new Tuple("");
		arguments[1] = new Tuple("Value4,Value2;");
		arguments[2] = new Tuple(";TestValue4,TestValue2");
		arguments[3] = new Tuple("TestValue");
		
		ArrayList<Tuple> expectedResults = new ArrayList<Tuple>();
		expectedResults.add(new Tuple("",null));
		expectedResults.add(new Tuple("Value4,Value2",null));
		expectedResults.add(new Tuple("","TestValue4,TestValue2"));
		expectedResults.add(new Tuple("TestValue",null));
		
		TupleListCollector collector = invokeFunction(new SplitString(declaredFields),arguments,Fields.ALL);
		Iterator<Tuple> itr = collector.iterator();
		ArrayList<Tuple> results = new ArrayList<Tuple>();
		while(itr.hasNext()){
			results.add(itr.next());
		}
		
		assertEquals("Results not as expected",expectedResults,results);
	}
}

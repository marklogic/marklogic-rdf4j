package com.marklogic.sesame.functionaltests.util;

import java.util.ArrayList;
import java.util.List;

public class StatementList <Statement> {
	
	private List<Statement> sList;
	
	public StatementList(Statement st){
		sList = new ArrayList<Statement>();
		sList.add(st);
		
	}
	
	public void add(Statement st){
		sList.add(st);
		
	}
	
	public int size(){
		return sList.size();
	}
	
	public Statement get(int i){
		return sList.get(i);
		
	}

	

}

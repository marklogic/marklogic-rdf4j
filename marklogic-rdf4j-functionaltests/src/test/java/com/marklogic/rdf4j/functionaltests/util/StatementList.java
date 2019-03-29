/*
 * Copyright 2015-2019 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.rdf4j.functionaltests.util;

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

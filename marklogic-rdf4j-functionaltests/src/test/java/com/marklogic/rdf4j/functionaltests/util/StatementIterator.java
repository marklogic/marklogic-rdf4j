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

import java.util.Iterator;

import org.eclipse.rdf4j.model.Statement;

public class StatementIterator implements Iterator<Statement>{
	
	private StatementList<Statement> sL;
	private int index = 0;
	private int size ;
	
	public StatementIterator(StatementList<Statement> sL){
		this.sL =sL;
		this.size =sL.size();
	}

	@Override
	public boolean hasNext() {
		if(index < size)
			return true;
		return false;
	}

	@Override
	public Statement next() {
		Statement st =  sL.get(index);
		index ++;
		return st;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}
	
	
}

/*
 * Copyright 2015-2017 MarkLogic Corporation
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
package com.marklogic.semantics.rdf4j.client;

import org.eclipse.rdf4j.http.client.QueueCursor;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParser;
import org.eclipse.rdf4j.query.resultio.helpers.BackgroundTupleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * wrapper on RDF4J BackgroundTupleResult
 *
 * @author James Fuller
 */
class MarkLogicBackgroundTupleResult extends BackgroundTupleResult {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicBackgroundGraphResult.class);

    /**
     *  constructor
     *
     * @param parser
     * @param in
     */
    public MarkLogicBackgroundTupleResult(TupleQueryResultParser parser, InputStream in) {
        super(parser, in);
    }

    /**
     * constructor
     *
     * @param queue
     * @param parser
     * @param in
     */
    public MarkLogicBackgroundTupleResult(QueueCursor<BindingSet> queue, TupleQueryResultParser parser, InputStream in) {
        super(queue, parser, in);
    }

    /**
     * wrap exception to return false instead of throwing error, debug log
     *
     */
    @Override
    public boolean hasNext()
            throws QueryEvaluationException
    {
        try {
            return super.hasNext();
        }catch(Exception e){
            logger.info("MarkLogicBackgroundTupleResult hasNext() stream closed");
            return false;
        }
    }

    /**
     * wrap exception, debug log
     *
     */
    @Override
    protected void handleClose() throws QueryEvaluationException {
        try {
            super.handleClose();
        }catch(Exception e){
            logger.error("MarkLogicBackgroundTupleResult handleClose() stream closed exception",e);
            throw new QueryEvaluationException(e);
        }
    }
}

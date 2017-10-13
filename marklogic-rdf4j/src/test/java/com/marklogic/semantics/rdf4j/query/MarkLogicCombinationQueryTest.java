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
package com.marklogic.semantics.rdf4j.query;


import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.*;
import com.marklogic.semantics.rdf4j.MarkLogicRepositoryConnection;
import com.marklogic.semantics.rdf4j.Rdf4jTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * test MarkLogic combination query
 *
 * @author James Fuller
 */
public class MarkLogicCombinationQueryTest extends Rdf4jTestBase {

    private QueryManager qmgr;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected MarkLogicRepositoryConnection conn;
    protected ValueFactory f;

    @Before
    public void setUp() throws RepositoryException {
        logger.debug("setting up test");
        rep.initialize();
        f = rep.getValueFactory();
        conn = rep.getConnection();
        logger.info("test setup complete.");

        String tripleDocOne =

                "<semantic-document>\n" +
                        "<title>First Title</title>\n" +
                        "<size>100</size>\n" +
                        "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                        "<sem:triple><sem:subject>http://example.org/r9928</sem:subject>" +
                        "<sem:predicate>http://example.org/p3</sem:predicate>" +
                        "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</sem:object></sem:triple>" +
                        "</sem:triples>\n" +
                        "</semantic-document>";

        String tripleDocTwo =

                "<semantic-document>\n" +
                        "<title>Second Title</title>\n" +
                        "<size>500</size>\n" +
                        "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                        "<sem:triple><sem:subject>http://example.org/r9929</sem:subject>" +
                        "<sem:predicate>http://example.org/p3</sem:predicate>" +
                        "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</sem:object></sem:triple>" +
                        "</sem:triples>\n" +
                        "</semantic-document>";

        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();
        docMgr.write("/directory1/doc1.xml", new StringHandle().with(tripleDocOne));
        docMgr.write("/directory2/doc2.xml", new StringHandle().with(tripleDocTwo));
        qmgr = writerClient.newQueryManager();

    }

    @After
    public void tearDown()
            throws Exception {
        logger.debug("tearing down...");
        conn.close();
        conn = null;
        // TODO: Un-comment line when issue 811 gets resolved. https://github.com/marklogic/java-client-api/issues/811
        //rep.shutDown();
        rep = null;
        logger.info("tearDown complete.");

        XMLDocumentManager docMgr = writerClient.newXMLDocumentManager();

        docMgr.delete("/directory1/doc1.xml");
        docMgr.delete("/directory2/doc2.xml");
    }

    @Test
    public void testCombinationQuery() throws MalformedQueryException, RepositoryException, QueryEvaluationException {

        String query1 = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String query2 = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";

        // case one, rawcombined
        String combinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"First Title\"}}";
        String negCombinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"Second Title\"}}";

        RawCombinedQueryDefinition rawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        RawCombinedQueryDefinition negRawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

        MarkLogicBooleanQuery askQuery =conn.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
        askQuery.setConstrainingQueryDefinition(rawCombined);
        Assert.assertEquals(true, askQuery.evaluate());
        logger.debug("query: {}", query1);

        askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,query2);
        askQuery.setConstrainingQueryDefinition(rawCombined);
        Assert.assertEquals(false, askQuery.evaluate());
        
        askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
        askQuery.setConstrainingQueryDefinition(negRawCombined);
        Assert.assertEquals(false, askQuery.evaluate());
    }

    @Test
    public void testStructuredCombinedQuery() throws MalformedQueryException, RepositoryException, QueryEvaluationException {
        StructuredQueryBuilder qb = new StructuredQueryBuilder();
        QueryDefinition structuredDef = qb.build(qb.term("Second"));

        String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        MarkLogicBooleanQuery askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        askQuery.setConstrainingQueryDefinition(structuredDef);
        Assert.assertEquals(true, askQuery.evaluate());

        askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
        askQuery.setConstrainingQueryDefinition(structuredDef);
        Assert.assertEquals(false, askQuery.evaluate());
    }

    @Test
    public void testStringCombinationQuery() throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
        String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";

        MarkLogicBooleanQuery askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        askQuery.setConstrainingQueryDefinition(stringDef);
        Assert.assertEquals(true, askQuery.evaluate());

        askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
        askQuery.setConstrainingQueryDefinition(stringDef);
        Assert.assertEquals(false, askQuery.evaluate());
    }

    // https://github.com/marklogic/marklogic-sesame/issues/124
    @Test
    public void testStringCombinationQueryUnsetSet() throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
        String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";

        MarkLogicBooleanQuery askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        askQuery.setConstrainingQueryDefinition(null);
        Assert.assertEquals(true, askQuery.evaluate());

    }

    // https://github.com/marklogic/marklogic-sesame/issues/142
    @Test
    public void testStringCombinationQueryWithDefaults() throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
        String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";

        conn.setDefaultConstrainingQueryDefinition(stringDef);

        MarkLogicBooleanQuery askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        Assert.assertEquals(true, askQuery.evaluate());

        askQuery = conn.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
        askQuery.setConstrainingQueryDefinition(stringDef);
        Assert.assertEquals(false, askQuery.evaluate());

    }
}
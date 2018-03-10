/*
 * Copyright 2015-2018 MarkLogic Corporation
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
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * RDF4J API.
 */
package com.marklogic.semantics.rdf4j.client;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.semantics.*;
import com.marklogic.semantics.rdf4j.utils.Util;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.eclipse.rdf4j.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ForbiddenUserException;
import com.marklogic.client.Transaction;
import com.marklogic.client.impl.SPARQLBindingsImpl;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.semantics.rdf4j.MarkLogicRdf4jException;

/**
 * Internal class for interacting with Java Client API.
 */
public class MarkLogicClientImpl {

    private static final Logger logger = LoggerFactory.getLogger(MarkLogicClientImpl.class);

    private static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";

    private SPARQLRuleset[] ruleset;
    private Integer optimizeLevel;
    private QueryDefinition constrainingQueryDef;
    private GraphPermissions graphPerms;

    private SPARQLQueryManager sparqlManager;
    private GraphManager graphManager;

    private DatabaseClient databaseClient;

    private Util util = Util.getInstance();

    private String[] stringContexts;

    private Pattern[] patterns = new Pattern[]{Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">")};

    /**
     * Constructor initialized with connection parameters.
     *
     * @param host
     * @param port
     * @param user
     * @param password
     * @param auth
     */
    public MarkLogicClientImpl(String host, int port, String user, String password, String database, String auth) {
        setDatabaseClient(util.getClientBasedOnAuth(host, port, user, password, database, auth));
    }

    /**
     * set databaseclient
     *
     * @param databaseClient
     */
    public MarkLogicClientImpl(DatabaseClient databaseClient) {
        setDatabaseClient(databaseClient);
    }

    /**
     * set databaseclient and instantiate related managers.
     *
     * @param databaseClient
     */
    private void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        this.sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        this.graphManager = getDatabaseClient().newGraphManager();
    }

    /**
     * gets database client
     *
     * @return DatabaseClient
     */
    public DatabaseClient getDatabaseClient() {
        return this.databaseClient;
    }

    /**
     * Executes SPARQLQuery
     *
     * @param queryString
     * @param bindings
     * @param start
     * @param pageLength
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performSPARQLQuery(queryString, bindings, new InputStreamHandle(), start, pageLength, tx, includeInferred, baseURI);
    }

    /**
     * Executes SPARQLQuery with InputStreamHandle
     *
     * @param queryString
     * @param bindings
     * @param handle
     * @param start
     * @param pageLength
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }
        if (Util.notNull(ruleset) && includeInferred) {
            qdef.setRulesets(ruleset);
        }
        if (Util.notNull(getConstrainingQueryDefinition())) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        }
        qdef.setIncludeDefaultRulesets(includeInferred);
        if (Util.notNull(graphPerms)) {
            qdef.setUpdatePermissions(graphPerms);
        }
        if (Util.notNull(optimizeLevel)) {
            qdef.setOptimizeLevel(optimizeLevel);
        }
        if (pageLength > 0) {
            sparqlManager.setPageLength(pageLength);
        } else {
            sparqlManager.clearPageLength();
        }
        sparqlManager.executeSelect(qdef, handle, start, tx);
        return new BufferedInputStream(handle.get());
    }

    /**
     * Executes GraphQuery
     *
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performGraphQuery(queryString, bindings, new InputStreamHandle(), tx, includeInferred, baseURI);
    }

    /**
     * Executes GraphQuery with InputStreamHandle.
     *
     * @param queryString
     * @param bindings
     * @param handle
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     * @throws JsonProcessingException
     */
    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }
        if (Util.notNull(ruleset) && includeInferred) {
            qdef.setRulesets(ruleset);
        }
        if (Util.notNull(getConstrainingQueryDefinition())) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        }
        if (Util.notNull(graphPerms)) {
            qdef.setUpdatePermissions(graphPerms);
        }
        if (Util.notNull(optimizeLevel)) {
            qdef.setOptimizeLevel(optimizeLevel);
        }
        qdef.setIncludeDefaultRulesets(includeInferred);
        sparqlManager.executeDescribe(qdef, handle, tx);
        return new BufferedInputStream(handle.get());
    }

    /**
     * Executes BooleanQuery
     *
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     * @return
     */
    public boolean performBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }
        qdef.setIncludeDefaultRulesets(includeInferred);
        if (Util.notNull(optimizeLevel)) {
            qdef.setOptimizeLevel(optimizeLevel);
        }
        if (Util.notNull(ruleset) && includeInferred) {
            qdef.setRulesets(ruleset);
        }
        if (Util.notNull(getConstrainingQueryDefinition())) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
            qdef.setDirectory(getConstrainingQueryDefinition().getDirectory());
            qdef.setCollections(getConstrainingQueryDefinition().getCollections());
            qdef.setResponseTransform(getConstrainingQueryDefinition().getResponseTransform());
            qdef.setOptionsName(getConstrainingQueryDefinition().getOptionsName());
        }
        if (Util.notNull(graphPerms)) {
            qdef.setUpdatePermissions(graphPerms);
        }
        return sparqlManager.executeAsk(qdef, tx);
    }

    /**
     * Executes UpdateQuery
     *
     * @param queryString
     * @param bindings
     * @param tx
     * @param includeInferred
     * @param baseURI
     */
    public void performUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(queryString);
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }
        if (Util.notNull(ruleset) && includeInferred) {
            qdef.setRulesets(ruleset);
        }
        if (Util.notNull(graphPerms)) {
            qdef.setUpdatePermissions(graphPerms);
        }
        if (Util.notNull(optimizeLevel)) {
            qdef.setOptimizeLevel(optimizeLevel);
        }
        qdef.setIncludeDefaultRulesets(includeInferred);
        sparqlManager.clearPageLength();
        try {
            sparqlManager.executeUpdate(qdef, tx);
        } catch (ForbiddenUserException e) {
            throw new RepositoryException(e.getMessage());
        }
    }

    /**
     * Executes merge of triples from File
     *
     * @param file
     * @param baseURI
     * @param dataFormat
     * @param tx
     * @param contexts
     * @throws RDFParseException
     */
    // performAdd
    public void performAdd(File file, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) throws RDFParseException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(18);
        List<Future<?>> futures = new ArrayList<>();

        if (dataFormat.supportsContexts()) {
            //Quads

            RDFParser parser = Rio.createParser(dataFormat);
            parseQuads(tx, executor, futures, parser);

            try {
                InputStream in = new FileInputStream(file);
                parser.parse(in, Util.notNull(baseURI) ? baseURI : file.toURI().toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();

        } else {
            //triples

            if (contexts.length == 0) {
                stringContexts = new String[1];
                stringContexts[0] = DEFAULT_GRAPH_URI;
            } else {
                stringContexts = new String[contexts.length];
                for (int i = 0; i < contexts.length; i++) {
                    if (Util.notNull(contexts[i])) {
                        stringContexts[i] = contexts[i].toString();
                    } else {
                        stringContexts[i] = DEFAULT_GRAPH_URI;
                    }
                }
            }

            //To create graph document
            for (String con : stringContexts) {
                futures.add(executor.submit(() -> {
                    performUpdateQuery("CREATE SILENT GRAPH <" + escapeXml(con) + ">", null, tx, true, null);
                }));
            }

            RDFParser parser = Rio.createParser(dataFormat);
            parseTriples(tx, parser, executor, futures);

            try {
                InputStream in = new FileInputStream(file);
                parser.parse(in, Util.notNull(baseURI) ? baseURI : file.toURI().toString());
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
        }
    }

    /**
     * Executes merge of triples from InputStream.
     *
     * @param in
     * @param baseURI
     * @param dataFormat
     * @param tx
     * @param contexts
     * @throws RDFParseException
     */
    public void performAdd(InputStream in, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) throws RDFParseException, MarkLogicRdf4jException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(18);
        List<Future<?>> futures = new ArrayList<>();

        if (dataFormat.supportsContexts()) {
            //Quads

            RDFParser parser = Rio.createParser(dataFormat);
            parseQuads(tx, executor, futures, parser);

            try {
                parser.parse(in, Util.notNull(baseURI) ? baseURI : "http://example.org");
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();

        } else {
            //triples

            if (contexts.length == 0) {
                stringContexts = new String[1];
                stringContexts[0] = DEFAULT_GRAPH_URI;
            } else {
                stringContexts = new String[contexts.length];
                for (int i = 0; i < contexts.length; i++) {
                    if (Util.notNull(contexts[i])) {
                        stringContexts[i] = contexts[i].toString();
                    } else {
                        stringContexts[i] = DEFAULT_GRAPH_URI;
                    }
                }
            }

            //To create graph document
            for (String con : stringContexts) {
                futures.add(executor.submit(() -> {
                    performUpdateQuery("CREATE SILENT GRAPH <" + escapeXml(con) + ">", null, tx, true, null);
                }));
            }

            RDFParser parser = Rio.createParser(dataFormat);
            parseTriples(tx, parser, executor, futures);

            try {
                parser.parse(in, Util.notNull(baseURI) ? baseURI : "http://example.org");
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
        }
    }

    /**
     * Executes INSERT of single triple.
     *
     * @param baseURI
     * @param subject
     * @param predicate
     * @param object
     * @param tx
     * @param contexts
     * @throws MarkLogicRdf4jException
     */
    public void performAdd(String baseURI, Resource subject, IRI predicate, Value object, Transaction tx, Resource... contexts) throws MarkLogicRdf4jException {
        StringBuilder sb = new StringBuilder();
        if (contexts.length > 0) {
            if (Util.notNull(baseURI)) sb.append("BASE <" + baseURI + ">\n");
            sb.append("INSERT DATA { ");
            for (int i = 0; i < contexts.length; i++) {
                if (Util.notNull(contexts[i])) {
                    sb.append("GRAPH <" + contexts[i].stringValue() + "> { ?s ?p ?o .} ");
                } else {
                    sb.append("GRAPH <" + DEFAULT_GRAPH_URI + "> { ?s ?p ?o .} ");
                }
            }
            sb.append("}");
        } else {
            sb.append("INSERT DATA { GRAPH <" + DEFAULT_GRAPH_URI + "> {?s ?p ?o .}}");
        }
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (Util.notNull(ruleset)) {
            qdef.setRulesets(ruleset);
        }
        if (Util.notNull(graphPerms)) {
            qdef.setUpdatePermissions(graphPerms);
        }
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }

        if (Util.notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if (Util.notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if (Util.notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.executeUpdate(qdef, tx);
    }

    /**
     * Executes DELETE of single triple.
     *
     * @param baseURI
     * @param subject
     * @param predicate
     * @param object
     * @param tx
     * @param contexts
     * @throws MarkLogicRdf4jException
     */
    public void performRemove(String baseURI, Resource subject, IRI predicate, Value object, Transaction tx, Resource... contexts) throws MarkLogicRdf4jException {
        StringBuilder sb = new StringBuilder();
        String[] contextArgs = null;
        if (contexts.length > 0) {
            if (Util.notNull(baseURI)) sb.append("BASE <" + baseURI + ">\n");
            contextArgs = new String[contexts.length];
            for (int i = 0; i < contexts.length; i++) {
                if (Util.notNull(contexts[i])) {
                    contextArgs[i] = contexts[i].stringValue();
                } else {
                    contextArgs[i] = DEFAULT_GRAPH_URI;
                }
            }
        }
        sb.append("DELETE WHERE { GRAPH ?ctx { ?s ?p ?o .}}");
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (Util.notNull(contextArgs)) qdef.setUsingNamedGraphUris(contextArgs);
        if (Util.notNull(baseURI) && !baseURI.isEmpty()) {
            qdef.setBaseUri(baseURI);
        }
        if (Util.notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if (Util.notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if (Util.notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.executeUpdate(qdef, tx);
    }

    /**
     * Clears triples from named graph.
     *
     * @param tx
     * @param contexts
     */
    public void performClear(Transaction tx, Resource... contexts) {
        if (contexts.length > 0) {
            for (int i = 0; i < contexts.length; i++) {
                if (Util.notNull(contexts[i])) {
                    graphManager.delete(contexts[i].stringValue(), tx);
                } else {
                    graphManager.delete(DEFAULT_GRAPH_URI, tx);
                }
            }
        } else {
            graphManager.delete(DEFAULT_GRAPH_URI, tx);
        }
    }

    /**
     * Clears all triples.
     *
     * @param tx
     */
    public void performClearAll(Transaction tx) {
        graphManager.deleteGraphs(tx);
    }

    public Integer getOptimizeLevel() {
        return optimizeLevel;
    }

    public void setOptimizeLevel(Integer optimizeLevel) {
        this.optimizeLevel = optimizeLevel;
    }

    /**
     * getter rulesets
     *
     * @return
     */
    public SPARQLRuleset[] getRulesets() {
        return this.ruleset;
    }

    /**
     * setter for rulesets, filters out nulls
     *
     * @param rulesets
     */
    public void setRulesets(SPARQLRuleset... rulesets) {
        if (Util.notNull(rulesets)) {
            List<SPARQLRuleset> list = new ArrayList<>();
            for (Object r : rulesets) {
                if (r != null && rulesets.length > 0) {
                    list.add((SPARQLRuleset) r);
                }
            }
            this.ruleset = list.toArray(new SPARQLRuleset[list.size()]);
        } else {
            this.ruleset = null;
        }
    }

    /**
     * setter for graph permissions
     *
     * @param graphPerms
     */
    public void setGraphPerms(GraphPermissions graphPerms) {
        this.graphPerms = graphPerms;
    }

    /**
     * getter for graph permissions
     *
     * @return
     */
    public GraphPermissions getGraphPerms() {
        return this.graphPerms;
    }

    /**
     * setter for ConstrainingQueryDefinition
     *
     * @param constrainingQueryDefinition
     */
    public void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition) {
        this.constrainingQueryDef = constrainingQueryDefinition;
    }

    /**
     * getter for ConstrainingQueryDefinition
     *
     * @return
     */
    public QueryDefinition getConstrainingQueryDefinition() {
        return this.constrainingQueryDef;
    }

    /**
     * Close client.
     */
    public void close() {
        // close MarkLogicClientImpl
    }

    public void release() {
        if (this.databaseClient != null) {
            try {
                this.databaseClient.release();
            } catch (Exception e) {
                logger.info("Failed releasing DB client", e);
            }
        }
    }
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Converts RDF4J BindingSet to java client api SPARQLBindings
     *
     * @param bindings
     * @return
     */
    protected SPARQLBindings getSPARQLBindings(SPARQLQueryBindingSet bindings) {
        SPARQLBindings sps = new SPARQLBindingsImpl();
        for (Binding binding : bindings) {
            sps.bind(binding.getName(), binding.getValue().stringValue());
        }
        return sps;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Bind object.
     *
     * @param qdef
     * @param variableName
     * @param object
     * @return
     * @throws MarkLogicRdf4jException
     */
    private static SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef, String variableName, Value object) throws MarkLogicRdf4jException {
        SPARQLBindings bindings = qdef.getBindings();
        if (object != null) {
            if (object instanceof IRI) {
                bindings.bind(variableName, object.stringValue());
            } else if (object instanceof Literal) {
                Literal lit = (Literal) object;
                if (lit.getLanguage().orElse(null) != null) {
                    String languageTag = lit.getLanguage().orElse(null);
                    bindings.bind(variableName, lit.getLabel(), Locale.forLanguageTag(languageTag));
                } else if (((Literal) object).getDatatype() != null) {
                    try {
                        String xsdType = lit.getDatatype().toString();
                        String fragment = new java.net.URI(xsdType).getFragment();
                        bindings.bind(variableName, lit.getLabel(), RDFTypes.valueOf(fragment.toUpperCase()));
                    } catch (URISyntaxException e) {
                        logger.error(e.getLocalizedMessage());
                        throw new MarkLogicRdf4jException("Problem with object datatype.");
                    }
                } else {
                    // assume we have a string value
                    bindings.bind(variableName, lit.getLabel(), RDFTypes.STRING);
                }
            }
            qdef.setBindings(bindings);
        }
        return qdef;
    }

    class Task implements Runnable {
        private DocumentWriteSet writeSet;
        private Transaction tx;
        private XMLDocumentManager documentManager;

        Task(DocumentWriteSet writeSet, Transaction tx, XMLDocumentManager documentManager) {
            this.writeSet = writeSet;
            this.tx = tx;
            this.documentManager = documentManager;
        }

        @Override
        public void run() {
            documentManager.write(writeSet, tx);
        }
    }

    private void parseTriples(Transaction tx, RDFParser parser, ThreadPoolExecutor executor, List<Future<?>> futures) {
        parser.setRDFHandler(new RDFHandler() {
            StringBuffer sb;
            int i = -1;
            int T_PER_DOC = 1000;
            int DOCS_PER_BATCH = 4;
            int n = 0;


            XMLDocumentManager documentManager = databaseClient.newXMLDocumentManager();
            DocumentWriteSet writeSet = documentManager.newWriteSet();

            void startDoc() {
                sb = new StringBuffer();
                sb.append("<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">\n");
            }

            void endDoc() {
                sb.append("</sem:triples>\n");
                String st = sb.toString();
                DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(stringContexts);
                writeSet.add("/triplestore/" + UUID.randomUUID() + ".xml", metadata, new StringHandle(st));

                n++;
                if (n == DOCS_PER_BATCH) {
                    n = 0;
                    futures.add(executor.submit(new Task(writeSet, tx, documentManager)));
                    writeSet = documentManager.newWriteSet();
                }
            }

            @Override
            public void startRDF() throws RDFHandlerException {
                startDoc();
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                endDoc();
                //flush remaining documents when DOCS_PER_BATCH is not full
                futures.add(executor.submit(new Task(writeSet, tx, documentManager)));
            }

            @Override
            public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                i++;
                if (i == T_PER_DOC) {
                    i = 0;
                    endDoc();
                    startDoc();
                }

                sb.append("<sem:triple>\n");
                sb.append(subject(st.getSubject()));
                sb.append(predicate(st.getPredicate()));
                sb.append(object(st.getObject()));
                sb.append("</sem:triple>\n");
            }

            @Override
            public void handleComment(String comment) throws RDFHandlerException {

            }
        });
    }

    private void parseQuads(Transaction tx, ThreadPoolExecutor executor, List<Future<?>> futures, RDFParser parser) {
        parser.setRDFHandler(new RDFHandler() {

            int i = 0;
            int T_PER_DOC = 100;
            int DOCS_PER_BATCH = 3000;
            int n = 0;

            XMLDocumentManager documentManager = databaseClient.newXMLDocumentManager();
            DocumentWriteSet writeSet = documentManager.newWriteSet();

            public void startDoc() {
                startDoc(graphCache.get(DEFAULT_GRAPH_URI));
            }

            public void startDoc(StringBuilder sb) {
                sb.append("<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">\n");
            }

            public void endDoc() {
                endDoc(DEFAULT_GRAPH_URI);
            }

            public void endDoc(String graph) {
                StringBuilder sb = graphCache.get(graph);
                sb.append("</sem:triples>\n");

                String st = sb.toString();
                DocumentMetadataHandle metadata = new DocumentMetadataHandle().withCollections(graph);
                writeSet.add("/triplestore/" + UUID.randomUUID() + ".xml", metadata, new StringHandle(st));

                n++;
                if (n == DOCS_PER_BATCH) {
                    n = 0;
                    futures.add(executor.submit(new Task(writeSet, tx, documentManager)));
                    writeSet = documentManager.newWriteSet();
                }

                graphCache.remove(graph);
            }


            Map<String, StringBuilder> graphCache;
            Map<String, Integer> tripleCounts;
            Set<String> graphSet;

            @Override
            public void startRDF() throws RDFHandlerException {
                documentManager = databaseClient.newXMLDocumentManager();
                graphCache = new ConcurrentHashMap<>();
                graphCache.put(DEFAULT_GRAPH_URI, new StringBuilder());
                startDoc();
                tripleCounts = new ConcurrentHashMap<>();
                writeSet = documentManager.newWriteSet();
                graphSet = new HashSet<>();
            }

            @Override
            public void endRDF() throws RDFHandlerException {

                for (String key : graphCache.keySet()) {
                    endDoc(key);
                }

                //flush remaining documents when DOCS_PER_BATCH is not full
                futures.add(executor.submit(new Task(writeSet, tx, documentManager)));
            }

            @Override
            public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(Statement st) throws RDFHandlerException {
                if (st.getContext() != null) {
                    //Quad
                    String graph = st.getContext().toString();

                    //To create graph document
                    if (!graphSet.contains(graph)) {
                        futures.add(executor.submit(() -> {
                            performUpdateQuery("CREATE SILENT GRAPH <" + escapeXml(graph) + ">", null, tx, true, null);
                        }));
                        graphSet.add(graph);
                    }

                    if (tripleCounts.containsKey(graph)) {
                        int j = 1 + tripleCounts.get(graph);
                        tripleCounts.put(graph, j);
                        if (j == T_PER_DOC) {
                            tripleCounts.put(graph, 0);
                            endDoc(graph);
                            graphCache.put(graph, new StringBuilder());
                            startDoc(graphCache.get(graph));
                        }
                    } else {
                        tripleCounts.put(graph, 1);
                        graphCache.put(graph, new StringBuilder());
                        startDoc(graphCache.get(graph));
                    }

                    triple(graphCache.get(graph), st);
                } else {
                    //Triple
                    i++;
                    if (i == T_PER_DOC) {
                        i = 0;
                        endDoc();
                        StringBuilder sb = new StringBuilder();
                        graphCache.put(DEFAULT_GRAPH_URI, sb);
                        startDoc(graphCache.get(DEFAULT_GRAPH_URI));
                    }

                    //To create graph document
                    if (!graphSet.contains(DEFAULT_GRAPH_URI)) {
                        futures.add(executor.submit(() -> {
                            performUpdateQuery("CREATE SILENT GRAPH <" + escapeXml(DEFAULT_GRAPH_URI) + ">", null, tx, true, null);
                        }));
                        graphSet.add(DEFAULT_GRAPH_URI);
                    }

                    triple(graphCache.get(DEFAULT_GRAPH_URI), st);
                }
            }

            @Override
            public void handleComment(String comment) throws RDFHandlerException {

            }

            private void triple(StringBuilder sb, Statement st) {
                sb.append("<sem:triple>\n");
                sb.append(subject(st.getSubject()));
                sb.append(predicate(st.getPredicate()));
                sb.append(object(st.getObject()));
                sb.append("</sem:triple>\n");
            }
        });
    }

    private String subject(Value subject) {
        if (subject instanceof org.eclipse.rdf4j.model.BNode) {
            //Skolemization using toString()
            return "<sem:subject>http://marklogic.com/semantics/blank/" + subject.toString() + "</sem:subject>\n";
        } else {
            return "<sem:subject>" + subject.stringValue() + "</sem:subject>\n";
        }
    }

    private String predicate(Value predicate) {
        return "<sem:predicate>" + predicate.stringValue() + "</sem:predicate>\n";
    }

    private String object(Value object) {
        if (object instanceof Literal) {
            Literal lit = (Literal) object;
            String lang = lit.getLanguage().orElse("");
            String type = lit.getDatatype().toString();

            if (!lang.equals("")) {
                lang = " xml:lang=\"" + lang + "\"";
            }

            if ("".equals(lang)) {
                if (type == null) {
                    type = "http://www.w3.org/2001/XMLSchema#string";
                }
                type = " datatype=\"" + type + "\"";
            } else {
                type = "";
            }

            return "<sem:object" + type + lang + ">" + object.stringValue() + "</sem:object>\n";
        } else if (object instanceof org.eclipse.rdf4j.model.BNode) {
            //Skolemization using toString()
            return "<sem:object>http://marklogic.com/semantics/blank/" + object.toString() + "</sem:object>\n";
        } else {
            return "<sem:object>" + object.stringValue() + "</sem:object>\n";
        }
    }

    private String escapeXml(String _in) {
        if (null == _in) {
            return "";
        }
        return patterns[2].matcher(
                patterns[1].matcher(
                        patterns[0].matcher(_in).replaceAll("&amp;"))
                        .replaceAll("&lt;")).replaceAll("&gt;");
    }
}
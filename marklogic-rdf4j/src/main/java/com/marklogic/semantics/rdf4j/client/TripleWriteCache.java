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

import com.marklogic.semantics.rdf4j.MarkLogicRdf4jException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.UpdateExecutionException;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLUtil;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.SPARQLQueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jfuller on 11/2/16.
 */
public class TripleWriteCache extends TripleCache {

    private static final Logger log = LoggerFactory.getLogger(TripleWriteCache.class);

    public TripleWriteCache(MarkLogicClient client) {
        super(client);
    }

    public TripleWriteCache(MarkLogicClient client, long cacheSize) {
        super(client, cacheSize);
    }
    /**
     * flushes the cache, writing triples as graph
     *
     * @throws MarkLogicRdf4jException
     */

    protected synchronized void flush() throws RepositoryException, MalformedQueryException, UpdateExecutionException, IOException {
        if (cache.isEmpty()) { return; }
        StringBuffer entireQuery = new StringBuffer();
        SPARQLQueryBindingSet bindingSet = new SPARQLQueryBindingSet();

        for (Namespace ns :cache.getNamespaces()){
            entireQuery.append("PREFIX "+ns.getPrefix()+": <"+ns.getName()+">. ");
        }
        entireQuery.append("INSERT DATA { ");

        Set<Resource> distinctCtx = new HashSet<Resource>();
        for (Resource context :cache.contexts()) {
            distinctCtx.add(context);
        }

        for (Resource ctx : distinctCtx) {
               if (ctx != null) {
                   entireQuery.append(" GRAPH <" + ctx + "> { ");
               }
                for (Statement stmt : cache.filter(null, null, null, ctx)) {
                    if (stmt.getSubject() instanceof org.eclipse.rdf4j.model.BNode) {
                        entireQuery.append("<http://marklogic.com/semantics/blank/" + stmt.getSubject().stringValue() + "> ");
                    }else {
                        entireQuery.append("<" + stmt.getSubject().stringValue() + "> ");
                    }
                    if (stmt.getPredicate() instanceof org.eclipse.rdf4j.model.BNode) {
                        entireQuery.append("<http://marklogic.com/semantics/blank/" + stmt.getPredicate().stringValue() + "> ");
                    }else{
                        entireQuery.append("<" + stmt.getPredicate().stringValue() + "> ");
                    }
                    Value object=stmt.getObject();
                    if (object instanceof Literal) {
                        Literal lit = (Literal) object;
                        entireQuery.append("\"");
                        entireQuery.append(SPARQLUtil.encodeString(lit.getLabel()));
                        entireQuery.append("\"");
                        if(null == lit.getLanguage().orElse(null)) {
                            entireQuery.append("^^<" + lit.getDatatype().stringValue() + ">");
                        }else{
                            entireQuery.append("@" + lit.getLanguage().orElse(null));
                        }
                    } else {
                        if (stmt.getObject() instanceof org.eclipse.rdf4j.model.BNode) {
                            entireQuery.append("<http://marklogic.com/semantics/blank/" + stmt.getObject().stringValue() + "> ");
                        }else {
                            entireQuery.append("<" + object.stringValue() + "> ");
                        }
                    }
                    entireQuery.append(".");
                }
                if (ctx != null) {
                    entireQuery.append(" }");
                }
        }

        entireQuery.append("} ");
        log.debug(entireQuery.toString());
        client.sendUpdateQuery(entireQuery.toString(),bindingSet,false,null);
        lastCacheAccess = new Date();
        log.debug("success writing cache: {}",String.valueOf(cache.size()));
        cache.clear();

    }

}

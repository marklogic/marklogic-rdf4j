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
package com.marklogic.semantics.rdf4j.utils;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;


public class Util {
    private static Util util = null;
    private Util(){

    }

    /**
     * Authentication enumerates the methods for verifying a user and
     * password with the database.
     */
    public enum Authentication {
        /**
         * Minimal security unless used with SSL.
         */
        BASIC,
        /**
         * Moderate security without SSL.
         */
        DIGEST,
        /**
         * Authentication using Kerberos.
         */
        KERBEROS,
        /**
         * Authentication using Certificates;
         */
        CERTIFICATE;
        /**
         * Returns the enumerated value for the case-insensitive name.
         * @param name	the name of the enumerated value
         * @return	the enumerated value
         */
        static public Authentication valueOfUncased(String name) {
            return Authentication.valueOf(name.toUpperCase());
        }
    }

    /**
     * Method to obtain instance of Util class.
     * @return Util
     */
    public static Util getInstance()
    {
        if(util == null){
            util = new Util();
        }
        return util;
    }

    /**
     * Public utility method that tests if an object is null.
     * @param item the item to check for null.
     * @return boolean
     */
    public static Boolean notNull(Object item) {
        return item!=null;
    }

    /**
     * Public utility method that skolemizes blank nodes (BNode).
     * @param s the blank node to be skolemized.
     * @return for BNode returns skolemized BNode or else the node itself.
     */
    public Value skolemize(Value s) {
        if (s instanceof org.eclipse.rdf4j.model.BNode) {
            return SimpleValueFactory.getInstance().createIRI("http://marklogic.com/semantics/blank/" + s.toString());
        } else {
            return s;
        }
    }

    /**
     * Public utility method to check if the RDF format is supported by MarkLogic database.
     * @param dataFormat the RDF format to check if supported by MarkLogic.
     * @return Boolean
     */
    public Boolean isFormatSupported(RDFFormat dataFormat){
        return dataFormat.equals(RDFFormat.TURTLE) || dataFormat.equals(RDFFormat.RDFXML) || dataFormat.equals(RDFFormat.TRIG)
            || dataFormat.equals(RDFFormat.NQUADS) || dataFormat.equals(RDFFormat.NTRIPLES) || dataFormat.equals(RDFFormat.RDFJSON)
                || dataFormat.equals(RDFFormat.N3);
    }

    public DatabaseClient getClientBasedOnAuth(String host, int port, String database, DatabaseClientFactory.SecurityContext securityContext)
    {
        return DatabaseClientFactory.newClient(host, port, database, securityContext);
    }

    /**
     * Public utility that returns DatabaseClient based on auth
     * @return DatabaseClient
     */
    public DatabaseClient getClientBasedOnAuth(String host, int port, String user, String password, String database, String auth) {
        Authentication type;

        if(auth != null)
        {
            type = Authentication.valueOfUncased(auth);

            if(type == Authentication.BASIC)
            {
                return DatabaseClientFactory.newClient(host, port, database, new DatabaseClientFactory.BasicAuthContext(user, password));
            }
            else if(type == Authentication.DIGEST)
            {
                return DatabaseClientFactory.newClient(host, port, new DatabaseClientFactory.DigestAuthContext(user, password));
            }
        }

        return null;
    }
}

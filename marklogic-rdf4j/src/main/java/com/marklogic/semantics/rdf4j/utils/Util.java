package com.marklogic.semantics.rdf4j.utils;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;

public class Util {
    private static Util util = null;
    private Util(){

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
     * @return for BNode return skolemized BNode or else the node itself.
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
}

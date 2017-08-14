package com.marklogic.semantics.rdf4j.utils;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class Util {
    private static Util util = null;
    private Util(){

    }

    public static Util getInstance()
    {
        if(util == null){
            util = new Util();
        }
        return util;
    }

    /**
     *
     * @param s
     * @return
     */
    public Value skolemize(Value s) {
        if (s instanceof org.eclipse.rdf4j.model.BNode) {
            return SimpleValueFactory.getInstance().createIRI("http://marklogic.com/semantics/blank/" + s.toString());
        } else {
            return s;
        }
    }

    /**
     * private utility method that tests if an object is null
     *
     * TBD -
     * @param item
     * @return boolean
     */
    public static Boolean notNull(Object item) {
        return item!=null;
    }
}

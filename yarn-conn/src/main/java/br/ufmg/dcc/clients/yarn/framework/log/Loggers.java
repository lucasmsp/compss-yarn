package br.ufmg.dcc.clients.yarn.framework.log;

import es.bsc.conn.exceptions.NonInstantiableException;

/**
 * Loggers' names for Yarn Connector Components
 *
 */


public final class Loggers {

    // Yarn Framework
    public static final String YARN = "br.ufmg.dcc.conn.loggers.Loggers.YARN";

    // Yarn Framework Client
    public static final String YARN_CLIENT = YARN + ".YARN_Client";

    // Yarn Task
    public static final String YARN_APPLICATION_MASTER = YARN + ".YARN_ApplicationMaster";


    private Loggers() {
        throw new NonInstantiableException("Loggers should not be instantiated");
    }

}

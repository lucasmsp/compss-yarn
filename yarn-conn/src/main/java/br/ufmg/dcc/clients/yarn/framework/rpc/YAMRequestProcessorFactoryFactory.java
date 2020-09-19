package br.ufmg.dcc.clients.yarn.framework.rpc;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.XmlRpcRequest;
import org.apache.xmlrpc.server.RequestProcessorFactoryFactory;


/*
 *  By default, Apache XML-RPC creates a new object for processing each request received at the server-side.
 *  However, we can emulate the behaviour of XMLRPC 2.x, where we registered handler objects instead of handler
 *  classes, using a custom RequestProcessorFactoryFactory.
 */
public class YAMRequestProcessorFactoryFactory implements RequestProcessorFactoryFactory {

    private final RequestProcessorFactory factory = new YAMRequestProcessorFactory();
    private final YarnApplicationMasterInterface yam;

    public YAMRequestProcessorFactoryFactory(YarnApplicationMasterInterface yam) {

        this.yam = yam;
    }

    public RequestProcessorFactory getRequestProcessorFactory(Class aClass) throws XmlRpcException {
        return factory;
    }

    private class YAMRequestProcessorFactory implements RequestProcessorFactory {
        public Object getRequestProcessor(XmlRpcRequest xmlRpcRequest) throws XmlRpcException {
            return yam;
        }
    }
}
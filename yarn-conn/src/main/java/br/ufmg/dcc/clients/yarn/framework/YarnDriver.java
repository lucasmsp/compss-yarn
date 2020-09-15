package br.ufmg.dcc.clients.yarn.framework;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;

import br.ufmg.dcc.clients.yarn.framework.log.Loggers;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.xmlrpc.*;



public class YarnDriver {

    /*
     * RPC Client to Yarn Client be able to connect with Yarn Application Master.
     */


    private XmlRpcClient client;
    private static final String UNDEFINED_IP = "-1.-1.-1.-1";
    private static final Logger logger = LogManager.getLogger(Loggers.YARN_CLIENT);
    private final int serverPort;
    private final String address;

    public YarnDriver(String address, int serverPort){
        this.serverPort = serverPort;
        this.address = address;
        startClient();
    }

    public void startClient(){
        try {
            Thread.sleep(1000);
            String url = "http://"+address+":"+serverPort;
            logger.debug("Connecting with Yarn Client via RPC ("+url+")");
            client = new XmlRpcClient(url);
            //client.setBasicAuthentication("myUsername", "myPassword");
        } catch (Exception exception) {
            //System.err.println("JavaClient: " + exception);
        }
    }

    public String requestWorker(String workerId, String imageName, int cpus, int memory,
                                String publicKey, String networkDocker) {

        String container_id = "";
        Vector params = new Vector();
        params.addElement(workerId);
        params.addElement(imageName);
        params.addElement(new Integer(cpus));
        params.addElement(new Integer(memory));
        params.addElement(publicKey);
        params.addElement(networkDocker);

        Object result = null;

        while (result == null) {
            try {
                result = client.execute("default.requestWorker", params);

            } catch (XmlRpcException | IOException e) {
                //e.printStackTrace();
            }
        }

        container_id = (String) result;
        return container_id;
    }

    public String waitTask(String id, int runWorkerTimeout, String runWorkerTimeoutUnits){
        String ip = UNDEFINED_IP;

        Vector params = new Vector();
        params.addElement(id);
        params.addElement(new Integer(runWorkerTimeout));
        params.addElement(runWorkerTimeoutUnits);

        while (ip.equals(UNDEFINED_IP)) {
            try {
                ip = (String) client.execute("default.waitTask", params);
                if (ip == null)
                    ip = UNDEFINED_IP;
            } catch (XmlRpcException | IOException e) {
                //e.printStackTrace();
            }
        }
        return ip;
    }

    public boolean removeTask(String id, int runWorkerTimeout, String runWorkerTimeoutUnits){

        Vector params = new Vector();
        params.addElement(id);
        params.addElement(runWorkerTimeout);
        params.addElement(runWorkerTimeoutUnits);

        Object result = null;
        while (result == null) {
            try {
                result = client.execute("default.removeTask", params);
            } catch (XmlRpcException | IOException e) {
               // e.printStackTrace();
            }
        }
        return  (boolean) result;
    }


    public void stopFramework(){


        try {
            PrintStream tmp = System.err;
            System.setErr(new PrintStream(new OutputStream() {
                @Override
                public void write(int arg0) throws IOException {
                }
            }));

            client.execute("default.stopFramework", new Vector());
            System.setErr(tmp);
        } catch (Exception e) {
            //System.out.println("Could not start server: " + e.getMessage());
            //e.printStackTrace();
        }

    }

}

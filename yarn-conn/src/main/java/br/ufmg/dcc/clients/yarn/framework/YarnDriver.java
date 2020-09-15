package br.ufmg.dcc.clients.yarn.framework;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeoutException;

import br.ufmg.dcc.clients.yarn.framework.log.Loggers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.apache.xmlrpc.*;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.TimingOutCallback;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;


public class YarnDriver {

    /*
     * RPC Client to Yarn Client be able to connect with Yarn Application Master.
     */


    private XmlRpcClient client;
    private static final String UNDEFINED_IP = "-1.-1.-1.-1";
    private static final Logger logger = LogManager.getLogger(Loggers.YARN_CLIENT);
    private final int serverPort;
    private final String address;
    private String publicKey;
    private String networkDocker;
    private long runWorkerTimeout;
    private long killWorkerTimeout;
    private final PrintStream original;
    private final PrintStream alternative;

    public YarnDriver(String address, int serverPort){
        this.serverPort = serverPort;
        this.address = address;
        this.original = System.err;
        this.alternative = new PrintStream(new OutputStream() {
            @Override
            public void write(int arg0) throws IOException {
            }
        });
        startClient();
    }

    public void setRunWorkerTimeout(long runWorkerTimeout) {
        this.runWorkerTimeout = runWorkerTimeout;
    }

    public void setKillWorkerTimeout(long killWorkerTimeout) {
        this.killWorkerTimeout = killWorkerTimeout;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public void startClient(){
        try {
            Thread.sleep(1000);
            System.setErr(alternative);
            String url = "http://"+address+":"+serverPort;
            logger.debug("Connecting with Yarn Client via RPC ("+url+")");

            client = new XmlRpcClient();

            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();

            config.setServerURL(new URL(url));
            config.setConnectionTimeout(10000);
            config.setReplyTimeout(60000);
            client.setConfig(config);

            //client.setBasicAuthentication("myUsername", "myPassword");
        } catch (Exception exception) {
            //System.err.println("JavaClient: " + exception);
        }
        System.setErr(original);
    }

    public String requestWorker(String workerId, String imageName, int cpus, int memory) {

        String container_id = "";
        Object result = null;
        System.setErr(alternative);

        Vector params = new Vector();
        params.addElement(workerId);
        params.addElement(imageName);
        params.addElement(new Integer(cpus));
        params.addElement(new Integer(memory));
        params.addElement(publicKey);
        params.addElement(networkDocker);

        TimingOutCallback callback = new TimingOutCallback(10 * 1000);

        try {
            client.executeAsync("default.requestWorker", params, callback);
            result = callback.waitForResponse();

        } catch (TimeoutException e) {
            System.out.println("No response from server.");
        } catch (Exception e) {
            System.out.println("Server returned an error message.");
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
//        while (result == null) {
//            try {
//                result = client.executeAsync("default.requestWorker", params);
//                Thread.sleep(1000);
//            } catch (XmlRpcException | IOException | InterruptedException e) {
//                //e.printStackTrace();
//            }
//        }
        System.setErr(original);
        container_id = (String) result;
        return container_id;
    }

    public String waitTask(String id){

        String ip = UNDEFINED_IP;
       // System.setErr(alternative);

        Vector params = new Vector();
        params.addElement(id);
        params.addElement(new Integer((int) runWorkerTimeout));

        TimingOutCallback callback = new TimingOutCallback(10 * 1000);

        try {
            client.executeAsync("default.waitTask", params, callback);
            ip = (String) callback.waitForResponse();

        } catch (TimeoutException e) {
            System.out.println("No response from server.");
        } catch (Exception e) {
            System.out.println("Server returned an error message.");
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

//        while (ip.equals(UNDEFINED_IP)) {
//            try {
//                ip = (String) client.execute("default.waitTask", params);
//                Thread.sleep(1000);
//                if (ip == null)
//                    ip = UNDEFINED_IP;
//            } catch ( InterruptedException | XmlRpcException e) {
//                //e.printStackTrace();
//            }
//        }
      //  System.setErr(original);
        return ip;
    }

    public boolean removeTask(String id){

        Object result = null;
        System.setErr(alternative);

        Vector params = new Vector();
        params.addElement(id);
        params.addElement(new Integer((int) killWorkerTimeout));

        while (result == null) {
            try {
                result = client.execute("default.removeTask", params);
                Thread.sleep(1000);
            } catch (XmlRpcException | InterruptedException e) {
               // e.printStackTrace();
            }
        }
        return  (boolean) result;
    }


    public void stopFramework(){

        try {
            System.setErr(alternative);
            client.execute("default.stopFramework", new Vector());
            System.setErr(original);
        } catch (Exception ignored) {

        }

    }

    public void setNetworkDocker(String networkDocker) {
        this.networkDocker = networkDocker;
    }
}

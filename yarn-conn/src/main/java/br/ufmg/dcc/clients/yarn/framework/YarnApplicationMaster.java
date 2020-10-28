package br.ufmg.dcc.clients.yarn.framework;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import br.ufmg.dcc.clients.yarn.framework.rpc.YAMRequestProcessorFactoryFactory;
import br.ufmg.dcc.clients.yarn.framework.rpc.YarnApplicationMasterInterface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.WebServer;


/**
 * Yarn Scheduler implementation for COMPSs.
 */
public class YarnApplicationMaster implements YarnApplicationMasterInterface {

    private static final String UNDEFINED_IP = "-1.-1.-1.-1";

    private static final String ERROR_TASK_ID = "ERROR: Task does not exist. TaskId = ";

    private List<String> runningTasks = Collections.synchronizedList(new LinkedList<String>());
    private List<String> pendingTasks = Collections.synchronizedList(new LinkedList<String>());
    private Map<String, YarnTask> tasks = Collections.synchronizedMap(new HashMap<String, YarnTask>());

    private AMRMClient<AMRMClient.ContainerRequest> rmClient;
    private NMClient nmClient;
    private Configuration conf;

    private ByteBuffer allTokens;
    private int progress;
    private final String masterIp;
    private final String masterHostname;
    private final int serverPort;
    private final String serverAddress;

    public static void main(String[] args) throws XmlRpcException, IOException, YarnException {

        String serverAddress = getServerAddress();
        int serverPort = getServerPort(serverAddress);

        WebServer webServer = new WebServer(serverPort);

        XmlRpcServer server = webServer.getXmlRpcServer();
        PropertyHandlerMapping phm = new PropertyHandlerMapping();

        YarnApplicationMaster yam = new YarnApplicationMaster(args[0], args[1], serverAddress, serverPort);
        phm.setRequestProcessorFactoryFactory(new YAMRequestProcessorFactoryFactory(yam));
        phm.addHandler(YarnApplicationMasterInterface.class.getName(), YarnApplicationMaster.class);
        server.setHandlerMapping(phm);

        XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) server.getConfig();
        serverConfig.setEnabledForExtensions(true);
        serverConfig.setContentLengthOptional(false);

        webServer.start();

        logger("XML-RPC Server started on port "+serverPort);
    }


    /**
     * Creates a new Yarn Framework scheduler.
     */
    public YarnApplicationMaster(String masterHostname, String masterIp,
                                 String serverAddress, int serverPort) throws IOException, YarnException {
        logger("Initializing " + this.getClass().getName());
        progress = 0;

        this.masterHostname = masterHostname;
        this.masterIp = masterIp;
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;

        init();
        waitRegistration();

    }

    private static boolean PortIsInUse(String serverAddress, int port) {

        Socket s = null;
        try {
            s = new Socket(serverAddress, port);
            // If the code makes it this far without an exception it means
            // something is using the port and has responded.
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if( s != null){
                try {
                    s.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error in PortIsInUse:" , e);
                }
            }
        }
    }

    public void init() throws IOException {

        Credentials credentials = UserGroupInformation.getCurrentUser()
                .getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Initialize clients to ResourceManager and NodeManagers
        conf = new YarnConfiguration();

        rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

    }

    public boolean stopFramework(int timeout) {
        try {
            logger("Stopping Yarn Application Master");
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "COMPSs Execution Completed",
                    "");

        }catch (YarnException | IOException ignored) {
            System.exit(1);
        }

        System.exit(0);
        return true;
    }


    /**
     * Petition to create a worker on Yarn, uses a docker image specified by imageName and will use resources specified
     * in list to be created.
     *

     * @param timeout used internally in rpc connection with client
     * @param workerId Docker Container Id.
     * @param imageName Docker image name
     * @param VCores number of cores inside each new container
     * @param Memory number in mega bytes inside each new container
     * @param publicKey public key of master user that is running a compss application
     * @param dockerNetwork docker network
     * @param userVM docker image user
     * @return Yarn container Identifier generated for that worker.
     */

    public synchronized String requestWorker(int timeout, String workerId, String imageName, int VCores, int Memory,
                                String publicKey, String dockerNetwork, String userVM){
        logger("Receiving request to create a new worker");
        String containerId = "";
        String keyPath = "~/.ssh/authorized_keys";
        if (userVM.equals("root"))
            keyPath = "/root/.ssh/authorized_keys";

        pendingTasks.add(workerId);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource requirements = Records.newRecord(Resource.class);
        requirements.setMemorySize(Memory);
        requirements.setVirtualCores(VCores);

        // Make container requests to ResourceManager
        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(requirements,
                null, null, priority);
        long requestID = containerAsk.getAllocationRequestId();

        rmClient.addContainerRequest(containerAsk);

        AllocateResponse response = null;
        boolean containerRequested = false;

        while (!containerRequested) {
            try {
                response = rmClient.allocate(progress++);
            } catch (YarnException | IOException e) {
                e.printStackTrace();
            }

            List<Container> containers = response.getAllocatedContainers();

            for (Container container : containers) {
                if (requestID == container.getAllocationRequestId()) {
                    rmClient.removeContainerRequest(containerAsk);

                    containerId = container.getId().toString();

                    Vector<CharSequence> vargs = new Vector<CharSequence>(10);
                    vargs.add("docker run -t --rm");
                    vargs.add("--cpus=" + VCores);
                    vargs.add("--memory=" + Memory + "m");
                    vargs.add("--network=" + dockerNetwork);
                    vargs.add("--entrypoint='bash'");
                    vargs.add("--name=" + workerId);
                    vargs.add(imageName);
                    vargs.add("-c ' echo " + publicKey + " >> " + keyPath + "; " +
                            "echo " + masterIp + " " + masterHostname + " > /etc/hosts; " +
                            "service ssh restart; " +
                            "echo worker is starting;" +
                            "hostname;" +
                            "hostname -I;" +
                            "echo worker is ready;" +
                            "sleep infinity; " +
                            "echo Timeout finished;'");
                    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
                    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

                    StringBuilder command = new StringBuilder();
                    for (CharSequence str : vargs) {
                        command.append(str).append(" ");
                    }

                    // Launch container by create ContainerLaunchContext
                    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                    ctx.setTokens(allTokens.duplicate());
                    ctx.setCommands(Collections.singletonList(command.toString()));

                    logger("Launching container " + containerId);

                    try {
                        nmClient.startContainer(container, ctx);
                    } catch (YarnException | IOException e) {
                        e.printStackTrace();
                    }

                    YarnTask yt = new YarnTask(workerId, ctx,
                            containerId,
                            container.getNodeId().toString(), userVM);

                    containerRequested = true;
                    tasks.put(workerId, yt);
                }
            }
        }

        return containerId;
    }

    /**
     * Wait for task with identifier to reach RUNNING state. If state is not reached, task is removed from pending and
     * running tasks.
     *
     * @param timeout Timeout in seconds.
     * @param id Task identifier to wait for.
     * @throws FrameworkException if waits for timeout units.
     */
    public String waitTask(int timeout, String id) throws FrameworkException {

        String ip = UNDEFINED_IP;
        logger("Waiting task "+ id);

        String result = "";

        if (!tasks.containsKey(id)) {
            throw new FrameworkException(ERROR_TASK_ID + id);
        }

        YarnTask task = tasks.get(id);

        waitDockerContainerIsReady(task);

        String nmHost = task.getNodeId().split(":")[0];
        String nmPort = System.getenv("NM_HTTP_PORT");

        String url = "http://" + nmHost + ":" + nmPort +
                "/ws/v1/node/containerlogs/" + task.getContainerId() + "/stdout";

        logger("Getting log from:  "+ url);
        while (!result.contains("ready")) {
            Runtime rt = Runtime.getRuntime();
            Process pr = null;
            try {
                pr = rt.exec("curl -s -S " + url);
            } catch (IOException e) {
                e.printStackTrace();
            }

            result = new BufferedReader(
                    new InputStreamReader(pr.getInputStream()))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }
        logger("Log received from: "+ url);

        Pattern pattern = Pattern.compile("worker is starting\n.*\n.*\nworker is ready", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(result);
        boolean matchFound = matcher.find();

        if (matchFound) {
            ip = matcher.group().split("\n")[2].split(" ")[0];

            task.setIp(ip);
            logger("Container is ready with ip " + ip);

            tasks.put(id, task);
            pendingTasks.remove(id);
            runningTasks.add(id);
        }

        return ip;
    }

    public void waitDockerContainerIsReady(YarnTask task) {
        logger("Checking if Yarn container is running");

        String nodeId = task.getNodeId();
        ContainerId container = ContainerId.fromString(task.getContainerId());
        NodeId node = NodeId.fromString(nodeId);

        ContainerStatus cs = null;
        boolean isReady = false;
        int retry = 0;

        try {
            while (!isReady){
                cs = nmClient.getContainerStatus(container, node);

                isReady = cs.getState().equals(ContainerState.RUNNING);
                retry++;

                if (retry % 1000 == 0){
                    logger("Waiting Docker Container (" + task.getId() + ") - retry: " + retry);
                    logger(cs.getDiagnostics());
                }
            }

        } catch (YarnException | IOException e) {
            logger("Yarn Connector failed to check status of Yarn container " +
                    container.toString());
        }

        logger("Yarn container is running");
    }

    /**
     * Wait for the framework to register in Yarn. If it is already registered returns immediately.
     */
    public void waitRegistration() throws IOException, YarnException {
        logger("Wait for framework to register");
        rmClient.registerApplicationMaster(serverAddress, serverPort, "");
        logger("Framework is registered (" +serverAddress+":"+serverPort+")");

    }

    /**
     * Removes a task. If it was on pending queue it has not a worker running on Yarn and only it is removed from
     * queue. If it is running on Yarn asks the driver to kill it and waits for status update.
     *
     *
     * @param id Task identifier.
     * @param timeout Timeout in seconds.
     * @throws FrameworkException if task does not exist.
     */
    public String removeTask(int timeout, String id) throws FrameworkException {

        // Task still in pending queue, not launched to run in Yarn
        if (pendingTasks.contains(id)) {
            logger("Task still in pending queue, not launched to run in Yarn");
            pendingTasks.remove(id);
            return "";

        } else if (!tasks.containsKey(id)) {
            runningTasks.remove(id);
            throw new FrameworkException(ERROR_TASK_ID + id);
        }

        YarnTask task  = tasks.get(id);
        String nodeId = task.getNodeId();
        String containerId = task.getContainerId();

        // stopping Yarn Container
        try {
            nmClient.stopContainer(ContainerId.fromString(containerId), NodeId.fromString(nodeId));
        } catch (YarnException | IOException e) {
            throw new FrameworkException("Yarn Connector failed to stop Yarn container with id " + containerId);
        }

        String hostContainer = task.getNodeId().split(":")[0];
        tasks.remove(id);
        logger("Docker Container (" + id + ") is removed.");

        return hostContainer;
    }

    public static String getServerAddress(){
        String serverAddress;
        try {
            serverAddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            serverAddress = "localhost";
        }
        return serverAddress;
    }

    public static int getServerPort(String serverAddress){
        int serverPort = 9942;
        while (PortIsInUse(serverAddress, serverPort))
            serverPort += 1;
        return serverPort;
    }

    public static void logger(String msg){
        System.out.println(msg);
    }

}

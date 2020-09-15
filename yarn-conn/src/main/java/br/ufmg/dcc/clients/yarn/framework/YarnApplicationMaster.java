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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.xmlrpc.server.PropertyHandlerMapping;
import org.apache.xmlrpc.server.XmlRpcServer;
import org.apache.xmlrpc.server.XmlRpcServerConfigImpl;
import org.apache.xmlrpc.webserver.ServletWebServer;
import org.apache.xmlrpc.webserver.WebServer;
import org.apache.xmlrpc.webserver.XmlRpcServlet;


/**
 * Yarn Scheduler implementation for COMPSs.
 */
public class YarnApplicationMaster {

    private static final String UNDEFINED_IP = "-1.-1.-1.-1";

    //private static final Logger LOGGER = LogManager.getLogger(Loggers.YARN_APPLICATION_MASTER);
    private static final String ERROR_TASK_ID = "ERROR: Task does not exist. TaskId = ";

    private List<String> runningTasks;
    private List<String> pendingTasks;
    private Map<String, YarnTask> tasks;

    private AMRMClient<AMRMClient.ContainerRequest> rmClient;
    private NMClient nmClient;
    private Configuration conf;

    private ByteBuffer allTokens;
    private int progress;
    private String defaultUser;
    private String masterIp;
    private String masterHostname;
    private String RPCServerFile;
    private int serverPort;
    private String serverAddress;
    private WebServer webServer;


    public static void main(String[] args){

        String serverAddress;
        try {
            serverAddress = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            serverAddress = "localhost";
        }

        int serverPort = 9942;
        while (PortIsInUse(serverAddress, serverPort))
            serverPort += 1;


        YarnApplicationMaster yam = new YarnApplicationMaster(args[0], args[1], args[2]);
        yam.startServer(serverAddress, serverPort);
    }

    public static void log(String msg){
        System.out.println(msg);
    }

    public void startServer(String serverAddress, int serverPort){
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;


        try {

            webServer = new WebServer(serverPort);

            XmlRpcServer server = webServer.getXmlRpcServer();
            PropertyHandlerMapping propHandlerMapping = new PropertyHandlerMapping();
            propHandlerMapping.addHandler("default", YarnApplicationMaster.class);
            server.setHandlerMapping(propHandlerMapping);

            XmlRpcServerConfigImpl serverConfig = (XmlRpcServerConfigImpl) server.getConfig();

            serverConfig.setEnabledForExtensions(false);
            serverConfig.setContentLengthOptional(false);
            serverConfig.setKeepAliveEnabled(true);

            webServer.start();

            init();
            waitRegistration();

            log("XML-RPC Server started on port "+serverPort);

        } catch (Exception exception){
            log("JavaServer: " + exception);

        }
    }


    /**
     * Creates a new Yarn Framework scheduler.
     */
    public YarnApplicationMaster(String defaultUser, String masterHostname, String masterIp) {
        log("Initialize " + this.getClass().getName());
        progress = 0;

        this.defaultUser = defaultUser;
        this.masterHostname = masterHostname;
        this.masterIp = masterIp;

        runningTasks = Collections.synchronizedList(new LinkedList<String>());
        pendingTasks = Collections.synchronizedList(new LinkedList<String>());
        tasks = Collections.synchronizedMap(new HashMap<String, YarnTask>());

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
        RPCServerFile = getRPCServerFile();

    }

    public void stopFramework() {
        try {
            File f= new File(RPCServerFile);
            f.delete();

            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
                    "COMPSs Execution Completed",
                    "");
        }catch (YarnException | IOException ignored) {
            System.exit(1);
        }

        System.exit(0);

    }


    /**
     * Petition to create a worker on Yarn, uses a docker image specified by imageName and will use resources specified
     * in list to be created.
     *

     * @param workerId Docker Container Id.
     * @param imageName Docker image name.
     * @return Yarn container Identifier generated for that worker.
     */
    public synchronized String requestWorker(String workerId, String imageName, int VCores, int Memory,
                                             String publicKey, String dockerNetwork) throws IOException {
        log("Requested worker");
        String containerId = "";
        pendingTasks.add(workerId);

        int startedContainer = 0;

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource requirements = Records.newRecord(Resource.class);
        requirements.setMemorySize(Memory);
        requirements.setVirtualCores(VCores);

        // Make container requests to ResourceManager
        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(requirements,
                null, null, priority);

        rmClient.addContainerRequest(containerAsk);

        AllocateResponse response = null;
        while (startedContainer == 0) {
            try {
                response = rmClient.allocate(progress++);
            } catch (YarnException | IOException e) {
                e.printStackTrace();
            }

            List<Container> containers = response.getAllocatedContainers();

            for (Container container : containers) {
                containerId = container.getId().toString();

                Map<String, LocalResource> localResources = new HashMap<>();
                LocalResource rpc_server = Records.newRecord(LocalResource.class);
                conf.set("fs.defaultFS", "file:///");
                FileSystem fs =  FileSystem.get(conf);
                Path dst = new Path("file://"+RPCServerFile);
                FileStatus destStatus = fs.getFileStatus(dst);
                rpc_server.setType(LocalResourceType.FILE);
                rpc_server.setVisibility(LocalResourceVisibility.PUBLIC);
                rpc_server.setResource(URL.fromPath(dst));
                rpc_server.setTimestamp(destStatus.getModificationTime());
                rpc_server.setSize(-1);
                localResources.put("rpc_server.py", rpc_server);

                Vector<CharSequence> vargs = new Vector<CharSequence>(30);
                vargs.add("docker run -t");
                vargs.add("--cpus=" + VCores);
                vargs.add("--memory=" + Memory + "m");
                vargs.add("--network=" + dockerNetwork);
                vargs.add("--entrypoint='python3'");
                vargs.add("--name=" + workerId);
                vargs.add("--volume=$PWD/rpc_server.py:/tmp/rpc_server.py");
                vargs.add(imageName);
                vargs.add("/tmp/rpc_server.py");
                vargs.add("'" + defaultUser +"' '" + publicKey + "' '"+ masterHostname +"' '"+ masterIp + "'");
                vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
                vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

                StringBuilder command = new StringBuilder();
                for (CharSequence str : vargs) {
                    command.append(str).append(" ");
                }
                log(command.toString());

                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setLocalResources(localResources);
                ctx.setTokens(allTokens.duplicate());
                ctx.setCommands(Collections.singletonList(command.toString()));

                log("Launching container " + containerId);

                try {
                    nmClient.startContainer(container, ctx);
                } catch (YarnException | IOException e) {
                    e.printStackTrace();
                }

                YarnTask yt = new YarnTask(workerId, ctx,
                        containerId,
                        container.getNodeId().toString());

                startedContainer++;
                tasks.put(workerId, yt);

            }
        }

        return containerId;
    }

    private String getRPCServerFile() {
        String filepath = "/tmp/rpc_server_"+ UUID.randomUUID().toString()+".py";
        InputStream inputStream = getClass().getResourceAsStream("/rpc/rpc_server.py");
        FileOutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(new File(filepath));
            IOUtils.copy(inputStream, outputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(outputStream);
        }

        return filepath;
    }


    /**
     * Wait for task with identifier to reach RUNNING state. If state is not reached, task is removed from pending and
     * running tasks.
     *
     * @param id Task identifier to wait for.
     * @param timeout Timeout in seconds.
     * @throws FrameworkException if waits for timeout units.
     */
    public synchronized String waitTask(String id, int timeout) throws FrameworkException, IOException, YarnException {

        String ip = UNDEFINED_IP;
        log("Waiting task "+ id);

        //TODO: add timeout
        //TimeUnit unit = TimeUnit.valueOf(unit);

        String result = "";

        if (!tasks.containsKey(id)) {
            throw new FrameworkException(ERROR_TASK_ID + id);
        }

        YarnTask task = tasks.get(id);

        waitDockerContainerIsReady(task, timeout);

        String nmPort = System.getenv("NM_HTTP_PORT");
        String nmHost = System.getenv("NM_HOST");

        String url = "http:///" + nmHost + ":" + nmPort +
                "/ws/v1/node/containerlogs/" + task.getContainerId() + "/stdout";

        while (!result.contains("ip:")) {
            Runtime rt = Runtime.getRuntime();
            Process pr = rt.exec("curl -s -S " + url);

            result = new BufferedReader(
                    new InputStreamReader(pr.getInputStream()))
                    .lines()
                    .collect(Collectors.joining("\n"));
        }

        Pattern pattern = Pattern.compile("ip:.*\n", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(result);
        boolean matchFound = matcher.find();

        if (matchFound) {
            ip = matcher.group()
                    .replace("ip:", "")
                    .replace(" ", "")
                    .replace("\n", "");

            task.setIp(ip);
            log("Container is ready with ip " + ip);

            tasks.put(id, task);
            pendingTasks.remove(id);
            runningTasks.add(id);
        }

        return ip;
    }

    public void waitDockerContainerIsReady(YarnTask task, int timeout) throws IOException, YarnException {
        log("Checking if Yarn container is running");

        String nodeId = task.getNodeId();
        ContainerId container = ContainerId.fromString(task.getContainerId());
        NodeId node = NodeId.fromString(nodeId);

        ContainerStatus cs = nmClient.getContainerStatus(container, node);
        boolean isReady = cs.getState().equals(ContainerState.RUNNING);

        int retry = 0;
        while (!isReady){
            retry++;
            if (retry % 100 == 0)
                log("Waiting Docker Container (" + task.getId() + ") - retry: " + retry);
            cs = nmClient.getContainerStatus(container, node);
            isReady = cs.getState().equals(ContainerState.RUNNING);

        }

    }

    /**
     * Wait for the framework to register in Yarn. If it is already registered returns immediately.
     */
    public void waitRegistration() throws IOException, YarnException {
        log("Wait for framework to register");
        rmClient.registerApplicationMaster(serverAddress, serverPort, "");
        log("Framework is registered");

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
    public boolean removeTask(String id, int timeout) throws FrameworkException, IOException, YarnException {

        synchronized (this) {
            // Task still in pending queue, not launched to run in Yarn
            if (pendingTasks.contains(id)) {
                log("Task still in pending queue, not launched to run in Yarn");
                pendingTasks.remove(id);
                return false; //checar pending
            } else if (!tasks.containsKey(id)) {
                runningTasks.remove(id);
                throw new FrameworkException(ERROR_TASK_ID + id);
            }
        }

        YarnTask task  = tasks.get(id);
        String nodeId = task.getNodeId();
        String containerId = task.getContainerId();

        // stopping Docker Container
        Runtime rt = Runtime.getRuntime();
        Process pr = rt.exec("docker rm -f " + id);

        String result = new BufferedReader(
                new InputStreamReader(pr.getInputStream()))
                .lines()
                .collect(Collectors.joining("\n"));

        if (result.equals(id)){
            log("Docker Container (" + id + ") is removed.");
            // stopping Yarn Container
            nmClient.stopContainer(ContainerId.fromString(containerId), NodeId.fromString(nodeId));
            //waitTask(id, TaskState.TASK_KILLED, timeout, unit);
            tasks.remove(id);
            return true;
        }

        return false;
    }

}

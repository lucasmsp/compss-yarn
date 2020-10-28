package br.ufmg.dcc.clients.yarn.framework;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;
import br.ufmg.dcc.clients.yarn.framework.log.Loggers;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import br.ufmg.dcc.clients.yarn.framework.rpc.CustomClientFactory;
import br.ufmg.dcc.clients.yarn.framework.rpc.YarnApplicationMasterInterface;
import com.jcraft.jsch.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;


/**
 * Representation of a Yarn Framework client
 *
 */
public class YarnApplicationClient {

    private static final String SERVER_IP = "Server";

    private static final String YARN_FRAMEWORK_NAME = "yarn-framework-name";
    private static final String YARN_FRAMEWORK_NAME_DEFAULT = "COMPSs_Framework";

    private static final String YARN_QUEUE = "yarn-queue";
    private static final String YARN_DEFULT_QUEUE = "default";

    private static final String YARN_FRAMEWORK_APPLICATION_TIMEOUT = "yarn-framework-application-timeout";
    private static final String YARN_FRAMEWORK_APPLICATION_TIMEOUT_DEFAULT = "7200"; // 2 hours

    private static final String YARN_WORKER_WAIT_TIMEOUT = "yarn-worker-wait-timeout";
    private static final String YARN_WORKER_KILL_TIMEOUT = "yarn-worker-kill-timeout";
    private static final String DEFAULT_TIMEOUT = "180"; // 3 minutes

    private static final String YARN_CONNECTOR_PATH = "yarn-hdfs-connector";

    private static final String MAX_CONNECTION_ERRORS = "max-connection-errors";
    private static final String DEFAULT_MAX_CONNECTION_ERRORS = "360";

    private static final String YARN_DOCKER_NETWORK_NAME = "yarn-docker-network-name";
    private static final String YARN_DOCKER_NETWORK_BRIDGE = "bridge";

    private static final String VM_USER = "vm-user";
    private static final String VM_USER_DEFAULT = "root";

    private static final String KEYPAIR_NAME = "vm-keypair-name";
    private static final String KEYPAIR_NAME_DEFAULT = "id_rsa";
    private static final String KEYPAIR_LOCATION = "vm-keypair-location";
    private static final String KEYPAIR_LOCATION_DEFAULT = "/home/" + System.getProperty("user.name") + "/.ssh";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.YARN_CLIENT);
    private static final String UNDEFINED_IP = "-1.-1.-1.-1";

    private Configuration conf;
    private YarnClient client;
    private ApplicationId applicationId;

    private int rpcPort;
    private String rpcAddress;
    private String masterHostname;
    private YarnApplicationMasterInterface yam;

    private final AtomicInteger taskIdGenerator = new AtomicInteger();
    private HashMap<String, String> containerMaps;

    private final String applicationName;
    private final int waitRequestTimeout;
    private final int killWorkerTimeout;
    private final String publicKey;
    private final String networkDocker;
    private final String hdfsJarPath;
    private final String defaultUser;
    private final String keyPairPath;


    /**
     * Creates a new YarnFramework client with the given properties
     *
     * @param props
     * @throws FrameworkException
     */
    public YarnApplicationClient(Map<String, String> props) throws FrameworkException {
        LOGGER.info("Starting Yarn Connector");

        String yarnMasterIp;
        if (props.containsKey(SERVER_IP)){
            yarnMasterIp = props.get(SERVER_IP).replace("yarn://", "");
        }else{
            throw new FrameworkException("Missing Yarn master IP");
        }

        int applicationTimeout = Integer.parseInt(
                props.getOrDefault(YARN_FRAMEWORK_APPLICATION_TIMEOUT, YARN_FRAMEWORK_APPLICATION_TIMEOUT_DEFAULT));
        this.waitRequestTimeout = Integer.parseInt(props.getOrDefault(YARN_WORKER_WAIT_TIMEOUT, DEFAULT_TIMEOUT));
        this.killWorkerTimeout = Integer.parseInt(props.getOrDefault(YARN_WORKER_KILL_TIMEOUT, DEFAULT_TIMEOUT));
        int maxAttempt = Integer.parseInt(props.getOrDefault(MAX_CONNECTION_ERRORS, DEFAULT_MAX_CONNECTION_ERRORS));

        this.applicationName = props.getOrDefault(YARN_FRAMEWORK_NAME, YARN_FRAMEWORK_NAME_DEFAULT);
        LOGGER.info("Setting name for the framework: " + applicationName);

        String queue = props.getOrDefault(YARN_QUEUE, YARN_DEFULT_QUEUE);
        LOGGER.info("Setting yarn queue: " + queue);

        this.networkDocker = props.getOrDefault(YARN_DOCKER_NETWORK_NAME, YARN_DOCKER_NETWORK_BRIDGE);
        LOGGER.info("Setting network for Docker: " + this.networkDocker);

        this.defaultUser = props.getOrDefault(VM_USER, VM_USER_DEFAULT);
        LOGGER.info("Setting docker image user: " + this.defaultUser);

        if (props.containsKey(YARN_CONNECTOR_PATH))
            this.hdfsJarPath = props.get(YARN_CONNECTOR_PATH);
        else{
            throw new FrameworkException("Missing Yarn connector path in HDFS");
        }

        String keyPairName = props.getOrDefault(KEYPAIR_NAME, KEYPAIR_NAME_DEFAULT);
        String keyPairLocation = props.getOrDefault(KEYPAIR_LOCATION, KEYPAIR_LOCATION_DEFAULT);

        String checkedKeyPairLocation = (keyPairLocation.equals("~/.ssh")) ? KEYPAIR_LOCATION_DEFAULT: keyPairLocation;
        this.keyPairPath = checkedKeyPairLocation + File.separator + keyPairName;
        this.publicKey = getPublicKey();

        try {
            this.masterHostname = InetAddress.getLocalHost().getHostName();
            LOGGER.info("Setting Yarn/COMPSs master node as "+ masterHostname);
        } catch (IOException ignored) {
        }

        this.containerMaps =  new HashMap<>();
        this.client = YarnClient.createYarnClient();
        this.conf = new YarnConfiguration();

        try {
            client.init(this.conf);
            client.start();
        }catch (ServiceStateException e){
            throw new FrameworkException("Yarn configuration was null, the state change not permitted, " +
                    "or something else went wrong. Try to add `$HADOOP_HOME/bin/hdfs classpath --glob` " +
                    "in your CLASSPATH");
        }
        LOGGER.info("Connected with Yarn cluster.");
        startYarnMasterApplication(yarnMasterIp, applicationTimeout, maxAttempt, queue);
        startRPCClient();

    }

    public void startYarnMasterApplication(String yarnMasterIp, int applicationTimeout,
                                           int maxAttempt, String queue) throws FrameworkException {

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setLocalResources(initResource());

        Map<String, String> environment = new HashMap<>();
        initEnvironment(environment);
        amContainer.setEnvironment(environment);

        Vector<CharSequence> vargs = new Vector<>(7);
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("br.ufmg.dcc.clients.yarn.framework.YarnApplicationMaster");
        vargs.add(this.masterHostname);
        vargs.add(yarnMasterIp);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        amContainer.setCommands(Collections.singletonList(command.toString()));

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(256);
        capability.setVirtualCores(1);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(1);

        // YARN supports for one timeout type i.e LIFETIME and corresponding timeout value in seconds.
        Map<ApplicationTimeoutType, Long> applicationTimeouts = new HashMap<>();
        applicationTimeouts.put(ApplicationTimeoutType.LIFETIME, (long) applicationTimeout);

        try {

            // Setup security tokens
            if (UserGroupInformation.isSecurityEnabled()) {
                ByteBuffer fsTokens = setupSecurity();
                amContainer.setTokens(fsTokens);
            }
;
            YarnClientApplication app = client.createApplication();
            ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
            appContext.setApplicationName(applicationName);
            appContext.setMaxAppAttempts(maxAttempt);
            appContext.setAMContainerSpec(amContainer);
            appContext.setResource(capability);
            appContext.setPriority(priority);
            appContext.setQueue(queue);
            appContext.setApplicationTimeouts(applicationTimeouts);

            // Submit application
            applicationId = appContext.getApplicationId();
            LOGGER.info("Initializing Yarn Client by applicationId " + applicationId);

            client.submitApplication(appContext);

        } catch (Exception exception) {
            exception.printStackTrace();
            throw new FrameworkException("Error while trying to start a Yarn Application Master");
        }

        try {

            ApplicationReport appReport = client.getApplicationReport(applicationId);
            YarnApplicationState appState = appReport.getYarnApplicationState();

            while (appState != YarnApplicationState.RUNNING) {
                appReport = client.getApplicationReport(applicationId);
                appState = appReport.getYarnApplicationState();
            }

            this.rpcPort = appReport.getRpcPort();
            this.rpcAddress = appReport.getHost();

        } catch (YarnException | IOException e) {
            throw new FrameworkException("Error while trying to retrieve the Yarn Application Master status");
        }


    }

    public void startRPCClient() throws FrameworkException {
        try {
            Thread.sleep(1000);

            String url = "http://" + this.rpcAddress + ":" + this.rpcPort;
            LOGGER.debug("Connecting with Yarn Client via RPC ("+url+")");

            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new java.net.URL(url));
            config.setConnectionTimeout(1000 * 1000);
            config.setReplyTimeout(6000 * 1000);

            XmlRpcClient rpcClient = new XmlRpcClient();
            rpcClient.setConfig(config);

            CustomClientFactory factory = new CustomClientFactory(rpcClient);

            yam = (YarnApplicationMasterInterface) factory.newInstance(YarnApplicationMasterInterface.class);

        } catch (Exception exception) {
            throw new FrameworkException("Error while trying to start an RPC service with Yarn Application Master");
        }
    }

    public String getPublicKey() throws FrameworkException {

        String filepath = this.keyPairPath + ".pub";
        LOGGER.debug("Copying " + filepath + " to be used by Docker Containers.");

        try {
            File file = new File(filepath);
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fis.close();
            return new String(data, StandardCharsets.UTF_8).replace("\n", "");

        } catch (IOException e) {
            throw new FrameworkException("Public key not found in " + filepath);
        }

    }

    public ByteBuffer setupSecurity() throws IOException {

        Credentials credentials = new Credentials();
        String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException(
                    "Can't get Master Kerberos principal for the RM to use as renewer");
        }
        FileSystem fs = FileSystem.get(conf);
        // For now, only getting tokens for the default file-system.
        final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer,
                credentials);
        if (tokens != null) {
            for (Token<?> token : tokens) {
                LOGGER.debug("Got dt for " + fs.getUri() + "; "+ token);

            }
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);

        return ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    public Map<String, LocalResource> initResource() throws FrameworkException {
        Map<String, LocalResource> localResources = new HashMap<>();

        try {
            LocalResource amJarSrc = Records.newRecord(LocalResource.class);
            Configuration ConfHadoop = new Configuration();
            ConfHadoop.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            ConfHadoop.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

            Path jarPath = new Path(this.hdfsJarPath);
            FileStatus destStatus = jarPath.getFileSystem(ConfHadoop).getFileStatus(jarPath);

            amJarSrc.setType(LocalResourceType.FILE);
            amJarSrc.setVisibility(LocalResourceVisibility.PUBLIC);
            amJarSrc.setResource(URL.fromPath(jarPath));
            amJarSrc.setTimestamp(destStatus.getModificationTime());
            amJarSrc.setSize(destStatus.getLen());
            localResources.put("yarn-conn.jar", amJarSrc);

        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
            throw new FrameworkException("Yarn connector jar could not be sent to Yarn container");
        }
        return localResources;
    }

    public void initEnvironment(Map<String, String> environment) {
        StringBuilder classPathEnv = new StringBuilder(
                ApplicationConstants.Environment.CLASSPATH.$$()).append(
                ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

        for (String c : conf
                .getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        environment.put("CLASSPATH", classPathEnv.toString());

    }

    /**
     * @return Framework identifier returned by Yarn.
     */
    public String getId() {
        return applicationId.toString();
    }

    /**
     * Request a worker to be run on Yarn.
     *
     * @param imageName Docker image name
     * @param cpus number of cores inside each new container
     * @param memory number in mega bytes inside each new container
     * @return Identifier assigned to new worker
     */
    public synchronized String requestWorker(String imageName, int cpus, int memory) {
        String workerId = generateWorkerId(applicationName, this.getId());
        LOGGER.debug("Requesting a yarn container with id "+workerId+" which has "+cpus+"vcpus and "+memory+"mb.");
        String containerId = yam.requestWorker(this.waitRequestTimeout, workerId, imageName, cpus, memory,
                this.publicKey, this.networkDocker, this.defaultUser);

        containerMaps.put(workerId, containerId);
        LOGGER.info("Docker worker "+ workerId + " created in Yarn container " + containerId);
        return workerId;
    }

    /**
     * @param appName Application name
     * @return Unique identifier for a worker.
     */
    public synchronized String generateWorkerId(String appName, String appId) {
        return appName.replace(" ", "_") + "-" +
                appId.replace("application_", "") + "-" + taskIdGenerator.incrementAndGet();
    }


    /**
     * Wait for worker with identifier id.
     *
     * @param id Worker identifier.
     * @return Worker IP address.
     */
    public synchronized String waitWorkerUntilRunning(String id)  {
        LOGGER.debug("Waiting worker with id " + id);
        try {
            return yam.waitTask(waitRequestTimeout, id);
        } catch (FrameworkException e) {
            return UNDEFINED_IP;
        }
    }

    /**
     * Stop worker running/staging in Yarn.
     *
     * @param id Worker identifier
     */
    public synchronized  void removeWorker(String id) throws FrameworkException {
        LOGGER.debug("Remove worker with id " + id);
        String hostContainer = "";

        // stopping yarn container
        try {
            hostContainer = yam.removeTask(killWorkerTimeout, id);
        } catch (FrameworkException e) {
            e.printStackTrace();
        }
        if (hostContainer.length()>0)
            LOGGER.debug("Yarn container is removed. Trying to remove its docker container.");

        // stopping docker container
        String command = "docker rm -f "+id;
        JSch jsch = new JSch();
        Session session = null;
        Channel channel = null;
        String result = "";

        try {
            jsch.addIdentity(this.keyPairPath);
            session = jsch.getSession(System.getProperty("user.name"), hostContainer, 22);
            session.setConfig("StrictHostKeyChecking", "no");

            session.connect();

            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(channel.getInputStream()));
            channel.connect();

            StringBuilder sBuilder = new StringBuilder();
            String read = reader.readLine();
            while (read != null) {
                read = reader.readLine();
                sBuilder.append(read);
            }

            channel.disconnect();
            session.disconnect();

            result = sBuilder.toString();
            if (result.contains(id)){
                LOGGER.debug("Worker " + id + " removed with success.");
            }

        } catch (IOException | JSchException e) {
            e.printStackTrace();
            throw new FrameworkException("Yarn Connector failed to remove docker container named " + id);
        }


    }

    /**
     * Stop the Yarn Framework.
     */
    public void stop() throws FrameworkException {
        LOGGER.debug("Stopping Yarn Framework");

        try {
            yam.stopFramework(killWorkerTimeout);

            ApplicationReport appReport = client.getApplicationReport(applicationId);
            YarnApplicationState appState = appReport.getYarnApplicationState();

            while (appState != YarnApplicationState.FINISHED
                    && appState != YarnApplicationState.KILLED
                    && appState != YarnApplicationState.FAILED) {
                Thread.sleep(100);
                appReport = client.getApplicationReport(applicationId);
                appState = appReport.getYarnApplicationState();
            }

            client.stop();

        }catch (InterruptedException | IOException | YarnException e) {
            throw new FrameworkException("The Yarn Framework could not be gracefully closed");
        }

    }


}

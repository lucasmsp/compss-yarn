package br.ufmg.dcc.clients.yarn.framework;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;
import br.ufmg.dcc.clients.yarn.framework.log.Loggers;

import java.io.*;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import br.ufmg.dcc.clients.yarn.framework.rpc.CustomClientFactory;
import br.ufmg.dcc.clients.yarn.framework.rpc.YarnApplicationMasterInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
    private static final String YARN_FRAMEWORK_NAME_DEFAULT = "COMPSs Framework";

    private static final String YARN_WORKER_NAME = "yarn-worker-name";
    private static final String YARN_WORKER_NAME_DEFAULT = "yarn-worker";

    private static final String YARN_QUEUE = "yarn-queue";
    private static final String YARN_DEFULT_QUEUE = "default";

    private static final String YARN_FRAMEWORK_APPLICATION_TIMEOUT = "yarn-framework-application-timeout";
    private static final String YARN_FRAMEWORK_APPLICATION_TIMEOUT_DEFAULT = "7200"; // 2 hours

    private static final String YARN_WORKER_WAIT_TIMEOUT = "yarn-worker-wait-timeout";
    private static final String YARN_WORKER_KILL_TIMEOUT = "yarn-worker-kill-timeout";
    private static final String DEFAULT_TIMEOUT = "180"; // 3 minutes

    private static final String MAX_CONNECTION_ERRORS = "max-connection-errors";
    private static final String DEFAULT_MAX_CONNECTION_ERRORS = "360";

    private static final String YARN_DOCKER_NETWORK_NAME = "yarn-docker-network-name";
    private static final String YARN_DOCKER_NETWORK_BRIDGE = "bridge";

    private static final String VM_USER = "vm-user";
    private static final String VM_USER_DEFAULT = "root";
    private static final String VM_PASS = "vm-password";

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

    private final String workerName;
    private final String applicationName;
    private final int runWorkerTimeout;
    private final int killWorkerTimeout;
    private final String defaultPassword;
    private final String publicKey;
    private final String networkDocker;


    /**
     * Creates a new YarnFramework client with the given properties
     *
     * @param props
     * @throws FrameworkException
     */
    public YarnApplicationClient(Map<String, String> props) throws FrameworkException {
        LOGGER.info("Starting Yarn Connector");

        conf = new YarnConfiguration();
        conf.set("fs.defaultFS", "file:///");
        this.containerMaps =  new HashMap<>();
        this.client = YarnClient.createYarnClient();

        String yarnMasterIp;
        if (props.containsKey(SERVER_IP)){
            yarnMasterIp = props.get(SERVER_IP).replace("yarn://", "");
        }else{
            throw new FrameworkException("Missing Yarn master IP");
        }

        int applicationTimeout = Integer.parseInt(
                props.getOrDefault(YARN_FRAMEWORK_APPLICATION_TIMEOUT, YARN_FRAMEWORK_APPLICATION_TIMEOUT_DEFAULT));
        this.runWorkerTimeout = Integer.parseInt(props.getOrDefault(YARN_WORKER_WAIT_TIMEOUT, DEFAULT_TIMEOUT));
        this.killWorkerTimeout = Integer.parseInt(props.getOrDefault(YARN_WORKER_KILL_TIMEOUT, DEFAULT_TIMEOUT));
        int maxAttempt = Integer.parseInt(props.getOrDefault(MAX_CONNECTION_ERRORS, DEFAULT_MAX_CONNECTION_ERRORS));

        this.applicationName = props.getOrDefault(YARN_FRAMEWORK_NAME, YARN_FRAMEWORK_NAME_DEFAULT);
        LOGGER.info("Setting name for the framework: " + applicationName);

        this.workerName = props.getOrDefault(YARN_WORKER_NAME, YARN_WORKER_NAME_DEFAULT);
        LOGGER.info("Setting name for the workers: " + workerName);

        String queue = props.getOrDefault(YARN_QUEUE, YARN_DEFULT_QUEUE);
        LOGGER.info("Setting yarn queue: " + queue);

        this.networkDocker = props.getOrDefault(YARN_DOCKER_NETWORK_NAME, YARN_DOCKER_NETWORK_BRIDGE);
        LOGGER.info("Using custom network for Docker: " + this.networkDocker);

        String defaultUser = props.getOrDefault(VM_USER, VM_USER_DEFAULT);
        this.defaultPassword = props.getOrDefault(VM_PASS, "");

        this.publicKey = getPublicKey(props);

        try {
            this.masterHostname = InetAddress.getLocalHost().getHostName();
            LOGGER.info("Setting Yarn/COMPSs master node as "+ masterHostname);
        } catch (IOException ignored) {
        }

        client.init(conf);
        client.start();

        startYarnMasterApplication(defaultUser, yarnMasterIp, applicationTimeout, maxAttempt, queue);
        startRPCClient();

    }

    public void startYarnMasterApplication(String defaultUser, String yarnMasterIp, int applicationTimeout,
                                           int maxAttempt, String queue) throws FrameworkException {

        try {

            ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

            Map<String, LocalResource> localResources = initResource();
            amContainer.setLocalResources(localResources);

            Map<String, String> environment = new HashMap<>();
            initEnvironment(environment);
            amContainer.setEnvironment(environment);

            Vector<CharSequence> vargs = new Vector<>(30);
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
            vargs.add("br.ufmg.dcc.clients.yarn.framework.YarnApplicationMaster");
            vargs.add(defaultUser);
            vargs.add(this.masterHostname);
            vargs.add(yarnMasterIp);
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            amContainer.setCommands(Collections.singletonList(command.toString()));

            // Setup security tokens
            if (UserGroupInformation.isSecurityEnabled()) {
                ByteBuffer fsTokens = setupSecurity();
                amContainer.setTokens(fsTokens);
            }

            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(256);
            capability.setVirtualCores(1);

            Priority priority = Records.newRecord(Priority.class);
            priority.setPriority(1);

            // YARN supports for one timeout type i.e LIFETIME and corresponding timeout value in seconds.
            Map<ApplicationTimeoutType, Long> applicationTimeouts = new HashMap<>();
            applicationTimeouts.put(ApplicationTimeoutType.LIFETIME, (long) applicationTimeout);

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
            config.setConnectionTimeout(100 * 1000);
            config.setReplyTimeout(600 * 1000);

            XmlRpcClient rpcClient = new XmlRpcClient();
            rpcClient.setConfig(config);

            CustomClientFactory factory = new CustomClientFactory(rpcClient);

            yam = (YarnApplicationMasterInterface) factory.newInstance(YarnApplicationMasterInterface.class);

        } catch (Exception exception) {
            throw new FrameworkException("Error while trying to start an RPC service with Yarn Application Master");
        }
    }

    public String getPublicKey(Map<String, String> props) throws FrameworkException {

        String keyPairName = props.getOrDefault(KEYPAIR_NAME, KEYPAIR_NAME_DEFAULT);
        String keyPairLocation = props.getOrDefault(KEYPAIR_LOCATION, KEYPAIR_LOCATION_DEFAULT);

        String checkedKeyPairLocation = (keyPairLocation.equals("~/.ssh")) ? KEYPAIR_LOCATION_DEFAULT: keyPairLocation;
        String filepath = checkedKeyPairLocation + File.separator + keyPairName + ".pub";
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

        String jarPath = "file://";
        try {
            jarPath +=
                    new File(YarnApplicationClient.class.getProtectionDomain().getCodeSource().getLocation()
                            .toURI()).getPath();

        } catch (URISyntaxException e) {
            jarPath += "/opt/COMPSs/Runtime/cloud-conn/yarn-conn.jar";
        }

        try {
            LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

            Path dst = new Path(jarPath);
            FileStatus destStatus = FileSystem.get(conf).getFileStatus(dst);
            amJarRsrc.setType(LocalResourceType.FILE);
            amJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
            amJarRsrc.setResource(URL.fromPath(dst));
            amJarRsrc.setTimestamp(destStatus.getModificationTime());
            amJarRsrc.setSize(destStatus.getLen());
            localResources.put("yarn-conn.jar", amJarRsrc);

        } catch (IllegalArgumentException | IOException e) {
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
        LOGGER.info("Get Application ID");
        return applicationId.toString();
    }

    /**
     * Request a worker to be run on Yarn.
     *
     * @return Identifier assigned to new worker
     */
    public synchronized String requestWorker(String imageName, int cpus, int memory) {

        LOGGER.info("Requested a worker");
        String workerId = generateWorkerId(applicationName, workerName, this.getId());
        String containerId = yam.requestWorker(runWorkerTimeout, workerId, imageName, cpus, memory,
                this.publicKey, this.networkDocker);

        containerMaps.put(workerId, containerId);
        LOGGER.info("Docker worker "+ workerId + " created in Yarn container " + containerId);
        return workerId;
    }

    /**
     * @param appName Application name
     * @return Unique identifier for a worker.
     */
    public synchronized String generateWorkerId(String appName, String workerName, String appId) {
        return appName.replace(" ", "_") +"_"+workerName+ "-" +
                appId.replace("application_", "") + "-" + taskIdGenerator.incrementAndGet();
    }


    /**
     * Wait for worker with identifier id.
     *
     * @param id Worker identifier.
     * @return Worker IP address.
     */
    public synchronized String waitWorkerUntilRunning(String id)  {
        LOGGER.info("Waiting worker with id " + id);
        try {
            return yam.waitTask(runWorkerTimeout, id);
        } catch (FrameworkException e) {
            return UNDEFINED_IP;
        }
    }

    /**
     * Stop worker running/staging in Yarn.
     *
     * @param id Worker identifier
     */
    public synchronized  void removeWorker(String id)  {
        LOGGER.debug("Remove worker with id " + id);
        boolean success = false;
        try {
            success = yam.removeTask(killWorkerTimeout, id);
        } catch (FrameworkException ignored) {
        }
        LOGGER.debug("Remove worker with id " + id + ": " + success);
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

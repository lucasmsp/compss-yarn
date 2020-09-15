package br.ufmg.dcc.clients.yarn.framework;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;
import br.ufmg.dcc.clients.yarn.framework.log.Loggers;


import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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


/**
 * Representation of a Yarn Framework client
 *
 */
public class YarnApplicationClient {

    private static final int FAILOVER_TIMEOUT = 120_000;

    // 3 min default timeout for all register, wait run, kill
    private static final String DEFAULT_TIMEOUT = "180000";
    private static final String DEFAULT_TIMEOUT_UNITS = "MILLISECONDS";

    private static final String TRUE = "true";

    private static final String SERVER_IP = "Server";

    // Yarn Framework options
    private static final String YARN_FRAMEWORK_NAME = "yarn-framework-name";
    private static final String YARN_FRAMEWORK_NAME_DEFAULT = "COMPSs Framework";

    private static final String MESOS_CHECKPOINT = "mesos-checkpoint";
    private static final String MESOS_AUTHENTICATE = "mesos-authenticate";
    private static final String MESOS_PRINCIPAL = "mesos-principal";
    private static final String MESOS_SECRET = "mesos-secret";


    private static final String YARN_QUEUE = "yarn-queue";
    private static final String YARN_DEFULT_QUEUE = "default";

    private final String runWorkerTimeoutUnits;
    private final String killWorkerTimeoutUnits;
    private static final String YARN_WORKER_NAME = "yarn-worker-name";
    private static final String YARN_WORKER_NAME_DEFAULT = "yarn-worker";
    private static final String YARN_FRAMEWORK_HOSTNAME = "yarn-framework-hostname";
    private static final String YARN_FRAMEWORK_REGISTER_TIMEOUT = "yarn-framework-register-timeout";
    private static final String YARN_FRAMEWORK_REGISTER_TIMEOUT_UNITS = "yarn-framework-register-timeout-units";
    private static final String YARN_WORKER_WAIT_TIMEOUT = "yarn-worker-wait-timeout";
    private static final String YARN_WORKER_WAIT_TIMEOUT_UNITS = "yarn-worker-wait-timeout-units";
    private static final String YARN_WORKER_KILL_TIMEOUT = "yarn-worker-kill-timeout";
    private static final String YARN_WORKER_KILL_TIMEOUT_UNITS = "yarn-worker-kill-timeout-units";
    private static final String MAX_CONNECTION_ERRORS = "max-connection-errors";

    private static final String YARN_DOCKER_NETWORK_NAME = "yarn-docker-network-name";
    private static final String YARN_DOCKER_NETWORK_HOST = "host";
    private static final String YARN_DOCKER_NETWORK_BRIDGE = "bridge";

    private static final String VM_USER = "vm-user";
    private static final String VM_PASS = "vm-password";
    private static final String VM_KEYPAIR_NAME = "vm-keypair-name";
    private static final String VM_KEYPAIR_LOCATION = "vm-keypair-location";
    private static final String DEFAULT_DEFAULT_USER = "root";
    private static final String DEFAULT_KEYPAIR_NAME = "id_rsa";
    private static final String DEFAULT_KEYPAIR_LOCATION = System.getProperty("user.home") + File.separator + ".ssh";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.YARN_CLIENT);

    private Configuration conf;
    private YarnClient client;
    private ApplicationSubmissionContext appContext;

    private final String workerName;
    private final String queue;
    private final String applicationName;
    private ApplicationId applicationId = null;
    private YarnDriver driver;
    private final AtomicInteger taskIdGenerator = new AtomicInteger();

    private final String defaultUser;
    private final String defaultPassword;
    private final String keyPairName;
    private final String keyPairLocation;
    private String publicKey = "";
    private String masterHostname;
    private final String yarnMasterIp;
    private final String networkDocker;
    private int serverPort;
    private String applicationMasterHost;
    private final int maxAttempt;
    private HashMap<String, String> containerMaps;
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

        if (props.containsKey(SERVER_IP)){
            this.yarnMasterIp = props.get(SERVER_IP).replace("yarn://", "");
        }else{
            throw new FrameworkException("Missing Yarn master IP");
        }

        this.runWorkerTimeoutUnits = this.getProperty(props, YARN_WORKER_WAIT_TIMEOUT_UNITS, "MILLISECONDS");
        this.killWorkerTimeoutUnits = this.getProperty(props, YARN_WORKER_KILL_TIMEOUT_UNITS, "MILLISECONDS");
        this.maxAttempt = Integer.parseInt(this.getProperty(props, MAX_CONNECTION_ERRORS, "360"));

        this.applicationName = this.getProperty(props, YARN_FRAMEWORK_NAME, YARN_FRAMEWORK_NAME_DEFAULT);
        LOGGER.info("Setting name for the framework: " + applicationName);

        this.workerName = this.getProperty(props, YARN_WORKER_NAME, YARN_WORKER_NAME_DEFAULT);
        LOGGER.info("Setting name for the workers: " + workerName);

        this.queue = this.getProperty(props, YARN_QUEUE, YARN_DEFULT_QUEUE);
        LOGGER.info("Setting yarn queue: " + queue);

        this.networkDocker = this.getProperty(props, YARN_DOCKER_NETWORK_NAME, YARN_DOCKER_NETWORK_BRIDGE);
        LOGGER.info("Using custom network for Docker: " + networkDocker);

        this.defaultUser = this.getProperty(props,VM_USER, DEFAULT_DEFAULT_USER);
        this.defaultPassword = this.getProperty(props,VM_PASS, "");
        this.keyPairName = this.getProperty(props,VM_KEYPAIR_NAME, DEFAULT_KEYPAIR_NAME);
        this.keyPairLocation = this.getProperty(props,VM_KEYPAIR_LOCATION, DEFAULT_KEYPAIR_LOCATION);
        this.publicKey = this.getPublicKey();

        try {
            this.masterHostname = InetAddress.getLocalHost().getHostName();
            LOGGER.info("Setting Yarn/COMPSs master node as "+ masterHostname);

        } catch (IOException ignored) {
        }


        client.init(conf);
        client.start();

        try {

            ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

            Map<String, LocalResource> localResources = initResource();
            amContainer.setLocalResources(localResources);

            Map<String, String> environment = new HashMap<>();
            initEnvironment(environment);
            amContainer.setEnvironment(environment);

            Vector<CharSequence> vargs = new Vector<CharSequence>(30);
            vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$()+"/bin/java");
            vargs.add("br.ufmg.dcc.clients.yarn.framework.YarnApplicationMaster");
            vargs.add(this.defaultUser);
            vargs.add(this.masterHostname);
            vargs.add(this.yarnMasterIp);
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

            Priority priortiy = Records.newRecord(Priority.class);
            priortiy.setPriority(1);

            YarnClientApplication app = client.createApplication();

            appContext = app.getApplicationSubmissionContext();
            appContext.setApplicationName(applicationName);
            appContext.setMaxAppAttempts(maxAttempt);
            appContext.setAMContainerSpec(amContainer);
            appContext.setResource(capability);
            appContext.setPriority(priortiy);
            appContext.setQueue(queue);

            // Submit application
            applicationId = appContext.getApplicationId();
            LOGGER.info("Initializing Yarn Client by applicationId "+applicationId);

            client.submitApplication(appContext);

            ApplicationReport appReport = client.getApplicationReport(applicationId);
            YarnApplicationState appState = appReport.getYarnApplicationState();
            while (appState != YarnApplicationState.RUNNING){
                Thread.sleep(1000);
                appReport = client.getApplicationReport(applicationId);
                appState = appReport.getYarnApplicationState();
            }
            this.serverPort = appReport.getRpcPort();
            this.applicationMasterHost = appReport.getHost();

            driver = new YarnDriver(applicationMasterHost, this.serverPort);


        } catch (YarnException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getPublicKey(){
        String publicKey = "";
        File file = new File(
                System.getProperty("user.home") +
                        File.separator +
                        ".ssh" +
                        File.separator +
                        keyPairName +
                        ".pub");

        try {
            FileInputStream fis = new FileInputStream(file);
            byte[] data = new byte[(int) file.length()];

            fis.read(data);
            fis.close();
            publicKey = new String(data, "UTF-8").replace("\n", "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return publicKey;
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
        final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer,
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

    public Map<String, LocalResource> initResource() {
        Map<String, LocalResource> localResources = new HashMap<>();

        try {

            LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

            FileSystem fs =  FileSystem.get(conf);
            String jarPath = "file://";
            try {
                 jarPath +=
                        new File(YarnApplicationClient.class.getProtectionDomain().getCodeSource().getLocation()
                                .toURI()).getPath();

            } catch (URISyntaxException e) {
                jarPath += "/opt/COMPSs/Runtime/cloud-conn/yarn-conn.jar";
            }

            Path dst = new Path(jarPath);
            FileStatus destStatus = fs.getFileStatus(dst);
            amJarRsrc.setType(LocalResourceType.FILE);
            amJarRsrc.setVisibility(LocalResourceVisibility.PUBLIC);
            amJarRsrc.setResource(URL.fromPath(dst));
            amJarRsrc.setTimestamp(destStatus.getModificationTime());
            amJarRsrc.setSize(destStatus.getLen());
            localResources.put("yarn-conn.jar", amJarRsrc);

        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
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
    public String requestWorker(String imageName, int cpus, int memory) {

        LOGGER.info("Requested a worker");
        String workerId = generateWorkerId(applicationName, workerName, this.getId());
        String containerId = driver.requestWorker(workerId, imageName, cpus, memory, this.publicKey, this.networkDocker);
        containerMaps.put(workerId, containerId);

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
    public String waitWorkerUntilRunning(String id)  {
        LOGGER.info("Waiting worker with id " + id);
        int runWorkerTimeout = 10000;

        return driver.waitTask(id, runWorkerTimeout, runWorkerTimeoutUnits);
    }

    /**
     * Stop worker running/staging in Yarn.
     *
     * @param id
     */
    public void removeWorker(String id) {
        LOGGER.debug("Remove worker with id " + id);
        int killWorkerTimeout = 10000;
        boolean success = driver.removeTask(id, killWorkerTimeout, killWorkerTimeoutUnits);
        LOGGER.debug("Remove worker with id " + id + ": " + success);
    }

    /**
     * Stop the Yarn Framework.
     */
    public void stop() {
        LOGGER.debug("Stopping Yarn Framework");

        try {
            driver.stopFramework();

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
//            client.close();
        }catch (InterruptedException | IOException | YarnException e) {
            //e.printStackTrace();
        }

    }

    private String getProperty(Map<String, String> props, String key, String defaultValue) {
        return props.containsKey(key) ? props.get(key) : defaultValue;
    }

}

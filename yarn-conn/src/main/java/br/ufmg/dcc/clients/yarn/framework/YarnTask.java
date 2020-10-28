package br.ufmg.dcc.clients.yarn.framework;

import br.ufmg.dcc.clients.yarn.framework.log.Loggers;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of a Yarn Task
 *
 */
public class YarnTask {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.YARN_APPLICATION_MASTER);

    // Properties
    private String id;
    private String ip;
    private ContainerLaunchContext ctx;
    private String nodeId;
    private String containerId;
    private String userVM;


    /**
     * Represents a Task to execute in Yarn.
     *
     * @param id Identifier.
     */
    public YarnTask(String id, ContainerLaunchContext ctx, String containerId, String nodeId, String userVM) {
        this.id = id;
        this.ctx = ctx;
        this.containerId = containerId;
        this.nodeId = nodeId;
        this.userVM = userVM;
    }

    public String getUserVM() {
        return userVM;
    }

    public void setUserVM(String userVM) {
        this.userVM = userVM;
    }

    /**
     * @return YarnTask identifier.
     */
    public String getId() {
        return id;
    }

    public String getNodeId() {return nodeId;}

    public void setNodeId(String id) {nodeId = id;}

    public String getContainerId() {return containerId;}

    public void setContainerId(String id) {containerId = id;}


    /**
     * @return Docker yarn container IP.
     */
    public String getIp() {
        return ip;
    }

    /**
     * @param ip New IP to assign.
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * @return YarnTask string.
     */
    @Override
    public String toString() {
        return String.format("[Task %s] is running in container %s by node %s", id, containerId, nodeId);
    }


}

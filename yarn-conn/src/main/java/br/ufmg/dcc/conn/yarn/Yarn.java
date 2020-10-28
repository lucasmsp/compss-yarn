package br.ufmg.dcc.conn.yarn;

import br.ufmg.dcc.clients.yarn.framework.YarnApplicationClient;
import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;
import br.ufmg.dcc.clients.yarn.framework.log.Loggers;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;
import es.bsc.conn.types.StarterCommand;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Implementation of Yarn connector
 *
 */
public class Yarn extends Connector {

    // Properties' names
    private static final String PROP_PRICE = "price";

    // Conversion constants
    private static final int GIGAS_TO_MEGAS = 1024;
    private static final String UNDEFINED_IP = "-1.-1.-1.-1";

    // Logger
    private static final Logger logger = LogManager.getLogger(Loggers.YARN);

    // Yarn Framework Client
    private final YarnApplicationClient framework;

    // Information about resources
    private final Map<String, VirtualResource> resources;


    /**
     * Initializes the Yarn connector with the given properties.
     * A Yarn Framework is started, it will do the communication with Yarn.
     *
     * @param props
     * @throws ConnException
     */
    public Yarn(Map<String, String> props) throws ConnException {
        super(props);
        logger.info("Initializing Yarn Connector");
        resources = new HashMap<>();
        try {
            this.framework = new YarnApplicationClient(props);
        } catch (FrameworkException e) {
            throw new ConnException(e);
        }

    }

    /**
     * Creates a yarn container.
     *
     * @param   hd Information about cpus, mem and price.
     * @param   sd Information about operating system.
     * @param   prop Properties inherited from Resources.
     * @return  Object Yarn container identifier generated.
     */
    @Override
    public Object create(String requestName, HardwareDescription hd, SoftwareDescription sd, Map<String,
            String> prop, StarterCommand starterCMD) {
        // memory must be a int because rpc xml do not support a long type.
        float memoryGb = (hd.getMemorySize() == -1.0F) ?  hd.getMemorySize() : 0.25F;
        int memoryMb = Math.round(memoryGb * GIGAS_TO_MEGAS);
        String newId = framework.requestWorker(hd.getImageName(), hd.getTotalCPUComputingUnits(), memoryMb);
        resources.put(newId, new VirtualResource(newId, hd, sd, prop));
        logger.debug("Yarn container created");
        return newId;
    }

    @Override
    public Object[] createMultiple(int replicas, String requestName, HardwareDescription hd, SoftwareDescription sd,
                                   Map<String, String> prop, StarterCommand starterCMD) {
        String[] envIds = new String[replicas];

        for(int i = 0; i < replicas; ++i) {
            envIds[i] = (String) this.create(requestName, hd, sd, prop, starterCMD);
        }

        return envIds;
    }

    /**
     * Waits Yarn container with identifier id to be ready.
     * @param  id Yarn container identifier.
     * @return VirtualResource assigned to that container.
     */
    @Override
    public VirtualResource waitUntilCreation(Object id) throws ConnException {
        logger.debug("Waiting until container creation");
        String identifier = (String) id;
        if (!resources.containsKey(identifier)) {
            throw new ConnException("This identifier does not exist " + identifier);
        }
        VirtualResource vr = resources.get(identifier);

        String ip = framework.waitWorkerUntilRunning(identifier);
        if (UNDEFINED_IP.equals(ip)) {
            throw new ConnException("Could not wait until creation of worker " + id);
        }

        vr.setIp(ip);
        return vr;
    }

    /**
     * @param  vr Corresponding machine to get the price from.
     * @return Price
     */
    @Override
    public float getPriceSlot(VirtualResource vr) {
        if (vr.getProperties().containsKey(PROP_PRICE)) {
            return Float.parseFloat(vr.getProperties().get(PROP_PRICE));
        }
        return 0.0f;
    }

    /**
     * Kills the Yarn container with identifier id.
     * @param id
     */
    @Override
    public void destroy(Object id) {
        String identifier = (String) id;
        resources.remove(identifier);
        try {
            framework.removeWorker(identifier);
        } catch (FrameworkException e) {
            e.printStackTrace();
        }
    }

    /**
     * Shutdowns the Yarn connector.
     */
    @Override
    public void close() {

        try {
            framework.stop();
        } catch (FrameworkException e) {
            e.printStackTrace();
        }

    }

}

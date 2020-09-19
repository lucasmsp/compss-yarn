package br.ufmg.dcc.clients.yarn.framework.rpc;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;


public interface YarnApplicationMasterInterface {

    String requestWorker(int timeout, String workerId, String imageName, int VCores, int Memory,
                         String publicKey, String dockerNetwork);

    String waitTask(int timeout, String id) throws FrameworkException;

    boolean removeTask(int timeout, String id) throws FrameworkException;

    boolean stopFramework(int timeout);

}

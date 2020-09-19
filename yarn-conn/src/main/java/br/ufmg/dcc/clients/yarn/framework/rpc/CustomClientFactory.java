package br.ufmg.dcc.clients.yarn.framework.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;

import br.ufmg.dcc.clients.yarn.framework.exceptions.FrameworkException;
import org.apache.xmlrpc.client.TimingOutCallback;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.common.TypeConverter;
import org.apache.xmlrpc.common.TypeConverterFactory;
import org.apache.xmlrpc.common.TypeConverterFactoryImpl;
import org.apache.xmlrpc.common.XmlRpcInvocationException;


/**
 * The CustomClientFactory is an adaptation of the official ClientFactory that uses an asynchronous RPC call.
 * This is also useful to include a notion of timeout.
 */
public class CustomClientFactory {
    private final XmlRpcClient client;
    private final TypeConverterFactory typeConverterFactory;
    private boolean objectMethodLocal;

    /** Creates a new instance.
     * @param pClient A fully configured XML-RPC client, which is used internally to perform XML-RPC calls.
     * @param pTypeConverterFactory Creates instances of TypeConverterFactory, which are used to transform the
     *                              result object in its target representation.
     */
    public CustomClientFactory(XmlRpcClient pClient, TypeConverterFactory pTypeConverterFactory) {
        typeConverterFactory = pTypeConverterFactory;
        client = pClient;
    }

    /** Creates a new instance. Shortcut for new ClientFactory(pClient, new TypeConverterFactoryImpl());
     * @param pClient A fully configured XML-RPC client, which is used internally to perform XML-RPC calls.
     */
    public CustomClientFactory(XmlRpcClient pClient) {
        this(pClient, new TypeConverterFactoryImpl());
    }

    /** Returns the factories client.
     */
    public XmlRpcClient getClient() {
        return client;
    }

    /** Returns, whether a method declared by the Object class is performed by the local object, rather than
     * by the server. Defaults to true.
     */
    public boolean isObjectMethodLocal() {
        return objectMethodLocal;
    }

    /** Sets, whether a method declared by the Object class is performed by the local object, rather than
     * by the server. Defaults to true.
     */
    public void setObjectMethodLocal(boolean pObjectMethodLocal) {
        objectMethodLocal = pObjectMethodLocal;
    }

    /**
     * Creates an object, which is implementing the given interface. The objects methods are internally calling an
     * XML-RPC server by using the factories client; shortcut for
     * newInstance(Thread.currentThread().getContextClassLoader(), pClass)
     */
    public Object newInstance(Class pClass) {
        return newInstance(Thread.currentThread().getContextClassLoader(), pClass);
    }

    /** Creates an object, which is implementing the given interface. The objects methods are internally calling an
     * XML-RPC server by using the factories client; shortcut for newInstance(pClassLoader, pClass, pClass.getName())
     */
    public Object newInstance(ClassLoader pClassLoader, Class pClass) {
        return newInstance(pClassLoader, pClass, pClass.getName());
    }

    /** Creates an object, which is implementing the given interface. The objects methods are internally calling an
     * XML-RPC server by using the factories client in an asynchronous call.
     * @param pClassLoader The class loader, which is being used for loading classes, if required.
     * @param pClass Interface, which is being implemented.
     * @param pRemoteName Handler name, which is being used when calling the server. This is used for composing the
     *   method name. For example, if <code>pRemoteName</code> is "Foo" and you want to invoke the method "bar" in
     *   the handler, then the full method name would be "Foo.bar".
     */
    public Object newInstance(ClassLoader pClassLoader, final Class pClass, final String pRemoteName) {
        return Proxy.newProxyInstance(pClassLoader, new Class[]{pClass}, new InvocationHandler(){
            public Object invoke(Object pProxy, Method pMethod, Object[] pArgs) throws Throwable {
                int Timeout = (int) pArgs[0];
                TimingOutCallback callback = new TimingOutCallback(Timeout * 1000);

                if (isObjectMethodLocal()  &&  pMethod.getDeclaringClass().equals(Object.class)) {
                    return pMethod.invoke(pProxy, pArgs);
                }
                final String methodName;
                if (pRemoteName == null  ||  pRemoteName.length() == 0) {
                    methodName = pMethod.getName();
                } else {
                    methodName = pRemoteName + "." + pMethod.getName();
                }
                Object result;
                try {
                    client.executeAsync(methodName, pArgs, callback);
                    result = callback.waitForResponse();

                }catch (TimingOutCallback.TimeoutException e) {
                    throw new FrameworkException("Timeout of "+Timeout+" seconds in "+ methodName + " operation.");

                } catch (XmlRpcInvocationException e) {
                    Throwable t = e.linkedException;
                    if (t instanceof RuntimeException) {
                        throw t;
                    }
                    Class[] exceptionTypes = pMethod.getExceptionTypes();
                    for (int i = 0;  i < exceptionTypes.length;  i++) {
                        Class c = exceptionTypes[i];
                        if (c.isAssignableFrom(t.getClass())) {
                            throw t;
                        }
                    }
                    throw new UndeclaredThrowableException(t);
                }
                TypeConverter typeConverter = typeConverterFactory.getTypeConverter(pMethod.getReturnType());
                return typeConverter.convert(result);
            }
        });
    }
}
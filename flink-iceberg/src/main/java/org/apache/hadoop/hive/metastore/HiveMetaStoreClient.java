////
//// Source code recreated from a .class file by IntelliJ IDEA
//// (powered by FernFlower decompiler)
////
//
//package org.apache.hadoop.hive.metastore;
//
//import com.google.common.annotations.VisibleForTesting;
//import com.google.common.collect.Lists;
//import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
//import org.apache.hadoop.classification.InterfaceAudience.Public;
//import org.apache.hadoop.classification.InterfaceStability.Evolving;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hive.common.ValidTxnList;
//import org.apache.hadoop.hive.common.ValidWriteIdList;
//import org.apache.hadoop.hive.metastore.api.Type;
//import org.apache.hadoop.hive.metastore.api.*;
//import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
//import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
//import org.apache.hadoop.hive.metastore.hooks.URIResolverHook;
//import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
//import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy.Factory;
//import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
//import org.apache.hadoop.hive.metastore.txn.TxnUtils;
//import org.apache.hadoop.hive.metastore.utils.JavaUtils;
//import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
//import org.apache.hadoop.hive.metastore.utils.ObjectPair;
//import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
//import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.hadoop.util.ReflectionUtils;
//import org.apache.hadoop.util.StringUtils;
//import org.apache.thrift.TApplicationException;
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TCompactProtocol;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.transport.TFramedTransport;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TTransport;
//import org.apache.thrift.transport.TTransportException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.security.auth.login.LoginException;
//import java.io.IOException;
//import java.lang.reflect.*;
//import java.net.InetAddress;
//import java.net.URI;
//import java.net.UnknownHostException;
//import java.nio.ByteBuffer;
//import java.security.PrivilegedExceptionAction;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//@Public
//@Evolving
//public class HiveMetaStoreClient implements IMetaStoreClient, AutoCloseable {
//    public static final ClientCapabilities VERSION;
//    public static final ClientCapabilities TEST_VERSION;
//    ThriftHiveMetastore.Iface client;
//    private TTransport transport;
//    private boolean isConnected;
//    private URI[] metastoreUris;
//    private final HiveMetaHookLoader hookLoader;
//    protected final Configuration conf;
//    private String tokenStrForm;
//    private final boolean localMetaStore;
//    private final MetaStoreFilterHook filterHook;
//    private final URIResolverHook uriResolverHook;
//    private final int fileMetadataBatchSize;
//    private Map<String, String> currentMetaVars;
//    private static final AtomicInteger connCount;
//    private int retries;
//    private long retryDelaySeconds;
//    private final ClientCapabilities version;
//    protected static final Logger LOG;
//    private static final String REPL_EVENTS_MISSING_IN_METASTORE = "Notification events are missing in the meta store.";
//
//    public HiveMetaStoreClient(Configuration conf) throws MetaException {
//        this(conf, (HiveMetaHookLoader) null, true);
//    }
//
//    public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader) throws MetaException {
//        this(conf, hookLoader, true);
//    }
//
//    public HiveMetaStoreClient(Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
//        this.client = null;
//        this.transport = null;
//        this.isConnected = false;
//        this.retries = 5;
//        this.retryDelaySeconds = 0L;
//        this.hookLoader = hookLoader;
//        if (conf == null) {
//            conf = MetastoreConf.newMetastoreConf();
//            this.conf = conf;
//        } else {
//            this.conf = new Configuration(conf);
//        }
//
//        this.version = MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) ? TEST_VERSION : VERSION;
//        this.filterHook = this.loadFilterHooks();
//        this.uriResolverHook = this.loadUriResolverHook();
//        this.fileMetadataBatchSize = MetastoreConf.getIntVar(conf, ConfVars.BATCH_RETRIEVE_OBJECTS_MAX);
//        String msUri = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
//        this.localMetaStore = MetastoreConf.isEmbeddedMetaStore(msUri);
//        if (this.localMetaStore) {
//            if (!allowEmbedded) {
//                throw new MetaException("Embedded metastore is not allowed here. Please configure " + ConfVars.THRIFT_URIS.toString() + "; it is currently set to [" + msUri + "]");
//            } else {
//                this.client = HiveMetaStore.newRetryingHMSHandler("hive client", this.conf, true);
//                this.isConnected = true;
//                this.snapshotActiveConf();
//            }
//        } else {
//            this.retries = MetastoreConf.getIntVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES);
//            this.retryDelaySeconds = MetastoreConf.getTimeVar(conf, ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
//            if (MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS) != null) {
//                this.resolveUris();
//                String HADOOP_PROXY_USER = "HADOOP_PROXY_USER";
//                String proxyUser = System.getenv(HADOOP_PROXY_USER);
//                if (proxyUser == null) {
//                    proxyUser = System.getProperty(HADOOP_PROXY_USER);
//                }
//
//                if (proxyUser != null) {
//                    LOG.info(HADOOP_PROXY_USER + " is set. Using delegation token for HiveMetaStore connection.");
//
//                    try {
//                        UserGroupInformation.getLoginUser().getRealUser().doAs(new PrivilegedExceptionAction<Void>() {
//                            public Void run() throws Exception {
//                                HiveMetaStoreClient.this.open();
//                                return null;
//                            }
//                        });
//                        String delegationTokenPropString = "DelegationTokenForHiveMetaStoreServer";
//                        String delegationTokenStr = this.getDelegationToken(proxyUser, proxyUser);
//                        SecurityUtils.setTokenStr(UserGroupInformation.getCurrentUser(), delegationTokenStr, delegationTokenPropString);
//                        MetastoreConf.setVar(this.conf, ConfVars.TOKEN_SIGNATURE, delegationTokenPropString);
//                        this.close();
//                    } catch (Exception var9) {
//                        Exception e = var9;
//                        LOG.error("Error while setting delegation token for " + proxyUser, e);
//                        if (e instanceof MetaException) {
//                            throw (MetaException) e;
//                        }
//
//                        throw new MetaException(e.getMessage());
//                    }
//                }
//
//                this.open();
//            } else {
//                LOG.error("NOT getting uris from conf");
//                throw new MetaException("MetaStoreURIs not found in conf file");
//            }
//        }
//    }
//
//    private void resolveUris() throws MetaException {
//        String[] metastoreUrisString = MetastoreConf.getVar(this.conf, ConfVars.THRIFT_URIS).split(",");
//        List<URI> metastoreURIArray = new ArrayList();
//
//        try {
//            int i = 0;
//            String[] var4 = metastoreUrisString;
//            int var5 = metastoreUrisString.length;
//
//            for (int var6 = 0; var6 < var5; ++var6) {
//                String s = var4[var6];
//                URI tmpUri = new URI(s);
//                if (tmpUri.getScheme() == null) {
//                    throw new IllegalArgumentException("URI: " + s + " does not have a scheme");
//                }
//
//                if (this.uriResolverHook != null) {
//                    metastoreURIArray.addAll(this.uriResolverHook.resolveURI(tmpUri));
//                } else {
//                    metastoreURIArray.add(new URI(tmpUri.getScheme(), tmpUri.getUserInfo(), HadoopThriftAuthBridge.getBridge().getCanonicalHostName(tmpUri.getHost()), tmpUri.getPort(), tmpUri.getPath(), tmpUri.getQuery(), tmpUri.getFragment()));
//                }
//            }
//
//            this.metastoreUris = new URI[metastoreURIArray.size()];
//
//            for (int j = 0; j < metastoreURIArray.size(); ++j) {
//                this.metastoreUris[j] = (URI) metastoreURIArray.get(j);
//            }
//
//            if (MetastoreConf.getVar(this.conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
//                List uriList = Arrays.asList(this.metastoreUris);
//                Collections.shuffle(uriList);
////                this.metastoreUris = (URI[]) ((URI[]) uriList.toArray());
//                this.metastoreUris =(URI[]) uriList.toArray(new URI[0]);
//            }
//        } catch (IllegalArgumentException var9) {
//            throw var9;
//        } catch (Exception var10) {
//            Exception e = var10;
//            MetaStoreUtils.logAndThrowMetaException(e);
//        }
//
//    }
//
//    private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
//        Class<? extends MetaStoreFilterHook> authProviderClass = MetastoreConf.getClass(this.conf, ConfVars.FILTER_HOOK, DefaultMetaStoreFilterHookImpl.class, MetaStoreFilterHook.class);
//        String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
//
//        try {
//            Constructor<? extends MetaStoreFilterHook> constructor = authProviderClass.getConstructor(Configuration.class);
//            return (MetaStoreFilterHook) constructor.newInstance(this.conf);
//        } catch (SecurityException | IllegalAccessException | InstantiationException | IllegalArgumentException |
//                 InvocationTargetException | NoSuchMethodException var4) {
//            Exception e = var4;
//            throw new IllegalStateException(msg + e.getMessage(), e);
//        }
//    }
//
//    private synchronized URIResolverHook loadUriResolverHook() throws IllegalStateException {
//        String uriResolverClassName = MetastoreConf.getAsString(this.conf, ConfVars.URI_RESOLVER);
//        if (uriResolverClassName.equals("")) {
//            return null;
//        } else {
//            LOG.info("Loading uri resolver" + uriResolverClassName);
//
//            try {
//                Class<?> uriResolverClass = Class.forName(uriResolverClassName, true, JavaUtils.getClassLoader());
//                return (URIResolverHook) ReflectionUtils.newInstance(uriResolverClass, (Configuration) null);
//            } catch (Exception var3) {
//                Exception e = var3;
//                LOG.error("Exception loading uri resolver hook" + e);
//                return null;
//            }
//        }
//    }
//
//    private void promoteRandomMetaStoreURI() {
//        if (this.metastoreUris.length > 1) {
//            Random rng = new Random();
//            int index = rng.nextInt(this.metastoreUris.length - 1) + 1;
//            URI tmp = this.metastoreUris[0];
//            this.metastoreUris[0] = this.metastoreUris[index];
//            this.metastoreUris[index] = tmp;
//        }
//    }
//
//    @VisibleForTesting
//    public TTransport getTTransport() {
//        return this.transport;
//    }
//
//    public boolean isLocalMetaStore() {
//        return this.localMetaStore;
//    }
//
//    public boolean isCompatibleWith(Configuration conf) {
//        Map<String, String> currentMetaVarsCopy = this.currentMetaVars;
//        if (currentMetaVarsCopy == null) {
//            return false;
//        } else {
//            boolean compatible = true;
//            MetastoreConf.ConfVars[] var4 = MetastoreConf.metaVars;
//            int var5 = var4.length;
//
//            for (int var6 = 0; var6 < var5; ++var6) {
//                MetastoreConf.ConfVars oneVar = var4[var6];
//                String oldVar = (String) currentMetaVarsCopy.get(oneVar.getVarname());
//                String newVar = MetastoreConf.getAsString(conf, oneVar);
//                if (oldVar != null) {
//                    if (oneVar.isCaseSensitive()) {
//                        if (oldVar.equals(newVar)) {
//                            continue;
//                        }
//                    } else if (oldVar.equalsIgnoreCase(newVar)) {
//                        continue;
//                    }
//                }
//
//                LOG.info("Mestastore configuration " + oneVar.toString() + " changed from " + oldVar + " to " + newVar);
//                compatible = false;
//            }
//
//            return compatible;
//        }
//    }
//
//    public void setHiveAddedJars(String addedJars) {
//        MetastoreConf.setVar(this.conf, ConfVars.ADDED_JARS, addedJars);
//    }
//
//    public void reconnect() throws MetaException {
//        if (this.localMetaStore) {
//            throw new MetaException("For direct MetaStore DB connections, we don't support retries at the client level.");
//        } else {
//            this.close();
//            if (this.uriResolverHook != null) {
//                this.resolveUris();
//            }
//
//            if (MetastoreConf.getVar(this.conf, ConfVars.THRIFT_URI_SELECTION).equalsIgnoreCase("RANDOM")) {
//                this.promoteRandomMetaStoreURI();
//            }
//
//            this.open();
//        }
//    }
//
//    public void alter_table(String dbname, String tbl_name, Table new_tbl) throws TException {
//        this.alter_table_with_environmentContext(dbname, tbl_name, new_tbl, (EnvironmentContext) null);
//    }
//
//    public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade) throws TException {
//        EnvironmentContext environmentContext = new EnvironmentContext();
//        if (cascade) {
//            environmentContext.putToProperties("CASCADE", "true");
//        }
//
//        this.alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
//    }
//
//    public void alter_table_with_environmentContext(String dbname, String tbl_name, Table new_tbl, EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
//        HiveMetaHook hook = this.getHook(new_tbl);
//        if (hook != null) {
//            hook.preAlterTable(new_tbl, envContext);
//        }
//
//        this.client.alter_table_with_environment_context(MetaStoreUtils.prependCatalogToDbName(dbname, this.conf), tbl_name, new_tbl, envContext);
//    }
//
//    public void alter_table(String catName, String dbName, String tblName, Table newTable, EnvironmentContext envContext) throws TException {
//        this.client.alter_table_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, newTable, envContext);
//    }
//
//    public void renamePartition(String dbname, String tableName, List<String> part_vals, Partition newPart) throws TException {
//        this.renamePartition(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, tableName, part_vals, newPart);
//    }
//
//    public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals, Partition newPart) throws TException {
//        this.client.rename_partition(MetaStoreUtils.prependCatalogToDbName(catName, dbname, this.conf), tableName, part_vals, newPart);
//    }
//
//    private void open() throws MetaException {
//        this.isConnected = false;
//        TTransportException tte = null;
//        boolean useSSL = MetastoreConf.getBoolVar(this.conf, ConfVars.USE_SSL);
//        boolean useSasl = MetastoreConf.getBoolVar(this.conf, ConfVars.USE_THRIFT_SASL);
//        boolean useFramedTransport = MetastoreConf.getBoolVar(this.conf, ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
//        boolean useCompactProtocol = MetastoreConf.getBoolVar(this.conf, ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
//        int clientSocketTimeout = (int) MetastoreConf.getTimeVar(this.conf, ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
//
//        for (int attempt = 0; !this.isConnected && attempt < this.retries; ++attempt) {
//            URI[] var8 = this.metastoreUris;
//            int var9 = var8.length;
//
//            for (int var10 = 0; var10 < var9; ++var10) {
//                URI store = var8[var10];
//                LOG.info("Trying to connect to metastore with URI " + store);
//
//                try {
//                    String tokenSig;
//                    if (useSSL) {
//                        try {
//                            String trustStorePath = MetastoreConf.getVar(this.conf, ConfVars.SSL_TRUSTSTORE_PATH).trim();
//                            if (trustStorePath.isEmpty()) {
//                                throw new IllegalArgumentException(ConfVars.SSL_TRUSTSTORE_PATH.toString() + " Not configured for SSL connection");
//                            }
//
//                            tokenSig = MetastoreConf.getPassword(this.conf, ConfVars.SSL_TRUSTSTORE_PASSWORD);
//                            this.transport = SecurityUtils.getSSLSocket(store.getHost(), store.getPort(), clientSocketTimeout, trustStorePath, tokenSig);
//                            LOG.info("Opened an SSL connection to metastore, current connections: " + connCount.incrementAndGet());
//                        } catch (IOException var20) {
//                            throw new IllegalArgumentException(var20);
//                        } catch (TTransportException var21) {
//                            throw new MetaException(var21.toString());
//                        }
//                    } else {
//                        this.transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout);
//                    }
//
//                    if (useSasl) {
//                        try {
//                            HadoopThriftAuthBridge.Client authBridge = HadoopThriftAuthBridge.getBridge().createClient();
//                            tokenSig = MetastoreConf.getVar(this.conf, ConfVars.TOKEN_SIGNATURE);
//                            this.tokenStrForm = SecurityUtils.getTokenStrForm(tokenSig);
//                            if (this.tokenStrForm != null) {
//                                LOG.info("HMSC::open(): Found delegation token. Creating DIGEST-based thrift connection.");
//                                this.transport = authBridge.createClientTransport((String) null, store.getHost(), "DIGEST", this.tokenStrForm, this.transport, MetaStoreUtils.getMetaStoreSaslProperties(this.conf, useSSL));
//                            } else {
//                                LOG.info("HMSC::open(): Could not find delegation token. Creating KERBEROS-based thrift connection.");
//                                String principalConfig = MetastoreConf.getVar(this.conf, ConfVars.KERBEROS_PRINCIPAL);
//                                this.transport = authBridge.createClientTransport(principalConfig, store.getHost(), "KERBEROS", (String) null, this.transport, MetaStoreUtils.getMetaStoreSaslProperties(this.conf, useSSL));
//                            }
//                        } catch (IOException var19) {
//                            LOG.error("Couldn't create client transport", var19);
//                            throw new MetaException(var19.toString());
//                        }
//                    } else if (useFramedTransport) {
//                        this.transport = new TFramedTransport(this.transport);
//                    }
//
//                    Object protocol;
//                    if (useCompactProtocol) {
//                        protocol = new TCompactProtocol(this.transport);
//                    } else {
//                        protocol = new TBinaryProtocol(this.transport);
//                    }
//
//                    this.client = new ThriftHiveMetastore.Client((TProtocol) protocol);
//
//                    try {
//                        if (!this.transport.isOpen()) {
//                            this.transport.open();
//                            LOG.info("Opened a connection to metastore, current connections: " + connCount.incrementAndGet());
//                        }
//
//                        this.isConnected = true;
//                    } catch (TTransportException var22) {
//                        tte = var22;
//                        if (LOG.isDebugEnabled()) {
//                            LOG.warn("Failed to connect to the MetaStore Server...", var22);
//                        } else {
//                            LOG.warn("Failed to connect to the MetaStore Server...");
//                        }
//                    }
//
//                    if (this.isConnected && !useSasl && MetastoreConf.getBoolVar(this.conf, ConfVars.EXECUTE_SET_UGI)) {
//                        try {
//                            UserGroupInformation ugi = SecurityUtils.getUGI();
//                            this.client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
//                        } catch (LoginException var16) {
//                            LOG.warn("Failed to do login. set_ugi() is not successful, Continuing without it.", var16);
//                        } catch (IOException var17) {
//                            LOG.warn("Failed to find ugi of client set_ugi() is not successful, Continuing without it.", var17);
//                        } catch (TException var18) {
//                            LOG.warn("set_ugi() not successful, Likely cause: new client talking to old server. Continuing without it.", var18);
//                        }
//                    }
//                } catch (MetaException var23) {
//                    MetaException e = var23;
//                    LOG.error("Unable to connect to metastore with URI " + store + " in attempt " + attempt, e);
//                }
//
//                if (this.isConnected) {
//                    break;
//                }
//            }
//
//            if (!this.isConnected && this.retryDelaySeconds > 0L) {
//                try {
//                    LOG.info("Waiting " + this.retryDelaySeconds + " seconds before next connection attempt.");
//                    Thread.sleep(this.retryDelaySeconds * 1000L);
//                } catch (InterruptedException var15) {
//                }
//            }
//        }
//
//        if (!this.isConnected) {
//            throw new MetaException("Could not connect to meta store using any of the URIs provided. Most recent failure: " + StringUtils.stringifyException(tte));
//        } else {
//            this.snapshotActiveConf();
//            LOG.info("Connected to metastore.");
//        }
//    }
//
//    private void snapshotActiveConf() {
//        this.currentMetaVars = new HashMap(MetastoreConf.metaVars.length);
//        MetastoreConf.ConfVars[] var1 = MetastoreConf.metaVars;
//        int var2 = var1.length;
//
//        for (int var3 = 0; var3 < var2; ++var3) {
//            MetastoreConf.ConfVars oneVar = var1[var3];
//            this.currentMetaVars.put(oneVar.getVarname(), MetastoreConf.getAsString(this.conf, oneVar));
//        }
//
//    }
//
//    public String getTokenStrForm() throws IOException {
//        return this.tokenStrForm;
//    }
//
//    public void close() {
//        this.isConnected = false;
//        this.currentMetaVars = null;
//
//        try {
//            if (null != this.client) {
//                this.client.shutdown();
//            }
//        } catch (TException var2) {
//            TException e = var2;
//            LOG.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
//        }
//
//        if (this.transport != null && this.transport.isOpen()) {
//            this.transport.close();
//            LOG.info("Closed a connection to metastore, current connections: " + connCount.decrementAndGet());
//        }
//
//    }
//
//    public void setMetaConf(String key, String value) throws TException {
//        this.client.setMetaConf(key, value);
//    }
//
//    public String getMetaConf(String key) throws TException {
//        return this.client.getMetaConf(key);
//    }
//
//    public void createCatalog(Catalog catalog) throws TException {
//        this.client.create_catalog(new CreateCatalogRequest(catalog));
//    }
//
//    public void alterCatalog(String catalogName, Catalog newCatalog) throws TException {
//        this.client.alter_catalog(new AlterCatalogRequest(catalogName, newCatalog));
//    }
//
//    public Catalog getCatalog(String catName) throws TException {
//        GetCatalogResponse rsp = this.client.get_catalog(new GetCatalogRequest(catName));
//        return rsp == null ? null : this.filterHook.filterCatalog(rsp.getCatalog());
//    }
//
//    public List<String> getCatalogs() throws TException {
//        GetCatalogsResponse rsp = this.client.get_catalogs();
//        return rsp == null ? null : this.filterHook.filterCatalogs(rsp.getNames());
//    }
//
//    public void dropCatalog(String catName) throws TException {
//        this.client.drop_catalog(new DropCatalogRequest(catName));
//    }
//
//    public Partition add_partition(Partition new_part) throws TException {
//        return this.add_partition(new_part, (EnvironmentContext) null);
//    }
//
//    public Partition add_partition(Partition new_part, EnvironmentContext envContext) throws TException {
//        if (!new_part.isSetCatName()) {
//            new_part.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        Partition p = this.client.add_partition_with_environment_context(new_part, envContext);
//        return this.deepCopy(p);
//    }
//
//    public int add_partitions(List<Partition> new_parts) throws TException {
//        if (new_parts != null && !new_parts.isEmpty() && !((Partition) new_parts.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            new_parts.forEach((p) -> {
//                p.setCatName(defaultCat);
//            });
//        }
//
//        return this.client.add_partitions(new_parts);
//    }
//
//    public List<Partition> add_partitions(List<Partition> parts, boolean ifNotExists, boolean needResults) throws TException {
//        if (parts.isEmpty()) {
//            return needResults ? new ArrayList() : null;
//        } else {
//            Partition part = (Partition) parts.get(0);
//            if (!part.isSetCatName()) {
//                String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//                parts.forEach((p) -> {
//                    p.setCatName(defaultCat);
//                });
//            }
//
//            AddPartitionsRequest req = new AddPartitionsRequest(part.getDbName(), part.getTableName(), parts, ifNotExists);
//            req.setCatName(part.isSetCatName() ? part.getCatName() : MetaStoreUtils.getDefaultCatalog(this.conf));
//            req.setNeedResult(needResults);
//            AddPartitionsResult result = this.client.add_partitions_req(req);
//            return needResults ? this.filterHook.filterPartitions(result.getPartitions()) : null;
//        }
//    }
//
//    public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
//        if (partitionSpec.getCatName() == null) {
//            partitionSpec.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.add_partitions_pspec(partitionSpec.toPartitionSpec());
//    }
//
//    public Partition appendPartition(String db_name, String table_name, List<String> part_vals) throws TException {
//        return this.appendPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, table_name, part_vals);
//    }
//
//    public Partition appendPartition(String dbName, String tableName, String partName) throws TException {
//        return this.appendPartition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partName);
//    }
//
//    public Partition appendPartition(String catName, String dbName, String tableName, String name) throws TException {
//        Partition p = this.client.append_partition_by_name(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, name);
//        return this.deepCopy(p);
//    }
//
//    public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals) throws TException {
//        Partition p = this.client.append_partition(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, partVals);
//        return this.deepCopy(p);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public Partition appendPartition(String dbName, String tableName, List<String> partVals, EnvironmentContext ec) throws TException {
//        return this.client.append_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(dbName, this.conf), tableName, partVals, ec).deepCopy();
//    }
//
//    public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destDb, String destinationTableName) throws TException {
//        return this.exchange_partition(partitionSpecs, MetaStoreUtils.getDefaultCatalog(this.conf), sourceDb, sourceTable, MetaStoreUtils.getDefaultCatalog(this.conf), destDb, destinationTableName);
//    }
//
//    public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat, String sourceDb, String sourceTable, String destCat, String destDb, String destTableName) throws TException {
//        return this.client.exchange_partition(partitionSpecs, MetaStoreUtils.prependCatalogToDbName(sourceCat, sourceDb, this.conf), sourceTable, MetaStoreUtils.prependCatalogToDbName(destCat, destDb, this.conf), destTableName);
//    }
//
//    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable, String destDb, String destinationTableName) throws TException {
//        return this.exchange_partitions(partitionSpecs, MetaStoreUtils.getDefaultCatalog(this.conf), sourceDb, sourceTable, MetaStoreUtils.getDefaultCatalog(this.conf), destDb, destinationTableName);
//    }
//
//    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat, String sourceDb, String sourceTable, String destCat, String destDb, String destTableName) throws TException {
//        return this.client.exchange_partitions(partitionSpecs, MetaStoreUtils.prependCatalogToDbName(sourceCat, sourceDb, this.conf), sourceTable, MetaStoreUtils.prependCatalogToDbName(destCat, destDb, this.conf), destTableName);
//    }
//
//    public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {
//        this.client.partition_name_has_valid_characters(partVals, true);
//    }
//
//    public void createDatabase(Database db) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
//        if (!db.isSetCatalogName()) {
//            db.setCatalogName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        this.client.create_database(db);
//    }
//
//    public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
//        this.createTable(tbl, (EnvironmentContext) null);
//    }
//
//    public void createTable(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
//        if (!tbl.isSetCatName()) {
//            tbl.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        HiveMetaHook hook = this.getHook(tbl);
//        if (hook != null) {
//            hook.preCreateTable(tbl);
//        }
//
//        boolean success = false;
//
//        try {
//            this.create_table_with_environment_context(tbl, envContext);
//            if (hook != null) {
//                hook.commitCreateTable(tbl);
//            }
//
//            success = true;
//        } finally {
//            if (!success && hook != null) {
//                try {
//                    hook.rollbackCreateTable(tbl);
//                } catch (Exception var11) {
//                    Exception e = var11;
//                    LOG.error("Create rollback failed with", e);
//                }
//            }
//
//        }
//
//    }
//
//    public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
//        if (!tbl.isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            tbl.setCatName(defaultCat);
//            if (primaryKeys != null) {
//                primaryKeys.forEach((pk) -> {
//                    pk.setCatName(defaultCat);
//                });
//            }
//
//            if (foreignKeys != null) {
//                foreignKeys.forEach((fk) -> {
//                    fk.setCatName(defaultCat);
//                });
//            }
//
//            if (uniqueConstraints != null) {
//                uniqueConstraints.forEach((uc) -> {
//                    uc.setCatName(defaultCat);
//                });
//            }
//
//            if (notNullConstraints != null) {
//                notNullConstraints.forEach((nn) -> {
//                    nn.setCatName(defaultCat);
//                });
//            }
//
//            if (defaultConstraints != null) {
//                defaultConstraints.forEach((def) -> {
//                    def.setCatName(defaultCat);
//                });
//            }
//
//            if (checkConstraints != null) {
//                checkConstraints.forEach((cc) -> {
//                    cc.setCatName(defaultCat);
//                });
//            }
//        }
//
//        HiveMetaHook hook = this.getHook(tbl);
//        if (hook != null) {
//            hook.preCreateTable(tbl);
//        }
//
//        boolean success = false;
//
//        try {
//            this.client.create_table_with_constraints(tbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
//            if (hook != null) {
//                hook.commitCreateTable(tbl);
//            }
//
//            success = true;
//        } finally {
//            if (!success && hook != null) {
//                hook.rollbackCreateTable(tbl);
//            }
//
//        }
//
//    }
//
//    public void dropConstraint(String dbName, String tableName, String constraintName) throws TException {
//        this.dropConstraint(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, constraintName);
//    }
//
//    public void dropConstraint(String catName, String dbName, String tableName, String constraintName) throws TException {
//        DropConstraintRequest rqst = new DropConstraintRequest(dbName, tableName, constraintName);
//        rqst.setCatName(catName);
//        this.client.drop_constraint(rqst);
//    }
//
//    public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
//        if (!primaryKeyCols.isEmpty() && !((SQLPrimaryKey) primaryKeyCols.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            primaryKeyCols.forEach((pk) -> {
//                pk.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_primary_key(new AddPrimaryKeyRequest(primaryKeyCols));
//    }
//
//    public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
//        if (!foreignKeyCols.isEmpty() && !((SQLForeignKey) foreignKeyCols.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            foreignKeyCols.forEach((fk) -> {
//                fk.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_foreign_key(new AddForeignKeyRequest(foreignKeyCols));
//    }
//
//    public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws NoSuchObjectException, MetaException, TException {
//        if (!uniqueConstraintCols.isEmpty() && !((SQLUniqueConstraint) uniqueConstraintCols.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            uniqueConstraintCols.forEach((uc) -> {
//                uc.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_unique_constraint(new AddUniqueConstraintRequest(uniqueConstraintCols));
//    }
//
//    public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws NoSuchObjectException, MetaException, TException {
//        if (!notNullConstraintCols.isEmpty() && !((SQLNotNullConstraint) notNullConstraintCols.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            notNullConstraintCols.forEach((nn) -> {
//                nn.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_not_null_constraint(new AddNotNullConstraintRequest(notNullConstraintCols));
//    }
//
//    public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws NoSuchObjectException, MetaException, TException {
//        if (!defaultConstraints.isEmpty() && !((SQLDefaultConstraint) defaultConstraints.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            defaultConstraints.forEach((def) -> {
//                def.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_default_constraint(new AddDefaultConstraintRequest(defaultConstraints));
//    }
//
//    public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws NoSuchObjectException, MetaException, TException {
//        if (!checkConstraints.isEmpty() && !((SQLCheckConstraint) checkConstraints.get(0)).isSetCatName()) {
//            String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//            checkConstraints.forEach((cc) -> {
//                cc.setCatName(defaultCat);
//            });
//        }
//
//        this.client.add_check_constraint(new AddCheckConstraintRequest(checkConstraints));
//    }
//
//    public boolean createType(Type type) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
//        return this.client.create_type(type);
//    }
//
//    public void dropDatabase(String name) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
//        this.dropDatabase(MetaStoreUtils.getDefaultCatalog(this.conf), name, true, false, false);
//    }
//
//    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
//        this.dropDatabase(MetaStoreUtils.getDefaultCatalog(this.conf), name, deleteData, ignoreUnknownDb, false);
//    }
//
//    public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
//        this.dropDatabase(MetaStoreUtils.getDefaultCatalog(this.conf), name, deleteData, ignoreUnknownDb, cascade);
//    }
//
//    public void dropDatabase(String catalogName, String dbName, boolean deleteData, boolean ignoreUnknownDb, boolean cascade) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
//        try {
//            this.getDatabase(catalogName, dbName);
//        } catch (NoSuchObjectException var12) {
//            NoSuchObjectException e = var12;
//            if (!ignoreUnknownDb) {
//                throw e;
//            }
//
//            return;
//        }
//
//        if (cascade) {
//            List<String> materializedViews = this.getTables(dbName, ".*", TableType.MATERIALIZED_VIEW);
//            Iterator var7 = materializedViews.iterator();
//
//            while (var7.hasNext()) {
//                String table = (String) var7.next();
//                this.dropTable(dbName, table, deleteData, true);
//            }
//
//            List<String> tableList = this.getAllTables(dbName);
//            Iterator var15 = tableList.iterator();
//
//            while (var15.hasNext()) {
//                String table = (String) var15.next();
//
//                try {
//                    this.dropTable(dbName, table, deleteData, true);
//                } catch (UnsupportedOperationException var11) {
//                }
//            }
//        }
//
//        this.client.drop_database(MetaStoreUtils.prependCatalogToDbName(catalogName, dbName, this.conf), deleteData, cascade);
//    }
//
//    public boolean dropPartition(String dbName, String tableName, String partName, boolean deleteData) throws TException {
//        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partName, deleteData);
//    }
//
//    public boolean dropPartition(String catName, String db_name, String tbl_name, String name, boolean deleteData) throws TException {
//        return this.client.drop_partition_by_name_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, name, deleteData, (EnvironmentContext) null);
//    }
//
//    private static EnvironmentContext getEnvironmentContextWithIfPurgeSet() {
//        Map<String, String> warehouseOptions = new HashMap();
//        warehouseOptions.put("ifPurge", "TRUE");
//        return new EnvironmentContext(warehouseOptions);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, EnvironmentContext env_context) throws TException {
//        return this.client.drop_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(db_name, this.conf), tbl_name, part_vals, true, env_context);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public boolean dropPartition(String dbName, String tableName, String partName, boolean dropData, EnvironmentContext ec) throws TException {
//        return this.client.drop_partition_by_name_with_environment_context(MetaStoreUtils.prependCatalogToDbName(dbName, this.conf), tableName, partName, dropData, ec);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public boolean dropPartition(String dbName, String tableName, List<String> partVals) throws TException {
//        return this.client.drop_partition(MetaStoreUtils.prependCatalogToDbName(dbName, this.conf), tableName, partVals, true);
//    }
//
//    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws TException {
//        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, PartitionDropOptions.instance().deleteData(deleteData));
//    }
//
//    public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals, boolean deleteData) throws TException {
//        return this.dropPartition(catName, db_name, tbl_name, part_vals, PartitionDropOptions.instance().deleteData(deleteData));
//    }
//
//    public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
//        return this.dropPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, options);
//    }
//
//    public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options) throws TException {
//        if (options == null) {
//            options = PartitionDropOptions.instance();
//        }
//
//        if (part_vals != null) {
//            Iterator var6 = part_vals.iterator();
//
//            while (var6.hasNext()) {
//                String partVal = (String) var6.next();
//                if (partVal == null) {
//                    throw new MetaException("The partition value must not be null.");
//                }
//            }
//        }
//
//        return this.client.drop_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, part_vals, options.deleteData, options.purgeData ? getEnvironmentContextWithIfPurgeSet() : null);
//    }
//
//    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
//        return this.dropPartitions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, partExprs, options);
//    }
//
//    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists, boolean needResult) throws NoSuchObjectException, MetaException, TException {
//        return this.dropPartitions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, partExprs, PartitionDropOptions.instance().deleteData(deleteData).ifExists(ifExists).returnResults(needResult));
//    }
//
//    public List<Partition> dropPartitions(String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists) throws NoSuchObjectException, MetaException, TException {
//        return this.dropPartitions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, partExprs, PartitionDropOptions.instance().deleteData(deleteData).ifExists(ifExists));
//    }
//
//    public List<Partition> dropPartitions(String catName, String dbName, String tblName, List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
//        RequestPartsSpec rps = new RequestPartsSpec();
//        List<DropPartitionsExpr> exprs = new ArrayList(partExprs.size());
//        Iterator var8 = partExprs.iterator();
//
//        while (var8.hasNext()) {
//            ObjectPair<Integer, byte[]> partExpr = (ObjectPair) var8.next();
//            DropPartitionsExpr dpe = new DropPartitionsExpr();
//            dpe.setExpr((byte[]) partExpr.getSecond());
//            dpe.setPartArchiveLevel((Integer) partExpr.getFirst());
//            exprs.add(dpe);
//        }
//
//        rps.setExprs(exprs);
//        DropPartitionsRequest req = new DropPartitionsRequest(dbName, tblName, rps);
//        req.setCatName(catName);
//        req.setDeleteData(options.deleteData);
//        req.setNeedResult(options.returnResults);
//        req.setIfExists(options.ifExists);
//        if (options.purgeData) {
//            LOG.info("Dropped partitions will be purged!");
//            req.setEnvironmentContext(getEnvironmentContextWithIfPurgeSet());
//        }
//
//        return this.client.drop_partitions_req(req).getPartitions();
//    }
//
//    public void dropTable(String dbname, String name, boolean deleteData, boolean ignoreUnknownTab) throws MetaException, TException, NoSuchObjectException, UnsupportedOperationException {
//        this.dropTable(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, name, deleteData, ignoreUnknownTab, (EnvironmentContext) null);
//    }
//
//    public void dropTable(String dbname, String name, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
//        this.dropTable(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, name, deleteData, ignoreUnknownTab, ifPurge);
//    }
//
//    public void dropTable(String dbname, String name) throws TException {
//        this.dropTable(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, name, true, true, (EnvironmentContext) null);
//    }
//
//    public void dropTable(String catName, String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTable, boolean ifPurge) throws TException {
//        EnvironmentContext envContext = null;
//        if (ifPurge) {
//            Map<String, String> warehouseOptions = new HashMap();
//            warehouseOptions.put("ifPurge", "TRUE");
//            envContext = new EnvironmentContext(warehouseOptions);
//        }
//
//        this.dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, envContext);
//    }
//
//    public void dropTable(String catName, String dbname, String name, boolean deleteData, boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException, NoSuchObjectException, UnsupportedOperationException {
//        Table tbl;
//        try {
//            tbl = this.getTable(catName, dbname, name);
//        } catch (NoSuchObjectException var15) {
//            NoSuchObjectException e = var15;
//            if (!ignoreUnknownTab) {
//                throw e;
//            }
//
//            return;
//        }
//
//        HiveMetaHook hook = this.getHook(tbl);
//        if (hook != null) {
//            hook.preDropTable(tbl);
//        }
//
//        boolean success = false;
//
//        try {
//            this.drop_table_with_environment_context(catName, dbname, name, deleteData, envContext);
//            if (hook != null) {
//                hook.commitDropTable(tbl, deleteData || envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge")));
//            }
//
//            success = true;
//        } catch (NoSuchObjectException var16) {
//            NoSuchObjectException e = var16;
//            if (!ignoreUnknownTab) {
//                throw e;
//            }
//        } finally {
//            if (!success && hook != null) {
//                hook.rollbackDropTable(tbl);
//            }
//
//        }
//
//    }
//
//    public void truncateTable(String dbName, String tableName, List<String> partNames) throws TException {
//        this.truncateTable(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partNames);
//    }
//
//    public void truncateTable(String catName, String dbName, String tableName, List<String> partNames) throws TException {
//        this.client.truncate_table(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, partNames);
//    }
//
//    public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
//        return this.client.cm_recycle(request);
//    }
//
//    public boolean dropType(String type) throws NoSuchObjectException, MetaException, TException {
//        return this.client.drop_type(type);
//    }
//
//    public Map<String, Type> getTypeAll(String name) throws MetaException, TException {
//        Map<String, Type> result = null;
//        Map<String, Type> fromClient = this.client.get_type_all(name);
//        if (fromClient != null) {
//            result = new LinkedHashMap();
//            Iterator var4 = fromClient.keySet().iterator();
//
//            while (var4.hasNext()) {
//                String key = (String) var4.next();
//                result.put(key, this.deepCopy((Type) fromClient.get(key)));
//            }
//        }
//
//        return result;
//    }
//
//    public List<String> getDatabases(String databasePattern) throws TException {
//        return this.getDatabases(MetaStoreUtils.getDefaultCatalog(this.conf), databasePattern);
//    }
//
//    public List<String> getDatabases(String catName, String databasePattern) throws TException {
//        return this.filterHook.filterDatabases(this.client.get_databases(MetaStoreUtils.prependCatalogToDbName(catName, databasePattern, this.conf)));
//    }
//
//    public List<String> getAllDatabases() throws TException {
//        return this.getAllDatabases(MetaStoreUtils.getDefaultCatalog(this.conf));
//    }
//
//    public List<String> getAllDatabases(String catName) throws TException {
//        return this.filterHook.filterDatabases(this.client.get_databases(MetaStoreUtils.prependCatalogToDbName(catName, (String) null, this.conf)));
//    }
//
//    public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts) throws TException {
//        return this.listPartitions(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, (String) tbl_name, (int) max_parts);
//    }
//
//    public List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts) throws TException {
//        List<Partition> parts = this.client.get_partitions(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, this.shrinkMaxtoShort(max_parts));
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
//        return this.listPartitionSpecs(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, maxParts);
//    }
//
//    public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts) throws TException {
//        return Factory.get(this.filterHook.filterPartitionSpecs(this.client.get_partitions_pspec(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, maxParts)));
//    }
//
//    public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws TException {
//        return this.listPartitions(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, max_parts);
//    }
//
//    public List<Partition> listPartitions(String catName, String db_name, String tbl_name, List<String> part_vals, int max_parts) throws TException {
//        List<Partition> parts = this.client.get_partitions_ps(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, part_vals, this.shrinkMaxtoShort(max_parts));
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name, short max_parts, String user_name, List<String> group_names) throws TException {
//        return this.listPartitionsWithAuthInfo(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, (String) tbl_name, (int) max_parts, user_name, group_names);
//    }
//
//    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName, int maxParts, String userName, List<String> groupNames) throws TException {
//        List<Partition> parts = this.client.get_partitions_with_auth(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, this.shrinkMaxtoShort(maxParts), userName, groupNames);
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name, List<String> part_vals, short max_parts, String user_name, List<String> group_names) throws TException {
//        return this.listPartitionsWithAuthInfo(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, max_parts, user_name, group_names);
//    }
//
//    public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName, List<String> partialPvals, int maxParts, String userName, List<String> groupNames) throws TException {
//        List<Partition> parts = this.client.get_partitions_ps_with_auth(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, partialPvals, this.shrinkMaxtoShort(maxParts), userName, groupNames);
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts) throws TException {
//        return this.listPartitionsByFilter(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, filter, max_parts);
//    }
//
//    public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name, String filter, int max_parts) throws TException {
//        List<Partition> parts = this.client.get_partitions_by_filter(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, filter, this.shrinkMaxtoShort(max_parts));
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts) throws TException {
//        return this.listPartitionSpecsByFilter(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, filter, max_parts);
//    }
//
//    public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name, String filter, int max_parts) throws TException {
//        return Factory.get(this.filterHook.filterPartitionSpecs(this.client.get_part_specs_by_filter(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, filter, max_parts)));
//    }
//
//    public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name, short max_parts, List<Partition> result) throws TException {
//        return this.listPartitionsByExpr(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, expr, default_partition_name, max_parts, result);
//    }
//
//    public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr, String default_partition_name, int max_parts, List<Partition> result) throws TException {
//        assert result != null;
//
//        PartitionsByExprRequest req = new PartitionsByExprRequest(db_name, tbl_name, ByteBuffer.wrap(expr));
//        if (default_partition_name != null) {
//            req.setDefaultPartitionName(default_partition_name);
//        }
//
//        if (max_parts >= 0) {
//            req.setMaxParts(this.shrinkMaxtoShort(max_parts));
//        }
//
//        PartitionsByExprResult r;
//        try {
//            r = this.client.get_partitions_by_expr(req);
//        } catch (TApplicationException var11) {
//            TApplicationException te = var11;
//            if (te.getType() != 1 && te.getType() != 3) {
//                throw te;
//            }
//
//            throw new IMetaStoreClient.IncompatibleMetastoreException("Metastore doesn't support listPartitionsByExpr: " + te.getMessage());
//        }
//
//        r.setPartitions(this.filterHook.filterPartitions(r.getPartitions()));
//        this.deepCopyPartitions(r.getPartitions(), result);
//        return !r.isSetHasUnknownPartitions() || r.isHasUnknownPartitions();
//    }
//
//    public Database getDatabase(String name) throws TException {
//        return this.getDatabase(MetaStoreUtils.getDefaultCatalog(this.conf), name);
//    }
//
//    public Database getDatabase(String catalogName, String databaseName) throws TException {
//        Database d = this.client.get_database(MetaStoreUtils.prependCatalogToDbName(catalogName, databaseName, this.conf));
//        return this.deepCopy(this.filterHook.filterDatabase(d));
//    }
//
//    public Partition getPartition(String db_name, String tbl_name, List<String> part_vals) throws TException {
//        return this.getPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals);
//    }
//
//    public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals) throws TException {
//        Partition p = this.client.get_partition(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, partVals);
//        return this.deepCopy(this.filterHook.filterPartition(p));
//    }
//
//    public List<Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names) throws TException {
//        return this.getPartitionsByNames(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_names);
//    }
//
//    public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name, List<String> part_names) throws TException {
//        List<Partition> parts = this.client.get_partitions_by_names(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, part_names);
//        return this.deepCopyPartitions(this.filterHook.filterPartitions(parts));
//    }
//
//    public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws MetaException, TException, NoSuchObjectException {
//        if (!request.isSetCatName()) {
//            request.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_partition_values(request);
//    }
//
//    public Partition getPartitionWithAuthInfo(String db_name, String tbl_name, List<String> part_vals, String user_name, List<String> group_names) throws TException {
//        return this.getPartitionWithAuthInfo(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, user_name, group_names);
//    }
//
//    public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName, List<String> pvals, String userName, List<String> groupNames) throws TException {
//        Partition p = this.client.get_partition_with_auth(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, pvals, userName, groupNames);
//        return this.deepCopy(this.filterHook.filterPartition(p));
//    }
//
//    public Table getTable(String dbname, String name) throws TException {
//        return this.getTable(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, name);
//    }
//
//    public Table getTable(String catName, String dbName, String tableName) throws TException {
//        GetTableRequest req = new GetTableRequest(dbName, tableName);
//        req.setCatName(catName);
//        req.setCapabilities(this.version);
//        Table t = this.client.get_table_req(req).getTable();
//        return this.deepCopy(this.filterHook.filterTable(t));
//    }
//
//    public List<Table> getTableObjectsByName(String dbName, List<String> tableNames) throws TException {
//        return this.getTableObjectsByName(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableNames);
//    }
//
//    public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames) throws TException {
//        GetTablesRequest req = new GetTablesRequest(dbName);
//        req.setCatName(catName);
//        req.setTblNames(tableNames);
//        req.setCapabilities(this.version);
//        List<Table> tabs = this.client.get_table_objects_by_name_req(req).getTables();
//        return this.deepCopyTables(this.filterHook.filterTables(tabs));
//    }
//
//    public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList) throws MetaException, InvalidOperationException, UnknownDBException, TException {
//        return this.client.get_materialization_invalidation_info(cm, validTxnList);
//    }
//
//    public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm) throws MetaException, InvalidOperationException, UnknownDBException, TException {
//        this.client.update_creation_metadata(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, cm);
//    }
//
//    public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm) throws MetaException, TException {
//        this.client.update_creation_metadata(catName, dbName, tableName, cm);
//    }
//
//    public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws TException {
//        return this.listTableNamesByFilter(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, filter, maxTables);
//    }
//
//    public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables) throws TException {
//        return this.filterHook.filterTableNames(catName, dbName, this.client.get_table_names_by_filter(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), filter, this.shrinkMaxtoShort(maxTables)));
//    }
//
//    public Type getType(String name) throws NoSuchObjectException, MetaException, TException {
//        return this.deepCopy(this.client.get_type(name));
//    }
//
//    public List<String> getTables(String dbname, String tablePattern) throws MetaException {
//        try {
//            return this.getTables(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, tablePattern);
//        } catch (Exception var4) {
//            Exception e = var4;
//            MetaStoreUtils.logAndThrowMetaException(e);
//            return null;
//        }
//    }
//
//    public List<String> getTables(String catName, String dbName, String tablePattern) throws TException {
//        return this.filterHook.filterTableNames(catName, dbName, this.client.get_tables(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tablePattern));
//    }
//
//    public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
//        try {
//            return this.getTables(MetaStoreUtils.getDefaultCatalog(this.conf), dbname, tablePattern, tableType);
//        } catch (Exception var5) {
//            Exception e = var5;
//            MetaStoreUtils.logAndThrowMetaException(e);
//            return null;
//        }
//    }
//
//    public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType) throws TException {
//        return this.filterHook.filterTableNames(catName, dbName, this.client.get_tables_by_type(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tablePattern, tableType.toString()));
//    }
//
//    public List<String> getMaterializedViewsForRewriting(String dbName) throws TException {
//        return this.getMaterializedViewsForRewriting(MetaStoreUtils.getDefaultCatalog(this.conf), dbName);
//    }
//
//    public List<String> getMaterializedViewsForRewriting(String catName, String dbname) throws MetaException {
//        try {
//            return this.filterHook.filterTableNames(catName, dbname, this.client.get_materialized_views_for_rewriting(MetaStoreUtils.prependCatalogToDbName(catName, dbname, this.conf)));
//        } catch (Exception var4) {
//            Exception e = var4;
//            MetaStoreUtils.logAndThrowMetaException(e);
//            return null;
//        }
//    }
//
//    public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes) throws MetaException {
//        try {
//            return this.getTableMeta(MetaStoreUtils.getDefaultCatalog(this.conf), dbPatterns, tablePatterns, tableTypes);
//        } catch (Exception var5) {
//            Exception e = var5;
//            MetaStoreUtils.logAndThrowMetaException(e);
//            return null;
//        }
//    }
//
//    public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns, List<String> tableTypes) throws TException {
//        return this.filterHook.filterTableMetas(this.client.get_table_meta(MetaStoreUtils.prependCatalogToDbName(catName, dbPatterns, this.conf), tablePatterns, tableTypes));
//    }
//
//    public List<String> getAllTables(String dbname) throws MetaException {
//        try {
//            return this.getAllTables(MetaStoreUtils.getDefaultCatalog(this.conf), dbname);
//        } catch (Exception var3) {
//            Exception e = var3;
//            MetaStoreUtils.logAndThrowMetaException(e);
//            return null;
//        }
//    }
//
//    public List<String> getAllTables(String catName, String dbName) throws TException {
//        return this.filterHook.filterTableNames(catName, dbName, this.client.get_all_tables(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf)));
//    }
//
//    public boolean tableExists(String databaseName, String tableName) throws TException {
//        return this.tableExists(MetaStoreUtils.getDefaultCatalog(this.conf), databaseName, tableName);
//    }
//
//    public boolean tableExists(String catName, String dbName, String tableName) throws TException {
//        try {
//            GetTableRequest req = new GetTableRequest(dbName, tableName);
//            req.setCatName(catName);
//            req.setCapabilities(this.version);
//            return this.filterHook.filterTable(this.client.get_table_req(req).getTable()) != null;
//        } catch (NoSuchObjectException var5) {
//            return false;
//        }
//    }
//
//    public List<String> listPartitionNames(String dbName, String tblName, short max) throws NoSuchObjectException, MetaException, TException {
//        return this.listPartitionNames(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, (String) tblName, (int) max);
//    }
//
//    public List<String> listPartitionNames(String catName, String dbName, String tableName, int maxParts) throws TException {
//        return this.filterHook.filterPartitionNames(catName, dbName, tableName, this.client.get_partition_names(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, this.shrinkMaxtoShort(maxParts)));
//    }
//
//    public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts) throws TException {
//        return this.listPartitionNames(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, part_vals, max_parts);
//    }
//
//    public List<String> listPartitionNames(String catName, String db_name, String tbl_name, List<String> part_vals, int max_parts) throws TException {
//        return this.filterHook.filterPartitionNames(catName, db_name, tbl_name, this.client.get_partition_names_ps(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, part_vals, this.shrinkMaxtoShort(max_parts)));
//    }
//
//    public int getNumPartitionsByFilter(String db_name, String tbl_name, String filter) throws TException {
//        return this.getNumPartitionsByFilter(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, filter);
//    }
//
//    public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter) throws TException {
//        return this.client.get_num_partitions_by_filter(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, filter);
//    }
//
//    public void alter_partition(String dbName, String tblName, Partition newPart) throws InvalidOperationException, MetaException, TException {
//        this.alter_partition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newPart, (EnvironmentContext) null);
//    }
//
//    public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
//        this.alter_partition(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newPart, environmentContext);
//    }
//
//    public void alter_partition(String catName, String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext) throws TException {
//        this.client.alter_partition_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, newPart, environmentContext);
//    }
//
//    public void alter_partitions(String dbName, String tblName, List<Partition> newParts) throws TException {
//        this.alter_partitions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newParts, (EnvironmentContext) null);
//    }
//
//    public void alter_partitions(String dbName, String tblName, List<Partition> newParts, EnvironmentContext environmentContext) throws TException {
//        this.alter_partitions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, newParts, environmentContext);
//    }
//
//    public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts, EnvironmentContext environmentContext) throws TException {
//        this.client.alter_partitions_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, newParts, environmentContext);
//    }
//
//    public void alterDatabase(String dbName, Database db) throws TException {
//        this.alterDatabase(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, db);
//    }
//
//    public void alterDatabase(String catName, String dbName, Database newDb) throws TException {
//        this.client.alter_database(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), newDb);
//    }
//
//    public List<FieldSchema> getFields(String db, String tableName) throws TException {
//        return this.getFields(MetaStoreUtils.getDefaultCatalog(this.conf), db, tableName);
//    }
//
//    public List<FieldSchema> getFields(String catName, String db, String tableName) throws TException {
//        List<FieldSchema> fields = this.client.get_fields(MetaStoreUtils.prependCatalogToDbName(catName, db, this.conf), tableName);
//        return this.deepCopyFieldSchemas(fields);
//    }
//
//    public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest req) throws TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_primary_keys(req).getPrimaryKeys();
//    }
//
//    public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest req) throws MetaException, NoSuchObjectException, TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_foreign_keys(req).getForeignKeys();
//    }
//
//    public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest req) throws MetaException, NoSuchObjectException, TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_unique_constraints(req).getUniqueConstraints();
//    }
//
//    public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest req) throws MetaException, NoSuchObjectException, TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_not_null_constraints(req).getNotNullConstraints();
//    }
//
//    public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest req) throws MetaException, NoSuchObjectException, TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_default_constraints(req).getDefaultConstraints();
//    }
//
//    public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest req) throws MetaException, NoSuchObjectException, TException {
//        if (!req.isSetCatName()) {
//            req.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_check_constraints(req).getCheckConstraints();
//    }
//
//    public boolean updateTableColumnStatistics(ColumnStatistics statsObj) throws TException {
//        if (!statsObj.getStatsDesc().isSetCatName()) {
//            statsObj.getStatsDesc().setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.update_table_column_statistics(statsObj);
//    }
//
//    public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj) throws TException {
//        if (!statsObj.getStatsDesc().isSetCatName()) {
//            statsObj.getStatsDesc().setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.update_partition_column_statistics(statsObj);
//    }
//
//    public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
//        String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//        Iterator var3 = request.getColStats().iterator();
//
//        while (var3.hasNext()) {
//            ColumnStatistics stats = (ColumnStatistics) var3.next();
//            if (!stats.getStatsDesc().isSetCatName()) {
//                stats.getStatsDesc().setCatName(defaultCat);
//            }
//        }
//
//        return this.client.set_aggr_stats_for(request);
//    }
//
//    public void flushCache() {
//        try {
//            this.client.flushCache();
//        } catch (TException var2) {
//            TException e = var2;
//            LOG.warn("Got error flushing the cache", e);
//        }
//
//    }
//
//    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) throws TException {
//        return this.getTableColumnStatistics(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, colNames);
//    }
//
//    public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName, List<String> colNames) throws TException {
//        TableStatsRequest rqst = new TableStatsRequest(dbName, tableName, colNames);
//        rqst.setCatName(catName);
//        return this.client.get_table_statistics_req(rqst).getTableStats();
//    }
//
//    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName, List<String> partNames, List<String> colNames) throws TException {
//        return this.getPartitionColumnStatistics(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partNames, colNames);
//    }
//
//    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName, String tableName, List<String> partNames, List<String> colNames) throws TException {
//        PartitionsStatsRequest rqst = new PartitionsStatsRequest(dbName, tableName, colNames, partNames);
//        rqst.setCatName(catName);
//        return this.client.get_partitions_statistics_req(rqst).getPartStats();
//    }
//
//    public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName) throws TException {
//        return this.deletePartitionColumnStatistics(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, partName, colName);
//    }
//
//    public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName, String colName) throws TException {
//        return this.client.delete_partition_column_statistics(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, partName, colName);
//    }
//
//    public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws TException {
//        return this.deleteTableColumnStatistics(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tableName, colName);
//    }
//
//    public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName) throws TException {
//        return this.client.delete_table_column_statistics(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tableName, colName);
//    }
//
//    public List<FieldSchema> getSchema(String db, String tableName) throws TException {
//        return this.getSchema(MetaStoreUtils.getDefaultCatalog(this.conf), db, tableName);
//    }
//
//    public List<FieldSchema> getSchema(String catName, String db, String tableName) throws TException {
//        EnvironmentContext envCxt = null;
//        String addedJars = MetastoreConf.getVar(this.conf, ConfVars.ADDED_JARS);
//        if (org.apache.commons.lang.StringUtils.isNotBlank(addedJars)) {
//            Map<String, String> props = new HashMap();
//            props.put("hive.added.jars.path", addedJars);
//            envCxt = new EnvironmentContext(props);
//        }
//
//        List<FieldSchema> fields = this.client.get_schema_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, db, this.conf), tableName, envCxt);
//        return this.deepCopyFieldSchemas(fields);
//    }
//
//    public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
//        return this.client.get_config_value(name, defaultValue);
//    }
//
//    public Partition getPartition(String db, String tableName, String partName) throws TException {
//        return this.getPartition(MetaStoreUtils.getDefaultCatalog(this.conf), db, tableName, partName);
//    }
//
//    public Partition getPartition(String catName, String dbName, String tblName, String name) throws TException {
//        Partition p = this.client.get_partition_by_name(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), tblName, name);
//        return this.deepCopy(this.filterHook.filterPartition(p));
//    }
//
//    public Partition appendPartitionByName(String dbName, String tableName, String partName) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
//        return this.appendPartitionByName(dbName, tableName, partName, (EnvironmentContext) null);
//    }
//
//    public Partition appendPartitionByName(String dbName, String tableName, String partName, EnvironmentContext envContext) throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
//        Partition p = this.client.append_partition_by_name_with_environment_context(dbName, tableName, partName, envContext);
//        return this.deepCopy(p);
//    }
//
//    public boolean dropPartitionByName(String dbName, String tableName, String partName, boolean deleteData) throws NoSuchObjectException, MetaException, TException {
//        return this.dropPartitionByName(dbName, tableName, partName, deleteData, (EnvironmentContext) null);
//    }
//
//    public boolean dropPartitionByName(String dbName, String tableName, String partName, boolean deleteData, EnvironmentContext envContext) throws NoSuchObjectException, MetaException, TException {
//        return this.client.drop_partition_by_name_with_environment_context(dbName, tableName, partName, deleteData, envContext);
//    }
//
//    private HiveMetaHook getHook(Table tbl) throws MetaException {
//        return this.hookLoader == null ? null : this.hookLoader.getHook(tbl);
//    }
//
//    public List<String> partitionNameToVals(String name) throws MetaException, TException {
//        return this.client.partition_name_to_vals(name);
//    }
//
//    public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
//        return this.client.partition_name_to_spec(name);
//    }
//
//    protected Partition deepCopy(Partition partition) {
//        Partition copy = null;
//        if (partition != null) {
//            copy = new Partition(partition);
//        }
//
//        return copy;
//    }
//
//    private Database deepCopy(Database database) {
//        Database copy = null;
//        if (database != null) {
//            copy = new Database(database);
//        }
//
//        return copy;
//    }
//
//    protected Table deepCopy(Table table) {
//        Table copy = null;
//        if (table != null) {
//            copy = new Table(table);
//        }
//
//        return copy;
//    }
//
//    private Type deepCopy(Type type) {
//        Type copy = null;
//        if (type != null) {
//            copy = new Type(type);
//        }
//
//        return copy;
//    }
//
//    private FieldSchema deepCopy(FieldSchema schema) {
//        FieldSchema copy = null;
//        if (schema != null) {
//            copy = new FieldSchema(schema);
//        }
//
//        return copy;
//    }
//
//    private Function deepCopy(Function func) {
//        Function copy = null;
//        if (func != null) {
//            copy = new Function(func);
//        }
//
//        return copy;
//    }
//
//    protected PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
//        PrincipalPrivilegeSet copy = null;
//        if (pps != null) {
//            copy = new PrincipalPrivilegeSet(pps);
//        }
//
//        return copy;
//    }
//
//    private List<Partition> deepCopyPartitions(List<Partition> partitions) {
//        return this.deepCopyPartitions(partitions, (List) null);
//    }
//
//    private List<Partition> deepCopyPartitions(Collection<Partition> src, List<Partition> dest) {
//        if (src == null) {
//            return (List) dest;
//        } else {
//            if (dest == null) {
//                dest = new ArrayList(src.size());
//            }
//
//            Iterator var3 = src.iterator();
//
//            while (var3.hasNext()) {
//                Partition part = (Partition) var3.next();
//                ((List) dest).add(this.deepCopy(part));
//            }
//
//            return (List) dest;
//        }
//    }
//
//    private List<Table> deepCopyTables(List<Table> tables) {
//        List<Table> copy = null;
//        if (tables != null) {
//            copy = new ArrayList();
//            Iterator var3 = tables.iterator();
//
//            while (var3.hasNext()) {
//                Table tab = (Table) var3.next();
//                copy.add(this.deepCopy(tab));
//            }
//        }
//
//        return copy;
//    }
//
//    protected List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
//        List<FieldSchema> copy = null;
//        if (schemas != null) {
//            copy = new ArrayList();
//            Iterator var3 = schemas.iterator();
//
//            while (var3.hasNext()) {
//                FieldSchema schema = (FieldSchema) var3.next();
//                copy.add(this.deepCopy(schema));
//            }
//        }
//
//        return copy;
//    }
//
//    public boolean grant_role(String roleName, String userName, PrincipalType principalType, String grantor, PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
//        GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
//        req.setRequestType(GrantRevokeType.GRANT);
//        req.setRoleName(roleName);
//        req.setPrincipalName(userName);
//        req.setPrincipalType(principalType);
//        req.setGrantor(grantor);
//        req.setGrantorType(grantorType);
//        req.setGrantOption(grantOption);
//        GrantRevokeRoleResponse res = this.client.grant_revoke_role(req);
//        if (!res.isSetSuccess()) {
//            throw new MetaException("GrantRevokeResponse missing success field");
//        } else {
//            return res.isSuccess();
//        }
//    }
//
//    public boolean create_role(Role role) throws MetaException, TException {
//        return this.client.create_role(role);
//    }
//
//    public boolean drop_role(String roleName) throws MetaException, TException {
//        return this.client.drop_role(roleName);
//    }
//
//    public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
//        return this.client.list_roles(principalName, principalType);
//    }
//
//    public List<String> listRoleNames() throws MetaException, TException {
//        return this.client.get_role_names();
//    }
//
//    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest req) throws MetaException, TException {
//        return this.client.get_principals_in_role(req);
//    }
//
//    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
//        return this.client.get_role_grants_for_principal(getRolePrincReq);
//    }
//
//    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
//        String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//        Iterator var3 = privileges.getPrivileges().iterator();
//
//        while (var3.hasNext()) {
//            HiveObjectPrivilege priv = (HiveObjectPrivilege) var3.next();
//            if (!priv.getHiveObject().isSetCatName()) {
//                priv.getHiveObject().setCatName(defaultCat);
//            }
//        }
//
//        GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
//        req.setRequestType(GrantRevokeType.GRANT);
//        req.setPrivileges(privileges);
//        GrantRevokePrivilegeResponse res = this.client.grant_revoke_privileges(req);
//        if (!res.isSetSuccess()) {
//            throw new MetaException("GrantRevokePrivilegeResponse missing success field");
//        } else {
//            return res.isSuccess();
//        }
//    }
//
//    public boolean revoke_role(String roleName, String userName, PrincipalType principalType, boolean grantOption) throws MetaException, TException {
//        GrantRevokeRoleRequest req = new GrantRevokeRoleRequest();
//        req.setRequestType(GrantRevokeType.REVOKE);
//        req.setRoleName(roleName);
//        req.setPrincipalName(userName);
//        req.setPrincipalType(principalType);
//        req.setGrantOption(grantOption);
//        GrantRevokeRoleResponse res = this.client.grant_revoke_role(req);
//        if (!res.isSetSuccess()) {
//            throw new MetaException("GrantRevokeResponse missing success field");
//        } else {
//            return res.isSuccess();
//        }
//    }
//
//    public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException, TException {
//        String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//        Iterator var4 = privileges.getPrivileges().iterator();
//
//        while (var4.hasNext()) {
//            HiveObjectPrivilege priv = (HiveObjectPrivilege) var4.next();
//            if (!priv.getHiveObject().isSetCatName()) {
//                priv.getHiveObject().setCatName(defaultCat);
//            }
//        }
//
//        GrantRevokePrivilegeRequest req = new GrantRevokePrivilegeRequest();
//        req.setRequestType(GrantRevokeType.REVOKE);
//        req.setPrivileges(privileges);
//        req.setRevokeGrantOption(grantOption);
//        GrantRevokePrivilegeResponse res = this.client.grant_revoke_privileges(req);
//        if (!res.isSetSuccess()) {
//            throw new MetaException("GrantRevokePrivilegeResponse missing success field");
//        } else {
//            return res.isSuccess();
//        }
//    }
//
//    public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges) throws MetaException, TException {
//        String defaultCat = MetaStoreUtils.getDefaultCatalog(this.conf);
//        objToRefresh.setCatName(defaultCat);
//        if (grantPrivileges.getPrivileges() != null) {
//            Iterator var5 = grantPrivileges.getPrivileges().iterator();
//
//            while (var5.hasNext()) {
//                HiveObjectPrivilege priv = (HiveObjectPrivilege) var5.next();
//                if (!priv.getHiveObject().isSetCatName()) {
//                    priv.getHiveObject().setCatName(defaultCat);
//                }
//            }
//        }
//
//        GrantRevokePrivilegeRequest grantReq = new GrantRevokePrivilegeRequest();
//        grantReq.setRequestType(GrantRevokeType.GRANT);
//        grantReq.setPrivileges(grantPrivileges);
//        GrantRevokePrivilegeResponse res = this.client.refresh_privileges(objToRefresh, authorizer, grantReq);
//        if (!res.isSetSuccess()) {
//            throw new MetaException("GrantRevokePrivilegeResponse missing success field");
//        } else {
//            return res.isSuccess();
//        }
//    }
//
//    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String userName, List<String> groupNames) throws MetaException, TException {
//        if (!hiveObject.isSetCatName()) {
//            hiveObject.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_privilege_set(hiveObject, userName, groupNames);
//    }
//
//    public List<HiveObjectPrivilege> list_privileges(String principalName, PrincipalType principalType, HiveObjectRef hiveObject) throws MetaException, TException {
//        if (!hiveObject.isSetCatName()) {
//            hiveObject.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.list_privileges(principalName, principalType, hiveObject);
//    }
//
//    public String getDelegationToken(String renewerKerberosPrincipalName) throws MetaException, TException, IOException {
//        String owner = SecurityUtils.getUser();
//        return this.getDelegationToken(owner, renewerKerberosPrincipalName);
//    }
//
//    public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws MetaException, TException {
//        return this.localMetaStore ? null : this.client.get_delegation_token(owner, renewerKerberosPrincipalName);
//    }
//
//    public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
//        return this.localMetaStore ? 0L : this.client.renew_delegation_token(tokenStrForm);
//    }
//
//    public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
//        if (!this.localMetaStore) {
//            this.client.cancel_delegation_token(tokenStrForm);
//        }
//    }
//
//    public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
//        return this.client.add_token(tokenIdentifier, delegationToken);
//    }
//
//    public boolean removeToken(String tokenIdentifier) throws TException {
//        return this.client.remove_token(tokenIdentifier);
//    }
//
//    public String getToken(String tokenIdentifier) throws TException {
//        return this.client.get_token(tokenIdentifier);
//    }
//
//    public List<String> getAllTokenIdentifiers() throws TException {
//        return this.client.get_all_token_identifiers();
//    }
//
//    public int addMasterKey(String key) throws MetaException, TException {
//        return this.client.add_master_key(key);
//    }
//
//    public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {
//        this.client.update_master_key(seqNo, key);
//    }
//
//    public boolean removeMasterKey(Integer keySeq) throws TException {
//        return this.client.remove_master_key(keySeq);
//    }
//
//    public String[] getMasterKeys() throws TException {
//        List<String> keyList = this.client.get_master_keys();
//        return (String[]) keyList.toArray(new String[keyList.size()]);
//    }
//
//    public ValidTxnList getValidTxns() throws TException {
//        return TxnUtils.createValidReadTxnList(this.client.get_open_txns(), 0L);
//    }
//
//    public ValidTxnList getValidTxns(long currentTxn) throws TException {
//        return TxnUtils.createValidReadTxnList(this.client.get_open_txns(), currentTxn);
//    }
//
//    public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
//        GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(Collections.singletonList(fullTableName), (String) null);
//        GetValidWriteIdsResponse validWriteIds = this.client.get_valid_write_ids(rqst);
//        return TxnUtils.createValidReaderWriteIdList((TableValidWriteIds) validWriteIds.getTblValidWriteIds().get(0));
//    }
//
//    public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
//        GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tablesList, validTxnList);
//        return this.client.get_valid_write_ids(rqst).getTblValidWriteIds();
//    }
//
//    public long openTxn(String user) throws TException {
//        OpenTxnsResponse txns = this.openTxnsIntr(user, 1, (String) null, (List) null);
//        return (Long) txns.getTxn_ids().get(0);
//    }
//
//    public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException {
//        OpenTxnsResponse txns = this.openTxnsIntr(user, srcTxnIds.size(), replPolicy, srcTxnIds);
//        return txns.getTxn_ids();
//    }
//
//    public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
//        return this.openTxnsIntr(user, numTxns, (String) null, (List) null);
//    }
//
//    private OpenTxnsResponse openTxnsIntr(String user, int numTxns, String replPolicy, List<Long> srcTxnIds) throws TException {
//        String hostname;
//        try {
//            hostname = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException var7) {
//            UnknownHostException e = var7;
//            LOG.error("Unable to resolve my host name " + e.getMessage());
//            throw new RuntimeException(e);
//        }
//
//        OpenTxnRequest rqst = new OpenTxnRequest(numTxns, user, hostname);
//        if (replPolicy != null) {
//            assert srcTxnIds != null;
//
//            assert numTxns == srcTxnIds.size();
//
//            rqst.setReplPolicy(replPolicy);
//            rqst.setReplSrcTxnIds(srcTxnIds);
//        } else {
//            assert srcTxnIds == null;
//        }
//
//        return this.client.open_txns(rqst);
//    }
//
//    public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
//        this.client.abort_txn(new AbortTxnRequest(txnid));
//    }
//
//    public void replRollbackTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TException {
//        AbortTxnRequest rqst = new AbortTxnRequest(srcTxnId);
//        rqst.setReplPolicy(replPolicy);
//        this.client.abort_txn(rqst);
//    }
//
//    public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {
//        this.client.commit_txn(new CommitTxnRequest(txnid));
//    }
//
//    public void replCommitTxn(long srcTxnId, String replPolicy) throws NoSuchTxnException, TxnAbortedException, TException {
//        CommitTxnRequest rqst = new CommitTxnRequest(srcTxnId);
//        rqst.setReplPolicy(replPolicy);
//        this.client.commit_txn(rqst);
//    }
//
//    public GetOpenTxnsInfoResponse showTxns() throws TException {
//        return this.client.get_open_txns_info();
//    }
//
//    public void abortTxns(List<Long> txnids) throws NoSuchTxnException, TException {
//        this.client.abort_txns(new AbortTxnsRequest(txnids));
//    }
//
//    public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames) throws TException {
//        String user;
//        try {
//            user = UserGroupInformation.getCurrentUser().getUserName();
//        } catch (IOException var9) {
//            IOException e = var9;
//            LOG.error("Unable to resolve current user name " + e.getMessage());
//            throw new RuntimeException(e);
//        }
//
//        String hostName;
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException var8) {
//            UnknownHostException e = var8;
//            LOG.error("Unable to resolve my host name " + e.getMessage());
//            throw new RuntimeException(e);
//        }
//
//        ReplTblWriteIdStateRequest rqst = new ReplTblWriteIdStateRequest(validWriteIdList, user, hostName, dbName, tableName);
//        if (partNames != null) {
//            rqst.setPartNames(partNames);
//        }
//
//        this.client.repl_tbl_writeid_state(rqst);
//    }
//
//    public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
//        return ((TxnToWriteId) this.allocateTableWriteIdsBatch(Collections.singletonList(txnId), dbName, tableName).get(0)).getWriteId();
//    }
//
//    public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException {
//        AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
//        rqst.setTxnIds(txnIds);
//        return this.allocateTableWriteIdsBatchIntr(rqst);
//    }
//
//    public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy, List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
//        AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(dbName, tableName);
//        rqst.setReplPolicy(replPolicy);
//        rqst.setSrcTxnToWriteIdList(srcTxnToWriteIdList);
//        return this.allocateTableWriteIdsBatchIntr(rqst);
//    }
//
//    private List<TxnToWriteId> allocateTableWriteIdsBatchIntr(AllocateTableWriteIdsRequest rqst) throws TException {
//        return this.client.allocate_table_write_ids(rqst).getTxnToWriteIds();
//    }
//
//    public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
//        return this.client.lock(request);
//    }
//
//    public LockResponse checkLock(long lockid) throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
//        return this.client.check_lock(new CheckLockRequest(lockid));
//    }
//
//    public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
//        this.client.unlock(new UnlockRequest(lockid));
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public ShowLocksResponse showLocks() throws TException {
//        return this.client.show_locks(new ShowLocksRequest());
//    }
//
//    public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
//        return this.client.show_locks(request);
//    }
//
//    public void heartbeat(long txnid, long lockid) throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
//        HeartbeatRequest hb = new HeartbeatRequest();
//        hb.setLockid(lockid);
//        hb.setTxnid(txnid);
//        this.client.heartbeat(hb);
//    }
//
//    public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws NoSuchTxnException, TxnAbortedException, TException {
//        HeartbeatTxnRangeRequest rqst = new HeartbeatTxnRangeRequest(min, max);
//        return this.client.heartbeat_txn_range(rqst);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {
//        CompactionRequest cr = new CompactionRequest();
//        if (dbname == null) {
//            cr.setDbname("default");
//        } else {
//            cr.setDbname(dbname);
//        }
//
//        cr.setTablename(tableName);
//        if (partitionName != null) {
//            cr.setPartitionname(partitionName);
//        }
//
//        cr.setType(type);
//        this.client.compact(cr);
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public void compact(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {
//        this.compact2(dbname, tableName, partitionName, type, tblproperties);
//    }
//
//    public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type, Map<String, String> tblproperties) throws TException {
//        CompactionRequest cr = new CompactionRequest();
//        if (dbname == null) {
//            cr.setDbname("default");
//        } else {
//            cr.setDbname(dbname);
//        }
//
//        cr.setTablename(tableName);
//        if (partitionName != null) {
//            cr.setPartitionname(partitionName);
//        }
//
//        cr.setType(type);
//        cr.setProperties(tblproperties);
//        return this.client.compact2(cr);
//    }
//
//    public ShowCompactResponse showCompactions() throws TException {
//        return this.client.show_compact(new ShowCompactRequest());
//    }
//
//    /**
//     * @deprecated
//     */
//    @Deprecated
//    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames) throws TException {
//        this.client.add_dynamic_partitions(new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames));
//    }
//
//    public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames, DataOperationType operationType) throws TException {
//        AddDynamicPartitions adp = new AddDynamicPartitions(txnId, writeId, dbName, tableName, partNames);
//        adp.setOperationType(operationType);
//        this.client.add_dynamic_partitions(adp);
//    }
//
//    public void insertTable(Table table, boolean overwrite) throws MetaException {
//        boolean failed = true;
//        HiveMetaHook hook = this.getHook(table);
//        if (hook != null && hook instanceof DefaultHiveMetaHook) {
//            DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
//
//            try {
//                hiveMetaHook.commitInsertTable(table, overwrite);
//                failed = false;
//            } finally {
//                if (failed) {
//                    hiveMetaHook.rollbackInsertTable(table, overwrite);
//                }
//
//            }
//
//        }
//    }
//
//    @LimitedPrivate({"HCatalog"})
//    public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, IMetaStoreClient.NotificationFilter filter) throws TException {
//        NotificationEventRequest rqst = new NotificationEventRequest(lastEventId);
//        rqst.setMaxEvents(maxEvents);
//        NotificationEventResponse rsp = this.client.get_next_notification(rqst);
//        LOG.debug("Got back " + rsp.getEventsSize() + " events");
//        NotificationEventResponse filtered = new NotificationEventResponse();
//        if (rsp != null && rsp.getEvents() != null) {
//            long nextEventId = lastEventId + 1L;
//
//            for (Iterator var10 = rsp.getEvents().iterator(); var10.hasNext(); ++nextEventId) {
//                NotificationEvent e = (NotificationEvent) var10.next();
//                if (e.getEventId() != nextEventId) {
//                    LOG.error("Requested events are found missing in NOTIFICATION_LOG table. Expected: {}, Actual: {}. Probably, cleaner would've cleaned it up. Try setting higher value for hive.metastore.event.db.listener.timetolive. Also, bootstrap the system again to get back the consistent replicated state.", nextEventId, e.getEventId());
//                    throw new IllegalStateException("Notification events are missing in the meta store.");
//                }
//
//                if (filter != null && filter.accept(e)) {
//                    filtered.addToEvents(e);
//                }
//            }
//        }
//
//        return filter != null ? filtered : rsp;
//    }
//
//    @LimitedPrivate({"HCatalog"})
//    public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
//        return this.client.get_current_notificationEventId();
//    }
//
//    @LimitedPrivate({"HCatalog"})
//    public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) throws TException {
//        if (!rqst.isSetCatName()) {
//            rqst.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.get_notification_events_count(rqst);
//    }
//
//    @LimitedPrivate({"Apache Hive, HCatalog"})
//    public FireEventResponse fireListenerEvent(FireEventRequest rqst) throws TException {
//        if (!rqst.isSetCatName()) {
//            rqst.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        return this.client.fire_listener_event(rqst);
//    }
//
//    public static IMetaStoreClient newSynchronizedClient(IMetaStoreClient client) {
//        return (IMetaStoreClient) Proxy.newProxyInstance(HiveMetaStoreClient.class.getClassLoader(), new Class[]{IMetaStoreClient.class}, new SynchronizedHandler(client));
//    }
//
//    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws TException {
//        this.markPartitionForEvent(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, partKVs, eventType);
//    }
//
//    public void markPartitionForEvent(String catName, String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws TException {
//        this.client.markPartitionForEvent(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, partKVs, eventType);
//    }
//
//    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws TException {
//        return this.isPartitionMarkedForEvent(MetaStoreUtils.getDefaultCatalog(this.conf), db_name, tbl_name, partKVs, eventType);
//    }
//
//    public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name, Map<String, String> partKVs, PartitionEventType eventType) throws TException {
//        return this.client.isPartitionMarkedForEvent(MetaStoreUtils.prependCatalogToDbName(catName, db_name, this.conf), tbl_name, partKVs, eventType);
//    }
//
//    public void createFunction(Function func) throws TException {
//        if (!func.isSetCatName()) {
//            func.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        this.client.create_function(func);
//    }
//
//    public void alterFunction(String dbName, String funcName, Function newFunction) throws TException {
//        this.alterFunction(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, funcName, newFunction);
//    }
//
//    public void alterFunction(String catName, String dbName, String funcName, Function newFunction) throws TException {
//        this.client.alter_function(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), funcName, newFunction);
//    }
//
//    public void dropFunction(String dbName, String funcName) throws TException {
//        this.dropFunction(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, funcName);
//    }
//
//    public void dropFunction(String catName, String dbName, String funcName) throws TException {
//        this.client.drop_function(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), funcName);
//    }
//
//    public Function getFunction(String dbName, String funcName) throws TException {
//        return this.getFunction(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, funcName);
//    }
//
//    public Function getFunction(String catName, String dbName, String funcName) throws TException {
//        return this.deepCopy(this.client.get_function(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), funcName));
//    }
//
//    public List<String> getFunctions(String dbName, String pattern) throws TException {
//        return this.getFunctions(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, pattern);
//    }
//
//    public List<String> getFunctions(String catName, String dbName, String pattern) throws TException {
//        return this.client.get_functions(MetaStoreUtils.prependCatalogToDbName(catName, dbName, this.conf), pattern);
//    }
//
//    public GetAllFunctionsResponse getAllFunctions() throws TException {
//        return this.client.get_all_functions();
//    }
//
//    protected void create_table_with_environment_context(Table tbl, EnvironmentContext envContext) throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
//        this.client.create_table_with_environment_context(tbl, envContext);
//    }
//
//    protected void drop_table_with_environment_context(String catName, String dbname, String name, boolean deleteData, EnvironmentContext envContext) throws TException {
//        this.client.drop_table_with_environment_context(MetaStoreUtils.prependCatalogToDbName(catName, dbname, this.conf), name, deleteData, envContext);
//    }
//
//    public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partNames) throws NoSuchObjectException, MetaException, TException {
//        return this.getAggrColStatsFor(MetaStoreUtils.getDefaultCatalog(this.conf), dbName, tblName, colNames, partNames);
//    }
//
//    public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames, List<String> partNames) throws TException {
//        if (!colNames.isEmpty() && !partNames.isEmpty()) {
//            PartitionsStatsRequest req = new PartitionsStatsRequest(dbName, tblName, colNames, partNames);
//            req.setCatName(catName);
//            return this.client.get_aggr_stats_for(req);
//        } else {
//            LOG.debug("Columns is empty or partNames is empty : Short-circuiting stats eval on client side.");
//            return new AggrStats(new ArrayList(), 0L);
//        }
//    }
//
//    public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(final List<Long> fileIds) throws TException {
//        return new MetastoreMapIterable<Long, ByteBuffer>() {
//            private int listIndex = 0;
//
//            protected Map<Long, ByteBuffer> fetchNextBatch() throws TException {
//                if (this.listIndex == fileIds.size()) {
//                    return null;
//                } else {
//                    int endIndex = Math.min(this.listIndex + HiveMetaStoreClient.this.fileMetadataBatchSize, fileIds.size());
//                    List<Long> subList = fileIds.subList(this.listIndex, endIndex);
//                    GetFileMetadataResult resp = HiveMetaStoreClient.this.sendGetFileMetadataReq(subList);
//                    if (!resp.isIsSupported()) {
//                        return null;
//                    } else {
//                        this.listIndex = endIndex;
//                        return resp.getMetadata();
//                    }
//                }
//            }
//        };
//    }
//
//    private GetFileMetadataResult sendGetFileMetadataReq(List<Long> fileIds) throws TException {
//        return this.client.get_file_metadata(new GetFileMetadataRequest(fileIds));
//    }
//
//    public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(final List<Long> fileIds, final ByteBuffer sarg, final boolean doGetFooters) throws TException {
//        return new MetastoreMapIterable<Long, MetadataPpdResult>() {
//            private int listIndex = 0;
//
//            protected Map<Long, MetadataPpdResult> fetchNextBatch() throws TException {
//                if (this.listIndex == fileIds.size()) {
//                    return null;
//                } else {
//                    int endIndex = Math.min(this.listIndex + HiveMetaStoreClient.this.fileMetadataBatchSize, fileIds.size());
//                    List<Long> subList = fileIds.subList(this.listIndex, endIndex);
//                    GetFileMetadataByExprResult resp = HiveMetaStoreClient.this.sendGetFileMetadataBySargReq(sarg, subList, doGetFooters);
//                    if (!resp.isIsSupported()) {
//                        return null;
//                    } else {
//                        this.listIndex = endIndex;
//                        return resp.getMetadata();
//                    }
//                }
//            }
//        };
//    }
//
//    private GetFileMetadataByExprResult sendGetFileMetadataBySargReq(ByteBuffer sarg, List<Long> fileIds, boolean doGetFooters) throws TException {
//        GetFileMetadataByExprRequest req = new GetFileMetadataByExprRequest(fileIds, sarg);
//        req.setDoGetFooters(doGetFooters);
//        return this.client.get_file_metadata_by_expr(req);
//    }
//
//    public void clearFileMetadata(List<Long> fileIds) throws TException {
//        ClearFileMetadataRequest req = new ClearFileMetadataRequest();
//        req.setFileIds(fileIds);
//        this.client.clear_file_metadata(req);
//    }
//
//    public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
//        PutFileMetadataRequest req = new PutFileMetadataRequest();
//        req.setFileIds(fileIds);
//        req.setMetadata(metadata);
//        this.client.put_file_metadata(req);
//    }
//
//    public boolean isSameConfObj(Configuration c) {
//        return this.conf == c;
//    }
//
//    public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts) throws TException {
//        CacheFileMetadataRequest req = new CacheFileMetadataRequest();
//        req.setDbName(dbName);
//        req.setTblName(tableName);
//        if (partName != null) {
//            req.setPartName(partName);
//        } else {
//            req.setIsAllParts(allParts);
//        }
//
//        CacheFileMetadataResult result = this.client.cache_file_metadata(req);
//        return result.isIsSupported();
//    }
//
//    public String getMetastoreDbUuid() throws TException {
//        return this.client.get_metastore_db_uuid();
//    }
//
//    public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName) throws InvalidObjectException, MetaException, TException {
//        WMCreateResourcePlanRequest request = new WMCreateResourcePlanRequest();
//        request.setResourcePlan(resourcePlan);
//        request.setCopyFrom(copyFromName);
//        this.client.create_resource_plan(request);
//    }
//
//    public WMFullResourcePlan getResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
//        WMGetResourcePlanRequest request = new WMGetResourcePlanRequest();
//        request.setResourcePlanName(resourcePlanName);
//        return this.client.get_resource_plan(request).getResourcePlan();
//    }
//
//    public List<WMResourcePlan> getAllResourcePlans() throws NoSuchObjectException, MetaException, TException {
//        WMGetAllResourcePlanRequest request = new WMGetAllResourcePlanRequest();
//        return this.client.get_all_resource_plans(request).getResourcePlans();
//    }
//
//    public void dropResourcePlan(String resourcePlanName) throws NoSuchObjectException, MetaException, TException {
//        WMDropResourcePlanRequest request = new WMDropResourcePlanRequest();
//        request.setResourcePlanName(resourcePlanName);
//        this.client.drop_resource_plan(request);
//    }
//
//    public WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan resourcePlan, boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMAlterResourcePlanRequest request = new WMAlterResourcePlanRequest();
//        request.setResourcePlanName(resourcePlanName);
//        request.setResourcePlan(resourcePlan);
//        request.setIsEnableAndActivate(canActivateDisabled);
//        request.setIsForceDeactivate(isForceDeactivate);
//        request.setIsReplace(isReplace);
//        WMAlterResourcePlanResponse resp = this.client.alter_resource_plan(request);
//        return resp.isSetFullResourcePlan() ? resp.getFullResourcePlan() : null;
//    }
//
//    public WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException {
//        return this.client.get_active_resource_plan(new WMGetActiveResourcePlanRequest()).getResourcePlan();
//    }
//
//    public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMValidateResourcePlanRequest request = new WMValidateResourcePlanRequest();
//        request.setResourcePlanName(resourcePlanName);
//        return this.client.validate_resource_plan(request);
//    }
//
//    public void createWMTrigger(WMTrigger trigger) throws InvalidObjectException, MetaException, TException {
//        WMCreateTriggerRequest request = new WMCreateTriggerRequest();
//        request.setTrigger(trigger);
//        this.client.create_wm_trigger(request);
//    }
//
//    public void alterWMTrigger(WMTrigger trigger) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMAlterTriggerRequest request = new WMAlterTriggerRequest();
//        request.setTrigger(trigger);
//        this.client.alter_wm_trigger(request);
//    }
//
//    public void dropWMTrigger(String resourcePlanName, String triggerName) throws NoSuchObjectException, MetaException, TException {
//        WMDropTriggerRequest request = new WMDropTriggerRequest();
//        request.setResourcePlanName(resourcePlanName);
//        request.setTriggerName(triggerName);
//        this.client.drop_wm_trigger(request);
//    }
//
//    public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan) throws NoSuchObjectException, MetaException, TException {
//        WMGetTriggersForResourePlanRequest request = new WMGetTriggersForResourePlanRequest();
//        request.setResourcePlanName(resourcePlan);
//        return this.client.get_triggers_for_resourceplan(request).getTriggers();
//    }
//
//    public void createWMPool(WMPool pool) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMCreatePoolRequest request = new WMCreatePoolRequest();
//        request.setPool(pool);
//        this.client.create_wm_pool(request);
//    }
//
//    public void alterWMPool(WMNullablePool pool, String poolPath) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMAlterPoolRequest request = new WMAlterPoolRequest();
//        request.setPool(pool);
//        request.setPoolPath(poolPath);
//        this.client.alter_wm_pool(request);
//    }
//
//    public void dropWMPool(String resourcePlanName, String poolPath) throws NoSuchObjectException, MetaException, TException {
//        WMDropPoolRequest request = new WMDropPoolRequest();
//        request.setResourcePlanName(resourcePlanName);
//        request.setPoolPath(poolPath);
//        this.client.drop_wm_pool(request);
//    }
//
//    public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMCreateOrUpdateMappingRequest request = new WMCreateOrUpdateMappingRequest();
//        request.setMapping(mapping);
//        request.setUpdate(isUpdate);
//        this.client.create_or_update_wm_mapping(request);
//    }
//
//    public void dropWMMapping(WMMapping mapping) throws NoSuchObjectException, MetaException, TException {
//        WMDropMappingRequest request = new WMDropMappingRequest();
//        request.setMapping(mapping);
//        this.client.drop_wm_mapping(request);
//    }
//
//    public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath, boolean shouldDrop) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
//        WMCreateOrDropTriggerToPoolMappingRequest request = new WMCreateOrDropTriggerToPoolMappingRequest();
//        request.setResourcePlanName(resourcePlanName);
//        request.setTriggerName(triggerName);
//        request.setPoolPath(poolPath);
//        request.setDrop(shouldDrop);
//        this.client.create_or_drop_wm_trigger_to_pool_mapping(request);
//    }
//
//    public void createISchema(ISchema schema) throws TException {
//        if (!schema.isSetCatName()) {
//            schema.setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        this.client.create_ischema(schema);
//    }
//
//    public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
//        this.client.alter_ischema(new AlterISchemaRequest(new ISchemaName(catName, dbName, schemaName), newSchema));
//    }
//
//    public ISchema getISchema(String catName, String dbName, String name) throws TException {
//        return this.client.get_ischema(new ISchemaName(catName, dbName, name));
//    }
//
//    public void dropISchema(String catName, String dbName, String name) throws TException {
//        this.client.drop_ischema(new ISchemaName(catName, dbName, name));
//    }
//
//    public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
//        if (!schemaVersion.getSchema().isSetCatName()) {
//            schemaVersion.getSchema().setCatName(MetaStoreUtils.getDefaultCatalog(this.conf));
//        }
//
//        this.client.add_schema_version(schemaVersion);
//    }
//
//    public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
//        return this.client.get_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
//    }
//
//    public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
//        return this.client.get_schema_latest_version(new ISchemaName(catName, dbName, schemaName));
//    }
//
//    public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
//        return this.client.get_schema_all_versions(new ISchemaName(catName, dbName, schemaName));
//    }
//
//    public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
//        this.client.drop_schema_version(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version));
//    }
//
//    public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
//        return this.client.get_schemas_by_cols(rqst);
//    }
//
//    public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName) throws TException {
//        this.client.map_schema_version_to_serde(new MapSchemaVersionToSerdeRequest(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version), serdeName));
//    }
//
//    public void setSchemaVersionState(String catName, String dbName, String schemaName, int version, SchemaVersionState state) throws TException {
//        this.client.set_schema_version_state(new SetSchemaVersionStateRequest(new SchemaVersionDescriptor(new ISchemaName(catName, dbName, schemaName), version), state));
//    }
//
//    public void addSerDe(SerDeInfo serDeInfo) throws TException {
//        this.client.add_serde(serDeInfo);
//    }
//
//    public SerDeInfo getSerDe(String serDeName) throws TException {
//        return this.client.get_serde(new GetSerdeRequest(serDeName));
//    }
//
//    private short shrinkMaxtoShort(int max) {
//        if (max < 0) {
//            return -1;
//        } else {
//            return max <= 32767 ? (short) max : 32767;
//        }
//    }
//
//    public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
//        return this.client.get_lock_materialization_rebuild(dbName, tableName, txnId);
//    }
//
//    public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
//        return this.client.heartbeat_lock_materialization_rebuild(dbName, tableName, txnId);
//    }
//
//    public void addRuntimeStat(RuntimeStat stat) throws TException {
//        this.client.add_runtime_stats(stat);
//    }
//
//    public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
//        GetRuntimeStatsRequest req = new GetRuntimeStatsRequest();
//        req.setMaxWeight(maxWeight);
//        req.setMaxCreateTime(maxCreateTime);
//        return this.client.get_runtime_stats(req);
//    }
//
//    static {
//        VERSION = new ClientCapabilities(Lists.newArrayList(new ClientCapability[]{ClientCapability.INSERT_ONLY_TABLES}));
//        TEST_VERSION = new ClientCapabilities(Lists.newArrayList(new ClientCapability[]{ClientCapability.INSERT_ONLY_TABLES, ClientCapability.TEST_CAPABILITY}));
//        connCount = new AtomicInteger(0);
//        LOG = LoggerFactory.getLogger(HiveMetaStoreClient.class);
//    }
//
//    public abstract static class MetastoreMapIterable<K, V> implements Iterable<Map.Entry<K, V>>, Iterator<Map.Entry<K, V>> {
//        private Iterator<Map.Entry<K, V>> currentIter;
//
//        public MetastoreMapIterable() {
//        }
//
//        protected abstract Map<K, V> fetchNextBatch() throws TException;
//
//        public Iterator<Map.Entry<K, V>> iterator() {
//            return this;
//        }
//
//        public boolean hasNext() {
//            this.ensureCurrentBatch();
//            return this.currentIter != null;
//        }
//
//        private void ensureCurrentBatch() {
//            if (this.currentIter == null || !this.currentIter.hasNext()) {
//                this.currentIter = null;
//
//                Map currentBatch;
//                do {
//                    try {
//                        currentBatch = this.fetchNextBatch();
//                    } catch (TException var3) {
//                        TException ex = var3;
//                        throw new RuntimeException(ex);
//                    }
//
//                    if (currentBatch == null) {
//                        return;
//                    }
//                } while (currentBatch.isEmpty());
//
//                this.currentIter = currentBatch.entrySet().iterator();
//            }
//        }
//
//        public Map.Entry<K, V> next() {
//            this.ensureCurrentBatch();
//            if (this.currentIter == null) {
//                throw new NoSuchElementException();
//            } else {
//                return (Map.Entry) this.currentIter.next();
//            }
//        }
//
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//    }
//
//    private static class SynchronizedHandler implements InvocationHandler {
//        private final IMetaStoreClient client;
//
//        SynchronizedHandler(IMetaStoreClient client) {
//            this.client = client;
//        }
//
//        public synchronized Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//            try {
//                return method.invoke(this.client, args);
//            } catch (InvocationTargetException var5) {
//                InvocationTargetException e = var5;
//                throw e.getTargetException();
//            }
//        }
//    }
//}

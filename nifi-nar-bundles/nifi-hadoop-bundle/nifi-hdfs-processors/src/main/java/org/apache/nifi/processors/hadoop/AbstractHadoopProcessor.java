/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;

/**
 * This is a base class that is helpful when building processors interacting with HDFS.
 */
public abstract class AbstractHadoopProcessor extends AbstractProcessor {
    /**
     * Compression Type Enum
     */
    public enum CompressionType {
        NONE,
        DEFAULT,
        BZIP,
        GZIP,
        LZ4,
        SNAPPY,
        AUTOMATIC;

        @Override
        public String toString() {
            switch (this) {
                case NONE: return "NONE";
                case DEFAULT: return DefaultCodec.class.getName();
                case BZIP: return BZip2Codec.class.getName();
                case GZIP: return GzipCodec.class.getName();
                case LZ4: return Lz4Codec.class.getName();
                case SNAPPY: return SnappyCodec.class.getName();
                case AUTOMATIC: return "Automatically Detected";
            }
            return null;
        }
    }


    private static final Validator KERBEROS_CONFIG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Check that both the principal & keytab are set before checking the kerberos config
            if (context.getProperty(KERBEROS_KEYTAB).getValue() == null || context.getProperty(KERBEROS_PRINCIPAL).getValue() == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("both keytab and principal must be set in order to use Kerberos authentication").build();
            }

            // Check that the Kerberos configuration is set
            if (NIFI_PROPERTIES.getKerberosConfigurationFile() == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation("you are missing the nifi.kerberos.krb5.file property in nifi.properties. " + "This must be set in order to use Kerberos").build();
            }

            // Check that the Kerberos configuration is readable
            if (!NIFI_PROPERTIES.getKerberosConfigurationFile().canRead()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                        .explanation(String.format("unable to read Kerberos config [%s], please make sure the path is valid " + "and nifi has adequate permissions",
                                NIFI_PROPERTIES.getKerberosConfigurationFile().getAbsoluteFile()))
                        .build();
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    };

    // properties
    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder().name("Hadoop Configuration Resources")
            .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                    + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration.")
            .required(false).addValidator(createMultipleFilesExistValidator()).build();

    public static NiFiProperties NIFI_PROPERTIES = null;

    public static final String DIRECTORY_PROP_NAME = "Directory";

    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder().name("Compression codec").required(true)
            .allowableValues(CompressionType.values()).defaultValue(CompressionType.NONE.toString()).build();

    public static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder().name("Kerberos Principal").required(false)
            .description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set " + "in your nifi.properties").addValidator(Validator.VALID)
            .addValidator(KERBEROS_CONFIG_VALIDATOR).build();

    public static final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder().name("Kerberos Keytab").required(false)
            .description("Kerberos keytab associated with the principal. Requires nifi.kerberos.krb5.file to be set " + "in your nifi.properties").addValidator(Validator.VALID)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).addValidator(KERBEROS_CONFIG_VALIDATOR).build();

    protected static final List<PropertyDescriptor> properties;

    private static final Object RESOURCES_LOCK = new Object();

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HADOOP_CONFIGURATION_RESOURCES);
        props.add(KERBEROS_PRINCIPAL);
        props.add(KERBEROS_KEYTAB);
        properties = Collections.unmodifiableList(props);
        try {
            NIFI_PROPERTIES = NiFiProperties.getInstance();
        } catch (Exception e) {
            // This will happen during tests
            NIFI_PROPERTIES = null;
        }
        if (NIFI_PROPERTIES != null && NIFI_PROPERTIES.getKerberosConfigurationFile() != null) {
            System.setProperty("java.security.krb5.conf", NIFI_PROPERTIES.getKerberosConfigurationFile().getAbsolutePath());
        }
    }

    // variables shared by all threads of this processor
    // Hadoop Configuration and FileSystem
    private final AtomicReference<Tuple<Configuration, FileSystem>> hdfsResources = new AtomicReference<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        hdfsResources.set(new Tuple<Configuration, FileSystem>(null, null));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /*
     * If your subclass also has an @OnScheduled annotated method and you need hdfsResources in that method, then be sure to call super.abstractOnScheduled(context)
     */
    @OnScheduled
    public final void abstractOnScheduled(ProcessContext context) throws IOException {
        try {
            Tuple<Configuration, FileSystem> resources = hdfsResources.get();
            if (resources.getKey() == null || resources.getValue() == null) {
                String configResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue();
                String dir = context.getProperty(DIRECTORY_PROP_NAME).getValue();
                dir = dir == null ? "/" : dir;
                resources = resetHDFSResources(configResources, dir, context);
                hdfsResources.set(resources);
            }
        } catch (IOException ex) {
            getLogger().error("HDFS Configuration error - {}", new Object[] { ex });
            hdfsResources.set(new Tuple<Configuration, FileSystem>(null, null));
            throw ex;
        }
    }

    @OnStopped
    public final void abstractOnStopped() {
        hdfsResources.set(new Tuple<Configuration, FileSystem>(null, null));
    }

    private static Configuration getConfigurationFromResources(String configResources) throws IOException {
        boolean foundResources = false;
        final Configuration config = new Configuration();
        if (null != configResources) {
            String[] resources = configResources.split(",");
            for (String resource : resources) {
                config.addResource(new Path(resource.trim()));
                foundResources = true;
            }
        }

        if (!foundResources) {
            // check that at least 1 non-default resource is available on the classpath
            String configStr = config.toString();
            for (String resource : configStr.substring(configStr.indexOf(":") + 1).split(",")) {
                if (!resource.contains("default") && config.getResource(resource.trim()) != null) {
                    foundResources = true;
                    break;
                }
            }
        }

        if (!foundResources) {
            throw new IOException("Could not find any of the " + HADOOP_CONFIGURATION_RESOURCES.getName() + " on the classpath");
        }
        return config;
    }

    /*
     * Reset Hadoop Configuration and FileSystem based on the supplied configuration resources.
     */
    Tuple<Configuration, FileSystem> resetHDFSResources(String configResources, String dir, ProcessContext context) throws IOException {
        // org.apache.hadoop.conf.Configuration saves its current thread context class loader to use for threads that it creates
        // later to do I/O. We need this class loader to be the NarClassLoader instead of the magical
        // NarThreadContextClassLoader.
        ClassLoader savedClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        try {
            Configuration config = getConfigurationFromResources(configResources);

            // first check for timeout on HDFS connection, because FileSystem has a hard coded 15 minute timeout
            checkHdfsUriForTimeout(config);

            // disable caching of Configuration and FileSystem objects, else we cannot reconfigure the processor without a complete
            // restart
            String disableCacheName = String.format("fs.%s.impl.disable.cache", FileSystem.getDefaultUri(config).getScheme());

            // If kerberos is enabled, create the file system as the kerberos principal
            // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at at time
            FileSystem fs = null;
            synchronized (RESOURCES_LOCK) {
                if (config.get("hadoop.security.authentication").equalsIgnoreCase("kerberos")) {
                    String principal = context.getProperty(KERBEROS_PRINCIPAL).getValue();
                    String keyTab = context.getProperty(KERBEROS_KEYTAB).getValue();
                    UserGroupInformation.setConfiguration(config);
                    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTab);
                    fs = getFileSystemAsUser(config, ugi);
                } else {
                    config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                    config.set("hadoop.security.authentication", "simple");
                    fs = getFileSystem(config);
                }
            }
            config.set(disableCacheName, "true");
            getLogger().info("Initialized a new HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                    new Object[] { fs.getWorkingDirectory(), fs.getDefaultBlockSize(new Path(dir)), fs.getDefaultReplication(new Path(dir)), config.toString() });
            return new Tuple<>(config, fs);

        } finally {
            Thread.currentThread().setContextClassLoader(savedClassLoader);
        }
    }

    /**
     * This exists in order to allow unit tests to override it so that they don't take several minutes waiting for UDP packets to be received
     *
     * @param config
     *            the configuration to use
     * @return the FileSystem that is created for the given Configuration
     * @throws IOException
     *             if unable to create the FileSystem
     */
    protected FileSystem getFileSystem(final Configuration config) throws IOException {
        return FileSystem.get(config);
    }

    protected FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(config);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage());
        }
    }

    /*
     * Drastically reduce the timeout of a socket connection from the default in FileSystem.get()
     */
    protected void checkHdfsUriForTimeout(Configuration config) throws IOException {
        URI hdfsUri = FileSystem.getDefaultUri(config);
        String address = hdfsUri.getAuthority();
        int port = hdfsUri.getPort();
        if (address == null || address.isEmpty() || port < 0) {
            return;
        }
        InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        Socket socket = null;
        try {
            socket = socketFactory.createSocket();
            NetUtils.connect(socket, namenode, 1000); // 1 second timeout
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    /*
     * Validates that one or more files exist, as specified in a single property.
     */
    public static final Validator createMultipleFilesExistValidator() {
        return new Validator() {

            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String[] files = input.split(",");
                for (String filename : files) {
                    try {
                        final File file = new File(filename.trim());
                        final boolean valid = file.exists() && file.isFile();
                        if (!valid) {
                            final String message = "File " + file + " does not exist or is not a file";
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        }
                    } catch (SecurityException e) {
                        final String message = "Unable to access " + filename + " due to " + e.getMessage();
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

        };
    }

    /**
     * Returns the configured CompressionCodec, or null if none is configured.
     *
     * @param context
     *            the ProcessContext
     * @param configuration
     *            the Hadoop Configuration
     * @return CompressionCodec or null
     */
    protected org.apache.hadoop.io.compress.CompressionCodec getCompressionCodec(ProcessContext context, Configuration configuration) {
        org.apache.hadoop.io.compress.CompressionCodec codec = null;
        if (context.getProperty(COMPRESSION_CODEC).isSet()) {
            String compressionClassname = CompressionType.valueOf(context.getProperty(COMPRESSION_CODEC).getValue()).toString();
            CompressionCodecFactory ccf = new CompressionCodecFactory(configuration);
            codec = ccf.getCodecByClassName(compressionClassname);
        }

        return codec;
    }

    /**
     * Returns the relative path of the child that does not include the filename or the root path.
     *
     * @param root
     *            the path to relativize from
     * @param child
     *            the path to relativize
     * @return the relative path
     */
    public static String getPathDifference(final Path root, final Path child) {
        final int depthDiff = child.depth() - root.depth();
        if (depthDiff <= 1) {
            return "".intern();
        }
        String lastRoot = root.getName();
        Path childsParent = child.getParent();
        final StringBuilder builder = new StringBuilder();
        builder.append(childsParent.getName());
        for (int i = (depthDiff - 3); i >= 0; i--) {
            childsParent = childsParent.getParent();
            String name = childsParent.getName();
            if (name.equals(lastRoot) && childsParent.toString().endsWith(root.toString())) {
                break;
            }
            builder.insert(0, Path.SEPARATOR).insert(0, name);
        }
        return builder.toString();
    }

    protected Configuration getConfiguration() {
        return hdfsResources.get().getKey();
    }

    protected FileSystem getFileSystem() {
        return hdfsResources.get().getValue();
    }
}

package com.zegelin.cassandra.exporter;

import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.zegelin.cassandra.exporter.collector.InternalGossiperMBeanMetricFamilyCollector;
import com.zegelin.jmx.DelegatingMBeanServerInterceptor;
import com.zegelin.cassandra.exporter.cli.HarvesterOptions;
import com.zegelin.jmx.NamedObject;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.lang.management.PlatformManagedObject;

class MBeanServerInterceptorHarvester extends Harvester {
    class MBeanServerInterceptor extends DelegatingMBeanServerInterceptor {
        MBeanServerInterceptor(final MBeanServer delegate) {
            super(delegate);
        }

        @Override
        public ObjectInstance registerMBean(final Object object, final ObjectName name) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
            // delegate first so that any exceptions (such as InstanceAlreadyExistsException) will throw first before additional processing occurs.
            final ObjectInstance objectInstance = super.registerMBean(object, name);

            final String interfaceClassName;
            try {
                interfaceClassName = (String) this.getMBeanInfo(objectInstance.getObjectName()).getDescriptor().getFieldValue(JMX.INTERFACE_CLASS_NAME_FIELD);

            } catch (final InstanceNotFoundException | IntrospectionException | ReflectionException e) {
                throw new MBeanRegistrationException(e);
            }

            MBeanServerInterceptorHarvester.this.registerMBean(new NamedObject<>(objectInstance.getObjectName(), object, interfaceClassName));

            return objectInstance;
        }

        @Override
        public void unregisterMBean(final ObjectName mBeanName) throws InstanceNotFoundException, MBeanRegistrationException {
            try {
                MBeanServerInterceptorHarvester.this.unregisterMBean(mBeanName);

            } finally {
                super.unregisterMBean(mBeanName);
            }
        }
    }

    MBeanServerInterceptorHarvester(final HarvesterOptions options) {
        this(new InternalMetadataFactory(), options);
    }

    private MBeanServerInterceptorHarvester(final MetadataFactory metadataFactory, final HarvesterOptions options) {
        super(metadataFactory, options);

        registerPlatformMXBeans();

        installMBeanServerInterceptor();

        addCollectorFactory(InternalGossiperMBeanMetricFamilyCollector.factory(metadataFactory));
    }


    private void registerPlatformMXBeans() {
        // the platform MXBeans get registered right at JVM startup, before the agent gets a chance to
        // install the interceptor.
        // instead, directly register the MXBeans here...

        for (final Class<? extends PlatformManagedObject> iface: ManagementFactory.getPlatformManagementInterfaces()) {
            for (final PlatformManagedObject platformMXBean : ManagementFactory.getPlatformMXBeans(iface)) {
                registerMBean(new NamedObject<>(platformMXBean.getObjectName(), platformMXBean, iface.getName()));
            }
        }
    }

    private void installMBeanServerInterceptor() {
        final JmxMBeanServer mBeanServer = (JmxMBeanServer) ManagementFactory.getPlatformMBeanServer();

        final MBeanServerInterceptor interceptor = new MBeanServerInterceptor(mBeanServer.getMBeanServerInterceptor());

        mBeanServer.setMBeanServerInterceptor(interceptor);
    }
}

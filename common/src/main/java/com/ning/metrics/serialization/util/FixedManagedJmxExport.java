/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.util;

import org.apache.log4j.Logger;

import javax.management.Descriptor;
import javax.management.IntrospectionException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanConstructorInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanInfoSupport;
import javax.management.modelmbean.ModelMBeanNotificationInfo;
import javax.management.modelmbean.ModelMBeanOperationInfo;
import javax.management.modelmbean.RequiredModelMBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expose all @Managed classes to JMX
 */
public class FixedManagedJmxExport
{
    private static final Logger log = Logger.getLogger(FixedManagedJmxExport.class);
    private static final Pattern getterOrSetterPattern = Pattern.compile("(get|set|is)(.)(.*)");
    private static final MBeanServer server = ManagementFactory.getPlatformMBeanServer();

    @SuppressWarnings("unused")
    public static void export(final String name, final Object monitoredObject)
    {
        try {
            final ObjectName objectName = new ObjectName(name);

            final RequiredModelMBean mbean = getMBean(monitoredObject);
            mbean.setManagedResource(monitoredObject, "objectReference");

            // register the model MBean in the MBean server
            server.registerMBean(mbean, objectName);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RequiredModelMBean getMBean(final Object monitoredObject)
    {
        try {
            final ModelMBeanInfo info = buildInfo(monitoredObject.getClass());

            return new RequiredModelMBean(new ModelMBeanInfoSupport(info));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean includeMethod(final Method method)
    {
        log.debug(String.format("Include method[%s]? %s", method.getName(), method.getAnnotation(Managed.class) != null));
        return method.getAnnotation(Managed.class) != null;
    }

    private static String getDescription(final Method method)
    {
        final Managed annotation = method.getAnnotation(Managed.class);
        if (annotation == null) {
            throw new RuntimeException("FixedManagedJmxExportScope is trying to export a method without a managed annotation.  Method name: " + method.getName());
        }

        log.debug(String.format("Method[%s] given description[%s].", method.getName(), annotation.description()));
        return annotation.description();
    }

    private static ModelMBeanInfo buildInfo(Class<?> clazz)
        throws IntrospectionException
    {
        final List<OperationDescriptor> operations = new ArrayList<OperationDescriptor>();
        final Map<String, AttributeDescriptor> attributeDescriptors = new HashMap<String, AttributeDescriptor>();

        final String className = clazz.getName();

        while (clazz != null) {
            for (final Method method : clazz.getMethods()) {
                if (!includeMethod(method)) {
                    continue;
                }

                final String name = method.getName();
                final String description = getDescription(method);

                final Matcher matcher = getterOrSetterPattern.matcher(name);
                OperationDescriptor operation = null;

                if (matcher.matches()) {
                    final String type = matcher.group(1);
                    final String first = matcher.group(2);
                    final String rest = matcher.group(3);

                    final String attributeName = first + (rest != null ? rest : "");

                    AttributeDescriptor descriptor = attributeDescriptors.get(attributeName);
                    if (descriptor == null) {
                        descriptor = new AttributeDescriptor(attributeName, description);
                    }

                    if ((type.equals("get") || type.equals("is")) && method.getParameterTypes().length == 0) {
                        descriptor.setGetter(method);
                        operation = new OperationDescriptor(method, "getter");
                        attributeDescriptors.put(attributeName, descriptor);
                    }
                    else if (type.equals("set") && method.getParameterTypes().length == 1) {
                        descriptor.setSetter(method);
                        operation = new OperationDescriptor(method, "setter");
                        attributeDescriptors.put(attributeName, descriptor);
                    }
                }

                if (operation == null) {
                    operation = new OperationDescriptor(method, "operation");
                }

                operations.add(operation);
            }

            clazz = clazz.getSuperclass();
        }


        final List<ModelMBeanAttributeInfo> attributeInfos = new ArrayList<ModelMBeanAttributeInfo>();

        for (final AttributeDescriptor attribute : attributeDescriptors.values()) {
            final Descriptor descriptor = new DescriptorSupport();
            descriptor.setField("name", attribute.getName());
            descriptor.setField("descriptorType", "attribute");
            if (attribute.getGetter() != null) {
                descriptor.setField("getMethod", attribute.getGetter().getName());
            }
            if (attribute.getSetter() != null) {
                descriptor.setField("setMethod", attribute.getSetter().getName());
            }

            attributeInfos.add(new ModelMBeanAttributeInfo(attribute.getName(),
                attribute.getDescription(),
                attribute.getGetter(),
                attribute.getSetter(),
                descriptor));
        }

        final List<ModelMBeanOperationInfo> operationInfos = new ArrayList<ModelMBeanOperationInfo>();

        for (final OperationDescriptor operation : operations) {
            final Descriptor descriptor = new DescriptorSupport();
            descriptor.setField("name", operation.getMethod().getName());
            descriptor.setField("class", className);
            descriptor.setField("descriptorType", "operation");
            descriptor.setField("role", operation.getRole());

            operationInfos.add(new ModelMBeanOperationInfo("",
                operation.getMethod(),
                descriptor));

            // TODO: discover parameter names
            // see http://jroller.com/eu/entry/using_asm_to_read_mathod
        }

        return new ModelMBeanInfoSupport(className, null,
            attributeInfos.toArray(new ModelMBeanAttributeInfo[attributeInfos.size()]),
            new ModelMBeanConstructorInfo[0],
            operationInfos.toArray(new ModelMBeanOperationInfo[attributeInfos.size()]),
            new ModelMBeanNotificationInfo[0]);
    }

    private static class OperationDescriptor
    {
        private final Method method;
        private final String role;

        public OperationDescriptor(final Method method, final String role)
        {
            this.method = method;
            this.role = role;
        }

        public Method getMethod()
        {
            return method;
        }

        public String getRole()
        {
            return role;
        }
    }

    private static class AttributeDescriptor
    {
        private Method getter = null;
        private Method setter = null;
        private final String description;
        private final String name;

        private AttributeDescriptor(final String name, final String description)
        {
            this.name = name;
            this.description = description;
        }

        public String getName()
        {
            return name;
        }

        public Method getGetter()
        {
            return getter;
        }

        public void setGetter(final Method getter)
        {
            this.getter = getter;
        }

        public Method getSetter()
        {
            return setter;
        }

        public void setSetter(final Method setter)
        {
            this.setter = setter;
        }

        public String getDescription()
        {
            return description;
        }
    }
}

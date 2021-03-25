/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.unomi.plugins.baseplugin.actions;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.unomi.api.Event;
import org.apache.unomi.api.PropertyType;
import org.apache.unomi.api.actions.Action;
import org.apache.unomi.api.actions.ActionExecutor;
import org.apache.unomi.api.services.EventService;
import org.apache.unomi.api.services.ProfileService;
import org.apache.unomi.persistence.spi.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CopyPropertiesAction implements ActionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CopyPropertiesAction.class);
    private ProfileService profileService;

    public void setProfileService(ProfileService profileService) {
        this.profileService = profileService;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public int execute(Action action, Event event) {
        boolean changed = false;
        List<String> mandatoryPropTypeSystemTags = (List<String>) action.getParameterValues().get("mandatoryPropTypeSystemTag");
        String singleValueStrategy = (String) action.getParameterValues().get("singleValueStrategy");
        for (Map.Entry<String, Object> entry : getEventPropsToCopy(action, event).entrySet()) {
            // propType Check
            PropertyType propertyType = profileService.getPropertyType(entry.getKey());
            Object previousValue = event.getProfile().getProperty(entry.getKey());
            if (mandatoryPropTypeSystemTags != null && mandatoryPropTypeSystemTags.size() > 0) {
                if (propertyType == null || propertyType.getMetadata() == null || propertyType.getMetadata().getSystemTags() == null
                        || !propertyType.getMetadata().getSystemTags().containsAll(mandatoryPropTypeSystemTags)) {
                    continue;
                }
            }
            String propertyName = "properties." + entry.getKey();

            if (previousValue == null && propertyType == null) {
                changed = changed || PropertyHelper.setProperty(event.getProfile(), propertyName, entry.getValue(), "alwaysSet");
            } else if (previousValue != null) {
                if (previousValue instanceof List) {
                    changed = changed || PropertyHelper.setProperty(event.getProfile(), propertyName, entry.getValue(), "addValues");
                } else if (entry.getValue() instanceof List) {
                    logger.error("A single property named {} is already set on the profile. Impossible to replace with a list",
                            entry.getKey());
                } else {
                    changed =
                            changed || PropertyHelper.setProperty(event.getProfile(), propertyName, entry.getValue(), singleValueStrategy);
                }
            } else {
                if (propertyType.isMultivalued()) {
                    changed = changed || PropertyHelper.setProperty(event.getProfile(), propertyName, entry.getValue(), "addValues");
                } else if (entry.getValue() instanceof List) {
                    logger.error("The property {} should contains a single value as declared in the property types", entry.getKey());
                } else {
                    changed =
                            changed || PropertyHelper.setProperty(event.getProfile(), propertyName, entry.getValue(), singleValueStrategy);
                }
            }
        }
        return changed ? EventService.PROFILE_UPDATED : EventService.NO_CHANGE;
    }

    private Map<String, Object> getEventPropsToCopy(Action action, Event event) {
        Map<String, Object> propsToCopy = new HashMap<String, Object>();

        String rootProperty = (String) action.getParameterValues().get("rootProperty");
        boolean copyEventProps = false;

        if (StringUtils.isEmpty(rootProperty)) {
            copyEventProps = true;
            rootProperty = "target.properties";
        }

        // copy props from the event.properties
        if (copyEventProps && event.getProperties() != null) {
            propsToCopy.putAll(event.getProperties());
        }

        // copy props from the specified level (default is: target.properties)
        try {
            Object targetProperties = BeanUtilsBean.getInstance().getPropertyUtils().getProperty(event, rootProperty);
            if (targetProperties instanceof Map) {
                propsToCopy.putAll((Map) targetProperties);
            }
        } catch (Exception e) {
            // Ignore
        }

        return propsToCopy;
    }
}

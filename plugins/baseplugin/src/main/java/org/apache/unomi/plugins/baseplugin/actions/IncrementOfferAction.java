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

import org.apache.unomi.api.CustomItem;
import org.apache.unomi.api.Event;
import org.apache.unomi.api.Profile;
import org.apache.unomi.api.actions.Action;
import org.apache.unomi.api.actions.ActionExecutor;
import org.apache.unomi.api.services.EventService;
import org.apache.unomi.persistence.spi.PropertyHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementOfferAction implements ActionExecutor {

    public int execute(Action action, Event event) {
        if (event.getEventType().equals("offer")) {
            Profile profile = event.getProfile();
            CustomItem offer = (CustomItem) event.getTarget();
            List<Map<String, Object>> offers = profile.getProperties().containsKey("offers") ?
                    (List<Map<String, Object>>) profile.getProperties().get("offers") :
                    new ArrayList<>();

            // check existing offer
            for (Map<String, Object> offerEntry : offers) {
                if (offerEntry.get("offerId").equals(offer.getItemId())) {
                    offerEntry.put("offerScore", ((Integer) offerEntry.get("offerScore")) + ((Integer) offer.getProperties().get("weight")));
                    PropertyHelper.setProperty(event.getProfile(), "properties.offers", offers, null);
                    return EventService.PROFILE_UPDATED;
                }
            }

            // new offer
            Map<String, Object> newOffer = new HashMap<>();
            newOffer.put("offerId", offer.getItemId());
            newOffer.put("offerScore", offer.getProperties().get("weight"));
            offers.add(newOffer);
            PropertyHelper.setProperty(event.getProfile(), "properties.offers", offers, null);
            return EventService.PROFILE_UPDATED;
        }
        return EventService.NO_CHANGE;
    }
}
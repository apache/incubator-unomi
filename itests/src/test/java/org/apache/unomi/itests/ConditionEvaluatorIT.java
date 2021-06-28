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
 * limitations under the License
 */

package org.apache.unomi.itests;

import org.apache.unomi.api.Item;
import org.apache.unomi.api.Profile;
import org.apache.unomi.api.conditions.Condition;
import org.apache.unomi.api.services.DefinitionsService;
import org.apache.unomi.persistence.spi.PersistenceService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;
import org.ops4j.pax.exam.util.Filter;

import javax.inject.Inject;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Integration tests for various condition types.
 *
 * @author Sergiy Shyrkov
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class ConditionEvaluatorIT extends BaseIT {
    protected ConditionBuilder builder;
    protected Item item;
    protected Date lastVisit;

    @Inject @Filter(timeout = 600000)
    protected PersistenceService persistenceService;
    @Inject @Filter(timeout = 600000)
    private DefinitionsService definitionsService;

    protected boolean eval(Condition c) {
        return persistenceService.testMatch(c, item);
    }

    @Before
    public void setUp() {
        assertNotNull("Definition service should be available", definitionsService);
        assertNotNull("Persistence service should be available", persistenceService);
        builder = new ConditionBuilder(definitionsService);

        lastVisit = new GregorianCalendar(2015, Calendar.FEBRUARY,1,20,30,0).getTime();

        Profile profile = new Profile("profile-" + UUID.randomUUID().toString());
        profile.setProperty("firstVisit", lastVisit);
        profile.setProperty("age", Integer.valueOf(30));
        profile.setProperty("gender", "female");
        profile.setProperty("lastVisit", lastVisit);
        profile.setProperty("randomStats", 0.15);
        profile.setSegments(new HashSet<>(Arrays.asList("s1", "s2", "s3")));
        item = profile;
    }

    @Test
    public void testCompound() {
        // test AND
        assertTrue(eval(builder.and(builder.profileProperty("properties.gender").equalTo("female"),
                builder.profileProperty("properties.age").equalTo(30)).build()));
        assertFalse(eval(builder.and(builder.profileProperty("properties.gender").equalTo("male"),
                builder.profileProperty("properties.age").equalTo(30)).build()));
        assertFalse(eval(builder.and(builder.profileProperty("properties.gender").equalTo("female"),
                builder.profileProperty("properties.age").equalTo(40)).build()));

        // test OR
        assertTrue(eval(builder.or(builder.profileProperty("properties.gender").equalTo("female"),
                builder.profileProperty("properties.age").equalTo(40)).build()));
        assertTrue(eval(builder.or(builder.profileProperty("properties.gender").equalTo("male"),
                builder.profileProperty("properties.age").equalTo(30)).build()));
        assertFalse(eval(builder.or(builder.profileProperty("properties.gender").equalTo("male"),
                builder.profileProperty("properties.age").equalTo(40)).build()));

        // test NOT
        assertTrue(eval(builder.not(builder.profileProperty("properties.gender").equalTo("male")).build()));
        assertFalse(eval(builder.not(builder.profileProperty("properties.age").equalTo(30)).build()));
    }

    @Test
    public void testDate() {
        assertTrue(eval(builder.profileProperty("properties.lastVisit").equalTo(lastVisit).build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit")
                .greaterThan(new Date(lastVisit.getTime() - 10000)).build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit").lessThan(new Date(lastVisit.getTime() + 10000))
                .build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit")
                .in(new Date(lastVisit.getTime() + 10000), new Date(lastVisit.getTime() - 10000), lastVisit).build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit")
                .notIn(new Date(lastVisit.getTime() + 10000), new Date(lastVisit.getTime() - 10000)).build()));
        assertFalse(eval(builder.profileProperty("properties.lastVisit")
                .notIn(new Date(lastVisit.getTime() + 10000), new Date(lastVisit.getTime() - 10000), lastVisit).build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit").all(lastVisit).build()));
        assertFalse(eval(builder.profileProperty("properties.lastVisit")
                .all(new Date(lastVisit.getTime() + 10000), lastVisit).build()));

        assertTrue(eval(builder.profileProperty("properties.lastVisit").isDay(lastVisit).build()));
        assertTrue(eval(builder.profileProperty("properties.lastVisit").isNotDay(new Date(lastVisit.getTime() + (24*60*60*1000))).build()));
    }

    @Test
    public void testExistence() {
        assertTrue("Gender property does not exist", eval(builder.profileProperty("properties.gender").exists().build()));
        assertFalse("Gender property missing", eval(builder.profileProperty("properties.gender").missing().build()));
        assertTrue("Strange property exists", eval(builder.profileProperty("properties.unknown").missing().build()));
        assertFalse("Strange property exists", eval(builder.profileProperty("properties.unknown").exists().build()));
    }

    @Test
    public void testInteger() {
        assertTrue(eval(builder.profileProperty("properties.age").equalTo(30).build()));
        assertTrue(eval(builder.not(builder.profileProperty("properties.age").equalTo(40)).build()));
        assertTrue(eval(builder.profileProperty("properties.age").notEqualTo(40).build()));
        assertTrue(eval(builder.profileProperty("properties.age").lessThan(40).build()));
        assertTrue(eval(builder.profileProperty("properties.age").greaterThan(20).build()));
        assertTrue(eval(builder.profileProperty("properties.age").greaterThanOrEqualTo(30).build()));
        assertFalse(eval(builder.profileProperty("properties.age").greaterThanOrEqualTo(31).build()));

        assertTrue(eval(builder.profileProperty("properties.age").in(30).build()));
        assertTrue(eval(builder.profileProperty("properties.age").in(31, 30).build()));
        assertTrue(eval(builder.profileProperty("properties.age").notIn(25, 26).build()));
        assertFalse(eval(builder.profileProperty("properties.age").notIn(25, 30).build()));

        assertTrue(eval(builder.profileProperty("properties.fieldNotExists").notIn(25, 30).build()));
        assertTrue(eval(builder.profileProperty("properties.fieldNotExists").notEqualTo(1).build()));
    }

    @Test
    public void testDouble() {
        ConditionBuilder.PropertyCondition doubleProperty = builder.profileProperty("properties.randomStats");

        assertTrue(eval(doubleProperty.equalTo(0.15).build()));
        assertTrue(eval(builder.not(doubleProperty.equalTo(2.5)).build()));
        assertTrue(eval(doubleProperty.notEqualTo(2.5).build()));
        assertTrue(eval(doubleProperty.greaterThan(0.13).build()));
        assertTrue(eval(doubleProperty.lessThan(0.17).build()));
        assertTrue(eval(doubleProperty.greaterThanOrEqualTo(0.15).build()));
        assertTrue(eval(doubleProperty.in(0.15).build()));
        assertTrue(eval(doubleProperty.in(0.18, 0.15).build()));
        assertTrue(eval(doubleProperty.notIn(2.8, 1.6).build()));
    }

    @Test
    public void testMultiValue() {
        assertTrue(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "in")
                .parameter("segments", "s10", "s20", "s2").build()));
        assertFalse(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "in")
                .parameter("segments", "s10", "s20", "s30").build()));
        assertTrue(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "notIn")
                .parameter("segments", "s10", "s20", "s30").build()));
        assertFalse(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "notIn")
                .parameter("segments", "s10", "s20", "s2").build()));
        assertTrue(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "all")
                .parameter("segments", "s1", "s2").build()));
        assertFalse(eval(builder.property("profileSegmentCondition", "segments").parameter("matchType", "all")
                .parameter("segments", "s1", "s5").build()));
    }

    @Test
    public void testString() {
        assertTrue(eval(builder.profileProperty("properties.gender").equalTo("female").build()));
        assertFalse(eval(builder.not(builder.profileProperty("properties.gender").equalTo("female")).build()));
        assertTrue(eval(builder.profileProperty("properties.gender").notEqualTo("male").build()));
        assertFalse(eval(builder.not(builder.profileProperty("properties.gender").notEqualTo("male")).build()));
        assertTrue(eval(builder.profileProperty("properties.gender").startsWith("fe").build()));
        assertTrue(eval(builder.profileProperty("properties.gender").endsWith("le").build()));
        assertTrue(eval(builder.profileProperty("properties.gender").contains("fem").build()));
        assertFalse(eval(builder.profileProperty("properties.gender").contains("mu").build()));
        assertTrue(eval(builder.profileProperty("properties.gender").matchesRegex(".*ale").build()));

        assertTrue(eval(builder.profileProperty("properties.gender").in("male", "female").build()));
        assertTrue(eval(builder.profileProperty("properties.gender").notIn("one", "two").build()));
        assertFalse(eval(builder.profileProperty("properties.gender").notIn("one", "two", "female").build()));
        assertTrue(eval(builder.profileProperty("properties.gender").all("female").build()));
        assertFalse(eval(builder.profileProperty("properties.gender").all("male", "female").build()));
    }

    @Test
    public void testRange() {
        // test AND
        Condition condition = builder.and(
                builder.profileProperty("properties.gender").equalTo("female"),
                builder.profileProperty("properties.age").greaterThanOrEqualTo(40)
        ).build();
        assertFalse(eval(condition));

        // test OR
        condition = builder.or(
                builder.profileProperty("properties.gender").equalTo("female"),
                builder.profileProperty("properties.age").greaterThanOrEqualTo(40)
        ).build();
        assertTrue(eval(condition));
    }
}

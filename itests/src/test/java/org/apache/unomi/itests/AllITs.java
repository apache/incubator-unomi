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

import org.apache.unomi.itests.graphql.*;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Defines suite of test classes to run.
 *
 * @author Sergiy Shyrkov
 */
@RunWith(Suite.class)
@SuiteClasses({
        BasicIT.class,
        ConditionEvaluatorIT.class,
        ConditionESQueryBuilderIT.class,
        SegmentIT.class,
        ProfileServiceIT.class,
        ProfileImportBasicIT.class,
        ProfileImportSurfersIT.class,
        ProfileImportRankingIT.class,
        ProfileImportActorsIT.class,
        ProfileExportIT.class,
        ProfileMergeIT.class,
        EventServiceIT.class,
        PropertiesUpdateActionIT.class,
        CopyPropertiesActionIT.class,
        IncrementPropertyIT.class,
        IncrementInterestsIT.class,
        InputValidationIT.class,
        ModifyConsentIT.class,
        PatchIT.class,
        ContextServletIT.class,
        SecurityIT.class,
        RuleServiceIT.class,
        GraphQLEventIT.class,
        GraphQLListIT.class,
        GraphQLProfileIT.class,
        GraphQLProfilePropertiesIT.class,
        GraphQLSegmentIT.class,
        GraphQLWebSocketIT.class
})
public class AllITs {
}

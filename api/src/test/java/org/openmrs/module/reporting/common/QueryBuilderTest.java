/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.openmrs.module.reporting.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openmrs.Encounter;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.service.CriteriaQuery;
import org.openmrs.module.reporting.evaluation.service.EvaluationService;
import org.openmrs.module.reporting.query.encounter.EncounterIdSet;
import org.openmrs.module.reporting.query.encounter.definition.BasicEncounterQuery;
import org.openmrs.module.reporting.query.encounter.service.EncounterQueryService;
import org.openmrs.test.BaseModuleContextSensitiveTest;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Testing the QueryBuilder
 */
public class QueryBuilderTest extends BaseModuleContextSensitiveTest {

	@Autowired
	EvaluationService evaluationService;

	protected Log log = LogFactory.getLog(this.getClass());

	@Test
	@Ignore
	public void benchmarkTableJoinNoHibernateWithHqlBuilder() throws Exception {
		BasicEncounterQuery q = new BasicEncounterQuery();
		//q.addEncounterType(Context.getEncounterService().getEncounterType("ART_INITIAL"));
		EncounterIdSet encounterIdSet = Context.getService(EncounterQueryService.class).evaluate(q, new EvaluationContext());

		String[] properties = {"encounterId","encounterDatetime","encounterType"};

		Timer timer = Timer.start();

		Context.getService(EvaluationService.class).persistIdSet(encounterIdSet.getMemberIds());

		for (String property : properties) {
			CriteriaQuery query = new CriteriaQuery(Encounter.class);
			query.addColumnsToReturn(property);
			query.whereEqual("encounterId", encounterIdSet);
			List<Map<String, Object>> result = query.execute();
			System.out.println("Retrieved " + result.size() + " " + property + "s");
		}

		Context.getService(EvaluationService.class).deleteIdSet(encounterIdSet.getMemberIds().hashCode());
	}

	protected void outputResults(List<Map<String, Object>> results) {
		for (Map<String, Object> row : results) {
			System.out.println(ObjectUtil.toString(row, ": ", ", "));
		}
	}

	@Override
	public Boolean useInMemoryDatabase() {
		return false;
	}

	@Before
	public void setup() throws Exception {
		authenticate();
	}

	@Override
	public Properties getRuntimeProperties() {
		Properties p = super.getRuntimeProperties();
		//p.setProperty("hibernate.show_sql", "true");
		p.setProperty("connection.url", "jdbc:mysql://localhost:3306/openmrs_neno?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
		return p;
	}
}
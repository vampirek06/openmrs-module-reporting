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
package org.openmrs.module.reporting.evaluation.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.reporting.common.Timer;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.context.EncounterEvaluationContext;
import org.openmrs.module.reporting.evaluation.querybuilder.QueryBuilder;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the EvaluationService interface
 */
public class EvaluationServiceImpl extends BaseOpenmrsService implements EvaluationService {

	private transient Log log = LogFactory.getLog(this.getClass());
	private AdministrationService administrationService;
	private List<Integer> currentIdSetKeys = Collections.synchronizedList(new ArrayList<Integer>());

	@Override
	public DatabaseQueryResult evaluate(QueryBuilder queryBuilder) {
		DatabaseQueryResult result = new DatabaseQueryResult();
		result.setColumns(queryBuilder.getColumns());
		SessionFactory sessionFactory = Context.getRegisteredComponents(SessionFactory.class).get(0);
		Timer timer = Timer.start();
		List l = queryBuilder.buildQuery(sessionFactory).list();
		log.debug(timer.logInterval("Primary query executed"));
		for (Object resultRow : l) {
			result.addResult(resultRow);
		}
		return result;
	}

	/**
	 * @see EvaluationService#generateKey(Set)
	 */
	@Override
	public Integer generateKey(Set<Integer> ids) {
		return ids.hashCode();
	}

	/**
	 * @see EvaluationService#startUsing(Set)
	 */
	@Transactional
	public Integer startUsing(Set<Integer> ids) {
		if (ids == null || ids.isEmpty()) {
			return null;
		}
		Integer idSetKey = generateKey(ids);
		if (isInUse(idSetKey)) {
			log.debug("Attempting to persist an IdSet that has previously been persisted.  Using existing values.");
			// TODO: As an additional check here, we could confirm that they are the same by loading into memory
		}
		else {
			StringBuilder q = new StringBuilder();
			q.append("insert into reporting_idset (idset_key, member_id) values ");
			for (Iterator<Integer> i = ids.iterator(); i.hasNext(); ) {
				Integer id = i.next();
				q.append("(").append(idSetKey).append(",").append(id).append(")").append(i.hasNext() ? "," : "");
			}
			administrationService.executeSQL(q.toString(), false);  // TODO: Use a SqlQuery implementation to save this
			log.debug("Persisted idset: " + idSetKey + "; size: " + ids.size() + "; total active: " + currentIdSetKeys.size());
		}
		currentIdSetKeys.add(idSetKey);
		return idSetKey;
	}

	/**
	 * @see EvaluationService#startUsing(EvaluationContext)
	 */
	@Override
	public List<Integer> startUsing(EvaluationContext context) {
		List<Integer> idSetsAdded = new ArrayList<Integer>();
		if (context.getBaseCohort() != null) {
			Set<Integer> ids = context.getBaseCohort().getMemberIds();
			if (!ids.isEmpty()) {
				idSetsAdded.add(startUsing(ids));
			}
		}
		if (context instanceof EncounterEvaluationContext) {
			EncounterEvaluationContext eec = (EncounterEvaluationContext)context;
			if (eec.getBaseEncounters() != null) {
				Set<Integer> ids = eec.getBaseEncounters().getMemberIds();
				if (!ids.isEmpty()) {
					idSetsAdded.add(startUsing(ids));
				}
			}
		}
		return idSetsAdded;
	}

	/**
	 * @see EvaluationService#isInUse(Integer)
	 */
	@Override
	public boolean isInUse(Integer idSetKey) {
		String existQuery = "select count(*) from reporting_idset where idset_key = "+idSetKey+"";
		String check = administrationService.executeSQL(existQuery, true).get(0).get(0).toString();
		return !check.equals("0");
	}

	/**
	 * @see EvaluationService#stopUsing(Integer)
	 */
	@Transactional
	public void stopUsing(Integer idSetKey) {
		int indexToRemove = currentIdSetKeys.lastIndexOf(idSetKey);
		currentIdSetKeys.remove(indexToRemove);
		if (!currentIdSetKeys.contains(idSetKey)) {
			String q = "delete from reporting_idset where idset_key = " + idSetKey + "";
			administrationService.executeSQL(q, false);  // TODO: Use a SqlQuery implementation to save this
			currentIdSetKeys.remove(idSetKey);
			log.debug("Deleted idset: " + idSetKey + "; total active: " + currentIdSetKeys.size());
		}
	}

	//***** PROPERTY ACCESS *****

	public AdministrationService getAdministrationService() {
		return administrationService;
	}

	public void setAdministrationService(AdministrationService administrationService) {
		this.administrationService = administrationService;
	}
}
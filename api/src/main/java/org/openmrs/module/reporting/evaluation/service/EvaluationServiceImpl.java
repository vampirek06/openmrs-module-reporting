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
import org.openmrs.api.AdministrationService;
import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.context.EncounterEvaluationContext;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the EvaluationService interface
 */
public class EvaluationServiceImpl extends BaseOpenmrsService implements EvaluationService {

	private transient Log log = LogFactory.getLog(this.getClass());
	private AdministrationService administrationService;
	private Set<Integer> currentIdSetKeys = Collections.synchronizedSet(new HashSet<Integer>());

	/**
	 * @see EvaluationService#executeQuery(DatabaseQuery)
	 */
	@Transactional
	public DatabaseQueryResult executeQuery(DatabaseQuery query) {
		return query.execute();
	}

	/**
	 * @see EvaluationService#retrieveIdSetKey(Set)
	 */
	@Override
	public Integer retrieveIdSetKey(Set<Integer> ids) {
		return ids.hashCode();
	}

	/**
	 * @see EvaluationService#isIdSetPersisted(Set)
	 */
	@Override
	public boolean isIdSetPersisted(Set<Integer> ids) {
		Integer idSetKey = retrieveIdSetKey(ids);
		String existQuery = "select count(*) from reporting_idset where idset_key = "+idSetKey+"";
		String check = administrationService.executeSQL(existQuery, true).get(0).get(0).toString();
		return !check.equals("0");
	}

	/**
	 * @see EvaluationService#persistIdSet(Set)
	 */
	@Transactional
	public boolean persistIdSet(Set<Integer> ids) {
		Integer idSetKey = retrieveIdSetKey(ids);
		if (isIdSetPersisted(ids)) {
			log.warn("Attempting to persist an IdSet that has previously been persisted.  Using existing values.");
			// TODO: As an additional check here, we could confirm that they are the same by loading into memory
			return false;
		}

		StringBuilder q = new StringBuilder();
		q.append("insert into reporting_idset (idset_key, member_id) values");
		for (Iterator<Integer> i = ids.iterator(); i.hasNext(); ) {
			Integer id = i.next();
			q.append("(").append(idSetKey).append(",").append(id).append(")").append(i.hasNext() ? "," : "");
		}
		administrationService.executeSQL(q.toString(), false);  // TODO: Use a SqlQuery implementation to save this
		currentIdSetKeys.add(idSetKey);
		log.warn("Persisted idset: " + idSetKey + "; size: " + ids.size() + "; total active: " + currentIdSetKeys.size());
		return true;
	}

	/**
	 * @see EvaluationService#persistIdSetsForContext(EvaluationContext)
	 */
	@Override
	public List<Integer> persistIdSetsForContext(EvaluationContext context) {
		List<Integer> idSetsOwned = new ArrayList<Integer>();
		if (context.getBaseCohort() != null) {
			Set<Integer> ids = context.getBaseCohort().getMemberIds();
			boolean newlyPersisted = persistIdSet(ids);
			if (newlyPersisted) {
				idSetsOwned.add(retrieveIdSetKey(ids));
			}
		}
		if (context instanceof EncounterEvaluationContext) {
			EncounterEvaluationContext eec = (EncounterEvaluationContext)context;
			if (eec.getBaseEncounters() != null) {
				Set<Integer> ids = eec.getBaseEncounters().getMemberIds();
				boolean newlyPersisted = persistIdSet(ids);
				if (newlyPersisted) {
					idSetsOwned.add(retrieveIdSetKey(ids));
				}
			}
		}
		return idSetsOwned;
	}

	/**
	 * @see EvaluationService#deleteIdSet(Integer)
	 */
	@Transactional
	public void deleteIdSet(Integer idSetKey) {
		String q = "delete from reporting_idset where idset_key = " + idSetKey + "";
		administrationService.executeSQL(q, false);  // TODO: Use a SqlQuery implementation to save this
		currentIdSetKeys.remove(idSetKey);
		log.warn("Deleted idset: " + idSetKey + "; total active: " + currentIdSetKeys.size());
	}

	@Override
	public void deleteIdSets(List<Integer> idSetKeyList) {
		for (Integer idSetKey : idSetKeyList) {
			deleteIdSet(idSetKey);
		}
	}

	/**
	 * @see EvaluationService#getCurrentIdSetKeys()
	 */
	@Transactional
	public Set<Integer> getCurrentIdSetKeys() {
		return currentIdSetKeys;
	}

	//***** PROPERTY ACCESS *****

	public AdministrationService getAdministrationService() {
		return administrationService;
	}

	public void setAdministrationService(AdministrationService administrationService) {
		this.administrationService = administrationService;
	}
}
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

import org.openmrs.api.OpenmrsService;
import org.openmrs.module.reporting.dataset.DataSetRowList;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataSetEvaluation DAO Queries
 */
public interface EvaluationService extends OpenmrsService {

	/**
	 * @return the results of evaluating the passed DatabaseQuery
	 */
	@Transactional
	public DatabaseQueryResult executeQuery(DatabaseQuery query);

	/**
	 * Get the key that can be used to uniquely reference this id set in temporary storage
	 */
	@Transactional
	public Integer retrieveIdSetKey(Set<Integer> ids);

	/**
	 * Returns true of an IdSet with the passed idSetKey is already persisted to temporary storage
	 */
	@Transactional
	public boolean isIdSetPersisted(Set<Integer> ids);

	/**
	 * Save the passed ids to temporary storage so queries can join against the members
	 * Returns false if the ids were already persisted and were not re-saved
	 */
	@Transactional
	public boolean persistIdSet(Set<Integer> ids);

	/**
	 * Persists IdSets in temporary storage for later querying as appropriate for the given EvaluationContext
	 * @return the Integer idSetKeys for each IdSet that was newly registered, implying these are the IdSets
	 * that the caller owns and can delete when finished
	 */
	public List<Integer> persistIdSetsForContext(EvaluationContext context);

	/**
	 * Remove the passed idSet from temporary storage
	 */
	@Transactional
	public void deleteIdSet(Integer idSetKey);

	/**
	 * Remove the passed idSets from temporary storage
	 */
	@Transactional
	public void deleteIdSets(List<Integer> idSetKey);

	/**
	 * @see EvaluationService#getCurrentIdSetKeys()
	 */
	@Transactional
	public Set<Integer> getCurrentIdSetKeys();
}
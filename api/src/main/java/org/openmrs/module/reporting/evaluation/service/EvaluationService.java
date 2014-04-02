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
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.querybuilder.QueryBuilder;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;

/**
 * DataSetEvaluation DAO Queries
 */
public interface EvaluationService extends OpenmrsService {

	/**
	 * Perform a full evaluation and return the results as a DatabaseQueryResult
	 */
	@Transactional
	public DatabaseQueryResult evaluate(QueryBuilder queryBuilder);

	/**
	 * Get the key that can be used to uniquely reference this id set in temporary storage
	 */
	@Transactional
	public Integer generateKey(Set<Integer> ids);

	/**
	 * Indicate that you require joining against a particular set of ids, and that they
	 * should be made available to your calling code until you call the stopUsing method
	 * Returns the key that can be used to reference this id set at a later point in time
	 */
	@Transactional
	public Integer startUsing(Set<Integer> ids);

	/**
	 * Indicate that you require using the different base id sets contained in the passed EvaluationContext
	 */
	public List<Integer> startUsing(EvaluationContext context);

	/**
	 * Returns true of an IdSet with the passed idSetKey is already persisted to temporary storage
	 */
	@Transactional
	public boolean isInUse(Integer idSetKey);

	/**
	 * Remove the passed idSet from temporary storage
	 */
	@Transactional
	public void stopUsing(Integer idSetKey);
}
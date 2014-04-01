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

import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.DataSetRowList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DatabaseQueryResult {

	//***** PROPERTIES *****

	private List<DataSetColumn> columns = new ArrayList<DataSetColumn>();
	private List<Object[]> results = new ArrayList<Object[]>();

	//***** CONSTRUCTORS *****

	public DatabaseQueryResult() {}

	//***** INSTANCE METHODS *****

	/**
	 * @return the number of result rows
	 */
	public int getNumRows() {
		return getResults().size();
	}

	/**
	 * @return the results of the query as a DataSetRowList
	 */
	public DataSetRowList asRowList() {
		DataSetRowList l = new DataSetRowList();
		for (Object[] o : getResults()) {
			DataSetRow row = new DataSetRow();
			for (int i = 0; i < getColumns().size(); i++) {
				row.addColumnValue(getColumns().get(i), o[i]);
			}
			l.add(row);
		}
		return null;
	}

	/**
	 * @return the results of the query as a List of column values
	 */
	public <T> List<T> asValueList(String columnName, Class<T> elementType) {
		List<T> l = new ArrayList<T>();

		int columnIndex = -1;
		for (int i=0; i<getColumns().size(); i++) {
			if (getColumns().get(i).getName().equals(columnName)) {
				columnIndex = i;
			}
		}

		if (columnIndex == -1) {
			throw new IllegalArgumentException("No column named " + columnName + " found in result set");
		}

		for (Object[] result : getResults()) {
			l.add((T)result[columnIndex]);
		}

		return l;
	}

	/**
	 * @return the results of the query as a Map of column values, using the first two defined columns
	 * If less than 2 columns are defined, an Exception is thrown
	 */
	public <T, V> Map<T, V> asMap(Class<T> keyType, Class<V> valueType) {
		Map<T, V> ret = new LinkedHashMap<T, V>();
		for (Object[] row : getResults()) {
			ret.put((T)row[0], (V)row[1]);
		}
		return ret;
	}

	//***** PROPERTY ACCESS *****

	public List<DataSetColumn> getColumns() {
		if (columns == null) {
			columns = new ArrayList<DataSetColumn>();
		}
		return columns;
	}

	public void setColumns(List<DataSetColumn> columns) {
		this.columns = columns;
	}

	public List<Object[]> getResults() {
		if (results == null) {
			results = new ArrayList<Object[]>();
		}
		return results;
	}

	public void addResult(Object result) {
		if (result instanceof Object[]) {
			getResults().add((Object[]) result);
		}
		else {
			getResults().add(new Object[]{result});
		}
	}

	public void setResults(List<Object[]> results) {
		this.results = results;
	}
}

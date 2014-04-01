package org.openmrs.module.reporting.evaluation.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.openmrs.Cohort;
import org.openmrs.OpenmrsObject;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.reporting.common.Timer;
import org.openmrs.module.reporting.dataset.DataSetColumn;
import org.openmrs.module.reporting.query.IdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Helper class for building and executing an HQL query with parameters
 */
public class HqlQuery implements DatabaseQuery {

	protected Log log = LogFactory.getLog(getClass());

	private Class<? extends OpenmrsObject> rootType;
	private String rootAlias;
	private List<DataSetColumn> registeredColumns = new ArrayList<DataSetColumn>();
	private List<String> clauses = new ArrayList<String>();
	private List<Object> parameters = new ArrayList<Object>();
	private List<Object> orderBy = new ArrayList<Object>();
	private List<Set<Integer>> idSetsRegistered = new ArrayList<Set<Integer>>();

	//***** CONSTRUCTORS *****

	public HqlQuery() { }

	//***** BUILDER METHODS *****

	/**
	 * Adds a new clause to the query
	 */
	public HqlQuery select(String...columns) {
		for (String column : columns) {
			registeredColumns.add(new DataSetColumn(column, column, Object.class)); // TODO: Figure out data type, alias
		}
		return this;
	}

	public HqlQuery from(Class<? extends OpenmrsObject> rootType, String rootAlias) {
		this.rootType = rootType;
		this.rootAlias = rootAlias;
		return this;
	}

	/**
	 * Adds a new clause to the query
	 */
	public HqlQuery where(String constraint) {
		clauses.add(constraint);
		return this;
	}

	/**
	 * Adds a parameter for the clause
	 */
	public HqlQuery withValue(Object parameterValue) {
		parameters.add(parameterValue);
		return this;
	}

	/**
	 * Restricts the query for where the value of the passed property name is null
	 */
	public HqlQuery whereNull(String propertyName) {
		where(propertyName = " is null");
		return this;
	}

	/**
	 * Restricts the query for where the value of the passed property equals the passed value
	 */
	public HqlQuery whereEqual(String propertyName, Object propertyValue) {
		if (propertyValue != null) {
			if (propertyValue instanceof Date) {
				Date d = (Date) propertyValue;
				Date startOfDay = DateUtil.getStartOfDay(d);
				if (d.equals(startOfDay)) {
					whereGreaterOrEqualTo(propertyName, startOfDay);
					whereLess(propertyName, DateUtil.getEndOfDay(d));
				}
				else {
					where(propertyName + " = ?").withValue(d);
				}
			}
			else {
				if (propertyValue instanceof Cohort) {
					Cohort c = (Cohort) propertyValue;
					whereIdIn(propertyName, c.getMemberIds());
				}
				else if (propertyValue instanceof IdSet) {
					IdSet idSet = (IdSet) propertyValue;
					whereIdIn(propertyName, idSet.getMemberIds());
				}
				else if (propertyValue instanceof Object[]) {
					where(propertyName + " in (?)").withValue(propertyValue);
				}
				else if (propertyValue instanceof Collection) {
					where(propertyName + " in (?)").withValue(propertyValue);
				}
				else {
					where(propertyName + " = ?").withValue(propertyValue);
				}
			}
		}
		return this;
	}

	public HqlQuery whereIdIn(String propertyName, Set<Integer> ids) {
		Integer idSetKey = Context.getService(EvaluationService.class).retrieveIdSetKey(ids);
		where(propertyName + " in ( select memberId from IdsetMember where key = ? )").withValue(idSetKey);
		idSetsRegistered.add(ids);
		return this;
	}

	public HqlQuery whereLike(String propertyName, Object propertyValue) {
		where(propertyName + " like ?").withValue(propertyValue);
		return this;
	}

	public HqlQuery whereGreater(String propertyName, Object propertyValue) {
		where(propertyName + " > ?").withValue(propertyValue);
		return this;
	}

	public HqlQuery whereGreaterOrEqualTo(String propertyName, Object propertyValue) {
		where(propertyName + " >= ?").withValue(propertyValue);
		return this;
	}

	public HqlQuery whereLess(String propertyName, Object propertyValue) {
		if (propertyValue instanceof Date) {
			propertyValue = DateUtil.getEndOfDayIfTimeExcluded((Date)propertyValue);
		}
		where(propertyName + " < ?").withValue(propertyValue);
		return this;
	}

	public HqlQuery whereLessOrEqualTo(String propertyName, Object propertyValue) {
		if (propertyValue instanceof Date) {
			propertyValue = DateUtil.getEndOfDayIfTimeExcluded((Date)propertyValue);
		}
		where(propertyName + " <= ?").withValue(propertyValue);
		return this;
	}

	public HqlQuery whereBetweenInclusive(String propertyName, Object minValue, Object maxValue) {
		if (minValue != null) {
			whereGreaterOrEqualTo(propertyName, minValue);
		}
		if (maxValue != null) {
			whereLessOrEqualTo(propertyName, maxValue);
		}
		return this;
	}

	public HqlQuery orderAsc(String propertyName) {
		orderBy.add(propertyName + " asc");
		return this;
	}

	public HqlQuery orderDesc(String propertyName) {
		orderBy.add(propertyName + " desc");
		return this;
	}

	public String getQueryString() {
		StringBuilder ret = new StringBuilder();
		for (DataSetColumn c : registeredColumns) {
			ret.append(ret.length() == 0 ? "select " : ", ").append(c.getName());
		}
		ret.append(" from ").append(rootType.getSimpleName());
		if (rootAlias != null) {
			ret.append(" as ").append(rootAlias);
		}
		for (int i=0; i<clauses.size(); i++) {
			ret.append(i == 0 ? " where " : " and ").append(clauses.get(i));
		}
		for (int i=0; i<orderBy.size(); i++) {
			ret.append(i == 0 ? " order by " : ", ").append(orderBy.get(i));
		}
		return ret.toString();
	}

	//***** EXECUTION METHODS *****

	/**
	 * @see org.openmrs.module.reporting.evaluation.service.DatabaseQuery#execute()
	 */
	public DatabaseQueryResult execute() {
		DatabaseQueryResult result = new DatabaseQueryResult();
		result.setColumns(registeredColumns);

		log.debug("Query Execution Start.");
		Timer timer = Timer.start();
		List<Integer> idSetsOwned = new ArrayList<Integer>();
		try {
			log.debug("Found " + idSetsRegistered + " configured for this query.");
			for (Set<Integer> ids : idSetsRegistered) {
				boolean isAlreadyPersisted = Context.getService(EvaluationService.class).isIdSetPersisted(ids);
				if (!isAlreadyPersisted) {
					log.debug("Need to persist a new IdSet for joining");
					Context.getService(EvaluationService.class).persistIdSet(ids);
					idSetsOwned.add(Context.getService(EvaluationService.class).retrieveIdSetKey(ids));
				}
				else {
					log.debug("IdSet for joining already persisted");
				}
			}

			String hqlQuery = getQueryString();
			log.debug("Executing query: " + hqlQuery);
			log.debug("With parameters: " + parameters);
			SessionFactory sessionFactory = Context.getRegisteredComponents(SessionFactory.class).get(0);
			Query q = sessionFactory.getCurrentSession().createQuery(hqlQuery);
			for (int i=0; i<parameters.size(); i++) {
				q.setParameter(i, parameters.get(i));
			}

			List<?> l = q.list();
			for (Object resultRow : l) {
				result.addResult(resultRow);
			}
			log.debug(timer.logInterval("Primary query executed"));
			return result;
		}
		finally {
			Context.getService(EvaluationService.class).deleteIdSets(idSetsOwned);
		}
	}
}

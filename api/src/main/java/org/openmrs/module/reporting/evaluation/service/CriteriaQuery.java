package org.openmrs.module.reporting.evaluation.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.openmrs.Cohort;
import org.openmrs.OpenmrsObject;
import org.openmrs.api.context.Context;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.reporting.common.HibernateUtil;
import org.openmrs.module.reporting.common.ObjectUtil;
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
public class CriteriaQuery implements DatabaseQuery {

	protected Log log = LogFactory.getLog(getClass());

	private ClassMetadata classMetadata;
	private Criteria criteria;
	private List<DataSetColumn> registeredColumns = new ArrayList<DataSetColumn>();
	private ProjectionList projectionList;
	private List<Set<Integer>> idSetsRegistered = new ArrayList<Set<Integer>>();

    public CriteriaQuery(Class<? extends OpenmrsObject> typeToQuery) {
		this(typeToQuery, null);
	}

	public CriteriaQuery(Class<? extends OpenmrsObject> typeToQuery, String alias) {
		SessionFactory sessionFactory = Context.getRegisteredComponents(SessionFactory.class).get(0);
		classMetadata = sessionFactory.getClassMetadata(typeToQuery);
		if (ObjectUtil.isNull(alias)) {
			criteria = sessionFactory.getCurrentSession().createCriteria(typeToQuery);
		}
		else {
			criteria = sessionFactory.getCurrentSession().createCriteria(typeToQuery, alias);
		}
		projectionList = Projections.projectionList();
		criteria.setProjection(projectionList);
	}

	//***** BUILDER METHODS *****

	/**
	 * In order to reference any nested properties of the primary typeToQuery referenced in the Constructor,
	 * you first need to explicitly join against them, providing an optional alias.  You must do this
	 * before you reference these nested properties in any columns, constraints, or orderings
	 */
	public CriteriaQuery addInnerJoin(String propertyName, String alias) {
		criteria.createAlias(propertyName, alias);
		return this;
	}

	/**
	 * Similar to addInnerJoin, but uses a left outer join instead
	 */
	public CriteriaQuery addLeftOuterJoin(String propertyName, String alias) {
		criteria.createAlias(propertyName, alias, Criteria.LEFT_JOIN);
		return this;
	}

	/**
	 * This is where you specify what columns you want your query to return
	 * You can specify the column in property dot notation relative to the typeToQuery specified in the Constructor
	 * You can also specify a column alias by appending it to the column name after a colon
	 * For example, if you wanted to return the birthdate column from person with alias "bd",
	 * you would add "birthdate:bd".  If you wanted to add the name of the encounter type associated with an
	 * Encounter, with alias "type", you would add "encounterType.name:type"
	 */
	public CriteriaQuery addColumnsToReturn(String... columns) {
		for (String column : columns) {
			String[] split = column.split("\\:");
			Projection p = Projections.property(split[0]);
			if (split.length > 1) {
				projectionList.add(p, split[1]);
				registeredColumns.add(new DataSetColumn(split[1], split[1], Object.class));
			}
			else {
				projectionList.add(p);
				registeredColumns.add(new DataSetColumn(split[0], split[0], Object.class));
			}
		}
		return this;
	}

	public CriteriaQuery addCriteria(Criterion criterion) {
		criteria.add(criterion);
		return this;
	}

	public CriteriaQuery whereEqual(String propertyName, Object propertyValue) {
		if (propertyValue == null) {
			addCriteria(Restrictions.isNull(propertyName));
		}
		else if (propertyValue instanceof Date) {
			Date d = (Date)propertyValue;
			Date endOfDateIfTimeExcluded = DateUtil.getEndOfDayIfTimeExcluded(d);
			if (d.equals(endOfDateIfTimeExcluded)) {
				addCriteria(Restrictions.eq(propertyName, d));
			}
			else {
				whereBetweenInclusive(propertyName, d, endOfDateIfTimeExcluded);
			}
		}
		else {
			if (propertyValue instanceof Cohort) {
				Cohort c = (Cohort)propertyValue;
				whereIdIn(propertyName, c.getMemberIds());
			}
			else if (propertyValue instanceof IdSet) {
				IdSet idSet = (IdSet)propertyValue;
				whereIdIn(propertyName, idSet.getMemberIds());
			}
			else if (propertyValue instanceof Object[]) {
				addCriteria(Restrictions.in(propertyName, (Object[]) propertyValue));
			}
			else if (propertyValue instanceof Collection) {
				addCriteria(Restrictions.in(propertyName, (Collection)propertyValue));
			}
			else {
				addCriteria(Restrictions.eq(propertyName, propertyValue));
			}
		}
		return this;
	}

	public CriteriaQuery whereIdIn(String propertyName, Set<Integer> ids) {
		// Get the appropriate sql column name for the passed in property name
		AbstractEntityPersister p = (AbstractEntityPersister) classMetadata;
		String columnName = p.getPropertyColumnNames(propertyName)[0];

		// Build sql criteria against this table
		Integer idSetKey = Context.getService(EvaluationService.class).retrieveIdSetKey(ids);
		String sql = "{alias}." + columnName + " in (select member_id from reporting_idset where idset_key = ?)";
		addCriteria(Restrictions.sqlRestriction(sql, idSetKey, HibernateUtil.standardType("INTEGER")));

		idSetsRegistered.add(ids);

		return this;
	}

	public CriteriaQuery whereLike(String propertyName, Object propertyValue) {
		addCriteria(Restrictions.like(propertyName, propertyValue));
		return this;
	}

	public CriteriaQuery whereGreater(String propertyName, Object propertyValue) {
		addCriteria(Restrictions.gt(propertyName, propertyValue));
		return this;
	}

	public CriteriaQuery whereGreaterOrEqualTo(String propertyName, Object propertyValue) {
		addCriteria(Restrictions.ge(propertyName, propertyValue));
		return this;
	}

	public CriteriaQuery whereLess(String propertyName, Object propertyValue) {
		if (propertyValue instanceof Date) {
			propertyValue = DateUtil.getEndOfDayIfTimeExcluded((Date)propertyValue);
		}
		addCriteria(Restrictions.lt(propertyName, propertyValue));
		return this;
	}

	public CriteriaQuery whereLessOrEqualTo(String propertyName, Object propertyValue) {
		if (propertyValue instanceof Date) {
			propertyValue = DateUtil.getEndOfDayIfTimeExcluded((Date)propertyValue);
		}
		addCriteria(Restrictions.le(propertyName, propertyValue));
		return this;
	}

	public CriteriaQuery whereBetweenInclusive(String propertyName, Object minValue, Object maxValue) {
		if (minValue != null) {
			whereGreaterOrEqualTo(propertyName, minValue);
		}
		if (maxValue != null) {
			whereLessOrEqualTo(propertyName, maxValue);
		}
		return this;
	}

	public CriteriaQuery orderBy(Order order) {
		criteria.addOrder(order);
		return this;
	}

	//***** EXECUTION METHODS *****

	/**
	 * @see DatabaseQuery#execute()
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
			List<?> l = criteria.list();
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

	//***** PROPERTY ACCESS *****


	public Criteria getCriteria() {
		return criteria;
	}

	public ProjectionList getProjectionList() {
		return projectionList;
	}

	public ClassMetadata getClassMetadata() {
		return classMetadata;
	}
}

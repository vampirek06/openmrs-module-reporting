package org.openmrs.module.reporting.web.cohorts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.Cohort;
import org.openmrs.api.context.Context;
import org.openmrs.module.htmlwidgets.web.WidgetUtil;
import org.openmrs.module.reporting.cohort.definition.CohortDefinition;
import org.openmrs.module.reporting.cohort.definition.service.CohortDefinitionService;
import org.openmrs.module.reporting.common.ObjectUtil;
import org.openmrs.module.reporting.common.ReflectionUtil;
import org.openmrs.module.reporting.definition.DefinitionUtil;
import org.openmrs.module.reporting.definition.configuration.Property;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.validator.CohortDefinitionValidator;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.SessionAttributes;

@Controller
@SessionAttributes({"cohortDefinition", "configurationProperties", "groupedProperties"})
public class ManageCohortDefinitionsController {
	
	protected static Log log = LogFactory.getLog(ManageCohortDefinitionsController.class);
	
    /**
     * Basically acts as the formBackingObject() method for saving a 
     * cohort definition.
     * 
     * @param uuid
     * @param type
     * @param returnUrl
     * @param model
     * @return
     */
    @RequestMapping("/module/reporting/cohorts/editCohortDefinition")
    public String editCohortDefinition(
    		@RequestParam(required=false, value="uuid") String uuid,
            @RequestParam(required=false, value="type") Class<? extends CohortDefinition> type,
    		ModelMap model) {
    	
    	CohortDefinitionService service = Context.getService(CohortDefinitionService.class);
    	CohortDefinition cd = service.getDefinition(uuid, type);
     	model.addAttribute("cohortDefinition", cd);

     	List<Property> properties = DefinitionUtil.getConfigurationProperties(cd);
     	model.addAttribute("configurationProperties", properties);
     	Map<String, List<Property>> groups = new LinkedHashMap<String, List<Property>>();
     	for (Property p : properties) {
     		List<Property> l = groups.get(p.getGroup());
     		if (l == null) {
     			l = new ArrayList<Property>();
     			groups.put(p.getGroup(), l);
     		}
     		l.add(p);
     	}
     	model.addAttribute("groupedProperties", groups);
	
        return "/module/reporting/cohorts/cohortDefinitionEditor";
    }
    
    /**
     * Saves a cohort definition.
     * 
     * @param uuid
     * @param type
     * @param name
     * @param description
     * @param model
     * @return
     */
    @RequestMapping("/module/reporting/cohorts/saveCohortDefinition")
    @SuppressWarnings("unchecked")
    public String saveCohortDefinition(
    		@RequestParam(required=false, value="uuid") String uuid,
            @RequestParam(required=false, value="type") Class<? extends CohortDefinition> type,
            @RequestParam(required=true, value="name") String name,
            @RequestParam(required=false, value="description") String description,
            HttpServletRequest request,
            @ModelAttribute("cohortDefinition") CohortDefinition cohortDefinition,
            BindingResult bindingResult,
    		ModelMap model
    ) {
    	
    	cohortDefinition.setName(name);
    	cohortDefinition.setDescription(description);
    	cohortDefinition.getParameters().clear();
    	
    	for (Property p : DefinitionUtil.getConfigurationProperties(cohortDefinition)) {
    		String fieldName = p.getField().getName();
    		String prefix = "parameter." + fieldName;
    		String valParamName =  prefix + ".value"; 
    		boolean isParameter = "t".equals(request.getParameter(prefix+".allowAtEvaluation"));
    		
    		Object valToSet = WidgetUtil.getFromRequest(request, valParamName, p.getField());
    		
    		Class<? extends Collection<?>> collectionType = null;
    		Class<?> fieldType = p.getField().getType();   		
			if (ReflectionUtil.isCollection(p.getField())) {
				collectionType = (Class<? extends Collection<?>>)p.getField().getType();
				fieldType = (Class<?>)ReflectionUtil.getGenericTypes(p.getField())[0];
			}
			
			if (isParameter) {
				ReflectionUtil.setPropertyValue(cohortDefinition, p.getField(), null);
				String paramLabel = ObjectUtil.nvlStr(request.getParameter(prefix + ".label"), fieldName);
				Parameter param = new Parameter(fieldName, paramLabel, fieldType, collectionType, valToSet);
				cohortDefinition.addParameter(param);
			}
			else {
				ReflectionUtil.setPropertyValue(cohortDefinition, p.getField(), valToSet);
			}
    	}
    	
    	new CohortDefinitionValidator().validate(cohortDefinition, bindingResult);
    	if(bindingResult.hasErrors())
    		return "/module/reporting/cohorts/cohortDefinitionEditor";
    	
    	if("".equals(cohortDefinition.getUuid()))
    		cohortDefinition.setUuid(null);
    	
    	log.warn("Saving: " + cohortDefinition);
    	Context.getService(CohortDefinitionService.class).saveDefinition(cohortDefinition);

        return "redirect:/module/reporting/definition/manageDefinitions.form?type="+CohortDefinition.class.getName();
    }

    
    /**
     * Evaluates a cohort definition given a uuid.
     * 
     * @param uuid
     * @param type
     * @param returnUrl
     * @param model
     * @return
     * @throws EvaluationException 
     */
    @RequestMapping("/module/reporting/cohorts/evaluateCohortDefinition")
    public String evaluateCohortDefinition(
    		@RequestParam(required=false, value="uuid") String uuid,
            @RequestParam(required=false, value="type") Class<? extends CohortDefinition> type,
    		ModelMap model) throws EvaluationException {
    	
    	CohortDefinitionService service = Context.getService(CohortDefinitionService.class);
    	CohortDefinition cohortDefinition = service.getDefinition(uuid, type);
     	
    	// Evaluate the cohort definition
    	EvaluationContext context = new EvaluationContext();
    	Cohort cohort = service.evaluate(cohortDefinition, context);
    	
    	// create the model and view to return
     	model.addAttribute("cohort", cohort);
     	model.addAttribute("cohortDefinition", cohortDefinition);
     	
        return "/module/reporting/cohorts/cohortDefinitionEvaluator";
    }
}

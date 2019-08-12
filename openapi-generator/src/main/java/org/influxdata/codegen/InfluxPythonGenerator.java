package org.influxdata.codegen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.swagger.v3.oas.models.media.Schema;
import org.openapitools.codegen.CodegenModel;
import org.openapitools.codegen.languages.PythonClientCodegen;

import static org.openapitools.codegen.utils.StringUtils.camelize;
import static org.openapitools.codegen.utils.StringUtils.underscore;

public class InfluxPythonGenerator extends PythonClientCodegen {

    public InfluxPythonGenerator() {
        apiPackage = "service";
        modelPackage = "domain";
    }

    /**
     * Configures a friendly name for the generator.  This will be used by the generator
     * to select the library with the -g flag.
     *
     * @return the friendly name for the generator
     */
    public String getName() {
        return "influx-python";
    }

    /**
     * Returns human-friendly help for the generator.  Provide the consumer with help
     * tips, parameters here
     *
     * @return A string value for the help message
     */
    public String getHelp() {
        return "Generates a influx-python client library.";
    }

    @Override
    public void processOpts() {

        super.processOpts();

        List<String> useless = Arrays.asList(
                ".gitignore", ".travis.yml", "README.md", "setup.py", "requirements.txt", "test-requirements.txt",
                "git_push.sh");

        //
        // Remove useless supports file
        //
        supportingFiles = supportingFiles.stream()
                .filter(supportingFile -> !useless.contains(supportingFile.destinationFilename))
                .collect(Collectors.toList());
    }

    @Override
    public CodegenModel fromModel(final String name, final Schema model, final Map<String, Schema> allDefinitions) {
        CodegenModel codegenModel = super.fromModel(name, model, allDefinitions);

        if (name.endsWith("ViewProperties") && !name.equals("ViewProperties"))
        {
            codegenModel.setParent("ViewProperties");
            codegenModel.setParentSchema("ViewProperties");
        }

        if (allDefinitions.containsKey(name + "Base")) {
            codegenModel.setParent(name + "Base");
            codegenModel.setParentSchema(name + "Base");
        }

        if (name.equals("ViewProperties"))  {
            codegenModel.setReadWriteVars(new ArrayList<>());
            codegenModel.setRequiredVars(new ArrayList<>());
            codegenModel.hasOnlyReadOnly = true;
            codegenModel.hasRequired = false;
        }

        return codegenModel;
    }

    @Override
    public Map<String, Object> postProcessAllModels(final Map<String, Object> models) {

        Map<String, Object> allModels = super.postProcessAllModels(models);

        for (Map.Entry<String, Object> entry : allModels.entrySet()) {

            String modelName = entry.getKey();
            Object modelConfig = entry.getValue();

            CodegenModel model = getModel((HashMap) modelConfig);

            if (model.getParent() != null) {
                CodegenModel parentModel = getModel((HashMap) allModels.get(model.getParent()));
                model.vendorExtensions.put("x-parent-classFilename", parentModel.getClassFilename());
                model.vendorExtensions.put("x-has-parent-vars", !parentModel.getVars().isEmpty());
                model.vendorExtensions.put("x-parent-vars", parentModel.getVars());
            }
        }

        return allModels;
    }

    @Override
    public String toApiName(String name) {
        if (name.length() == 0) {
            return "DefaultService";
        }
        // e.g. phone_number_service => PhoneNumberService
        return camelize(name) + "Service";
    }

    @Override
    public String toApiVarName(String name) {

        if (name.length() == 0) {
            return "default_service";
        }
        return underscore(name) + "_service";
    }

    @Override
    public String toApiFilename(String name) {
        // replace - with _ e.g. created-at => created_at
        name = name.replaceAll("-", "_");

        // e.g. PhoneNumberService.py => phone_number_service.py
        return underscore(name) + "_service";
    }

    @Nonnull
    private CodegenModel getModel(@Nonnull final HashMap modelConfig) {

        HashMap models = (HashMap) ((ArrayList) modelConfig.get("models")).get(0);

        return (CodegenModel) models.get("model");
    }
}

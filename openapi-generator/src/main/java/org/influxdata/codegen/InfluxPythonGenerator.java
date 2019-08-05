package org.influxdata.codegen;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
}

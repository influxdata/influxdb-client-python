"""
How to use Templates and Stack API.
"""
import datetime

from influxdb_client import InfluxDBClient, TemplatesService, TemplateApply, TemplateApplyRemotes, PatchStackRequest, \
    TemplateApplyTemplate

"""
Define credentials
"""
url = 'http://localhost:8086'
token = 'my-token'
bucket_name = 'my-bucket'
org_name = 'my-org'

with InfluxDBClient(url=url, token=token, org=org_name, debug=True) as client:
    uniqueId = str(datetime.datetime.now())
    """
    Find Organization ID by Organization API.
    """
    org = client.organizations_api().find_organizations(org=org_name)[0]

    """
    Initialize Template service
    """
    templates_service = TemplatesService(api_client=client.api_client)

    """
    Apply 'Linux System Monitoring Template'
    """
    template_yaml_url = "https://raw.githubusercontent.com/influxdata/community-templates/master/linux_system/linux_system.yml"  # noqa: E501
    template_linux = templates_service.apply_template(
        template_apply=TemplateApply(dry_run=False,
                                     org_id=org.id,
                                     remotes=[TemplateApplyRemotes(url=template_yaml_url)]))
    """
    Set Stack name
    """
    templates_service.update_stack(stack_id=template_linux.stack_id,
                                   patch_stack_request=PatchStackRequest(name="linux_system"))

    """
    Create template as a inline definition
    """
    template_definition = {
        "apiVersion": "influxdata.com/v2alpha1",
        "kind": "Bucket",
        "metadata": {"name": "template-bucket"},
        "spec": {"description": "bucket 1 description"}
    }
    template_inline = templates_service.apply_template(
        template_apply=TemplateApply(dry_run=False,
                                     org_id=org.id,
                                     template=TemplateApplyTemplate(content_type="json",
                                                                    contents=[template_definition])))
    """
    Set Stack name
    """
    templates_service.update_stack(stack_id=template_inline.stack_id,
                                   patch_stack_request=PatchStackRequest(name="inline_stack"))

    """
    List installed stacks
    """
    print(f"\n------- List -------\n")
    stacks = templates_service.list_stacks(org_id=org.id).stacks
    print("\n".join([f" ---\n ID: {it.id}\n Stack: {it}" for it in stacks]))
    print("---")

    """
    Delete previously created Script
    """
    print(f"------- Delete -------\n")
    templates_service.delete_stack(stack_id=template_linux.stack_id, org_id=org.id)
    print(f" Successfully deleted script: '{template_linux.stack_id}'")

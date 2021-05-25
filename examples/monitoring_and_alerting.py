"""
How to create a check with Slack notification.
"""
import datetime

from influxdb_client.service.notification_rules_service import NotificationRulesService

from influxdb_client.domain.rule_status_level import RuleStatusLevel

from influxdb_client.domain.status_rule import StatusRule

from influxdb_client.domain.slack_notification_rule import SlackNotificationRule

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.domain.check_status_level import CheckStatusLevel
from influxdb_client.domain.dashboard_query import DashboardQuery
from influxdb_client.domain.lesser_threshold import LesserThreshold
from influxdb_client.domain.query_edit_mode import QueryEditMode
from influxdb_client.domain.slack_notification_endpoint import SlackNotificationEndpoint
from influxdb_client.domain.task_status_type import TaskStatusType
from influxdb_client.domain.threshold_check import ThresholdCheck
from influxdb_client.service.checks_service import ChecksService
from influxdb_client.service.notification_endpoints_service import NotificationEndpointsService

"""
Define credentials
"""
url = "http://localhost:8086"
token = "my-token"
org_name = "my-org"
bucket_name = "my-bucket"


with InfluxDBClient(url=url, token=token, org=org_name, debug=False) as client:
    uniqueId = str(datetime.datetime.now())

    """
    Find Organization ID by Organization API.
    """
    org = client.organizations_api().find_organizations(org=org_name)[0]

    """
    Prepare data
    """
    client.write_api(write_options=SYNCHRONOUS).write(record="mem,production=true free=40", bucket=bucket_name)

    """
    Create Threshold Check - set status to `Critical` if the current value is lesser than `35`.
    """
    threshold = LesserThreshold(value=35.0,
                                level=CheckStatusLevel.CRIT)
    query = f'''
            from(bucket:"{bucket_name}") 
                |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
                |> filter(fn: (r) => r["_measurement"] == "mem")
                |> filter(fn: (r) => r["_field"] == "free")
                |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)        
                |> yield(name: "mean")
            '''

    check = ThresholdCheck(name=f"Check created by Remote API_{uniqueId}",
                           status_message_template="The value is on: ${ r._level } level!",
                           every="5s",
                           offset="0s",
                           query=DashboardQuery(edit_mode=QueryEditMode.ADVANCED, text=query),
                           thresholds=[threshold],
                           org_id=org.id,
                           status=TaskStatusType.ACTIVE)

    checks_service = ChecksService(api_client=client.api_client)
    checks_service.create_check(check)

    """
    Create Slack Notification endpoint
    """
    notification_endpoint = SlackNotificationEndpoint(name=f"Slack Dev Channel_{uniqueId}",
                                                      url="https://hooks.slack.com/services/x/y/z",
                                                      org_id=org.id)
    notification_endpoint_service = NotificationEndpointsService(api_client=client.api_client)
    notification_endpoint = notification_endpoint_service.create_notification_endpoint(notification_endpoint)

    """
    Create Notification Rule to notify critical value to Slack Channel
    """
    notification_rule = SlackNotificationRule(name=f"Critical status to Slack_{uniqueId}",
                                              every="10s",
                                              offset="0s",
                                              message_template="${ r._message }",
                                              status_rules=[StatusRule(current_level=RuleStatusLevel.CRIT)],
                                              tag_rules=[],
                                              endpoint_id=notification_endpoint.id,
                                              org_id=org.id,
                                              status=TaskStatusType.ACTIVE)

    notification_rules_service = NotificationRulesService(api_client=client.api_client)
    notification_rules_service.create_notification_rule(notification_rule)

    """
    List all Checks
    """
    print(f"\n------- Checks: -------\n")
    checks = checks_service.get_checks(org_id=org.id).checks
    print("\n".join([f" ---\n ID: {it.id}\n Name: {it.name}\n Type: {type(it)}" for it in checks]))
    print("---")

    """
    List all Endpoints
    """
    print(f"\n------- Notification Endpoints: -------\n")
    notification_endpoints = notification_endpoint_service.get_notification_endpoints(org_id=org.id).notification_endpoints
    print("\n".join([f" ---\n ID: {it.id}\n Name: {it.name}\n Type: {type(it)}" for it in notification_endpoints]))
    print("---")

    """
    List all Notification Rules
    """
    print(f"\n------- Notification Rules: -------\n")
    notification_rules = notification_rules_service.get_notification_rules(org_id=org.id).notification_rules
    print("\n".join([f" ---\n ID: {it.id}\n Name: {it.name}\n Type: {type(it)}" for it in notification_rules]))
    print("---")

from datetime import date, datetime
from collections import defaultdict
import boto3
import botocore
import csv
import os

## Parameters to run the script, their parameters either hard-code or put in environment variable ##
document_name = "gethostnamelinux"
landing_zone = "aws_lz212_ssm_history"
env = "non-prod"
year = date.today().year
report_name = landing_zone + "-" + env + "-" + str(year) + ".csv"
local_file_path = "/tmp/" + report_name
bucket_name = "ses-mail1"

## initial connection ##

ssm = boto3.client("ssm")

paginator_ssm = ssm.get_paginator("list_commands")

marker = None
pages = paginator_ssm.paginate(
    PaginationConfig={"PageSize": 5, "StartingToken": marker}
)


def download_from_s3(file_name):
    s3 = boto3.resource("s3")
    try:
        s3.Object(bucket_name, file_name).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return 1
    else:
        s3 = boto3.client("s3")
        s3.download_file(bucket_name, file_name, local_file_path)
        return 0


def upload_file_to_bucket(bucket_name, file_path):
    try:
        s3_resource = boto3.resource(service_name="s3")
        file_dir, file_name = os.path.split(file_path)

        bucket = s3_resource.Bucket(bucket_name)
        bucket.upload_file(
            Filename=file_path, Key=file_name, ExtraArgs={"ACL": "public-read"}
        )

        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    except Exception as e:
        return e
    return s3_url


def write_result(list_commands):
    field_names = [
        "id",
        "document_name",
        "execution_date",
        "status",
        "instances status",
    ]

    total = len(list_commands)

    current_month = date.today().month
    report_existed = download_from_s3(report_name)

    mode = "a" if report_existed == 0 and current_month != 1 else "w"

    before_append_lines = 0
    if mode is "a":
        with open(local_file_path) as f:
            before_append_lines = sum(1 for line in f)

    with open(report_name, mode, encoding="UTF8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        if current_month == 1 or report_existed == 1:
            writer.writeheader()
        for k, v in list_commands.items():
            target_states = [i for i in v["TargetStatus"]]
            format_target_states = ",".join(target_states)
            print(format_target_states)
            writer.writerow(
                {
                    "CommandId": k,
                    "DocumentName": v["DocumentName"],
                    "RunTime": v["RunTime"],
                    "Status": v["Status"],
                    "TargetsStatus": format_target_states,
                }
            )

    with open(local_file_path) as f:
        after_append_lines = sum(1 for line in f)
    return (
        upload_file_to_bucket(bucket_name=bucket_name, file_path=local_file_path),
        total,
        report_existed,
        mode,
        before_append_lines,
        after_append_lines,
    )


def target_status(instances, cmd_id):
    ids_states = []
    for id in instances:
        state_response = ssm.list_commands(CommandId=cmd_id, InstanceId=id)
        id_state = id + ":" + state_response["Commands"][0]["Status"]
        ids_states.append(id_state)
    return ids_states


def lambda_handler(event, context):

    ssm_history = defaultdict()

    for page in pages:
        list_ex_cmds = page["Commands"]
        # filter the list_execute_commands with the document name only
        filter_list_cmds = list(
            filter(
                lambda list_ex_cmds: list_ex_cmds["DocumentName"] in document_name,
                list_ex_cmds,
            )
        )
        if len(filter_list_cmds) <= 0:
            continue
        for exe_cmd in filter_list_cmds:
            ssm_history[exe_cmd["CommandId"]] = {
                "CommandId": exe_cmd["CommandId"],
                "DocumentName": exe_cmd["DocumentName"],
                "RunTime": exe_cmd["RequestedDateTime"].strftime("%d/%b/%Y %H:%M:%S"),
                "Status": exe_cmd["Status"],
                "TargetStatus": target_status(
                    exe_cmd["Targets"][0]["Values"], exe_cmd["CommandId"]
                ),
            }

    return len(ssm_history), write_result(ssm_history)

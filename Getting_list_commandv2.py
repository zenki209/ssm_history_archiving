from datetime import date, datetime
from unittest import skip
import boto3
import botocore
import json
import csv
import os

## Parameters to run the script, their parameters either hard-code or put in environment variable ##
session = boto3.Session(profile_name="default")
document_name = "gethostnamelinux"
landing_zone = "aws_lz212_ssm_history"
env = "non-prod"
year = date.today().year
report_name = landing_zone + "-" + env + "-" + str(year) + ".csv"
bucket_name = "ses-mail1"


class SSM_command:
    def __init__(self, id, document_name, run_date, status):
        self.id = id
        self.document_name = document_name
        self.run_date = run_date.strftime("%d/%b/%Y %H:%M:%S")
        self.status = status


ssm = session.client("ssm")

paginator_ssm = ssm.get_paginator("list_commands")

marker = None
pages = paginator_ssm.paginate(
    PaginationConfig={"PageSize": 5, "StartingToken": marker}
)


def download_from_s3(file_name):
    s3 = session.resource("s3")
    try:
        s3.Object(bucket_name, file_name).load()
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return 1
    else:
        s3 = session.client("s3")
        s3.download_file(
            bucket_name, file_name, "d:/1.REPO/Automation Tasks/" + file_name
        )
        return 0


def upload_file_to_bucket(bucket_name, file_path):
    try:
        s3_resource = session.resource(service_name="s3")
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
    current_month = date.today().month
    report_existed = download_from_s3(report_name)
    mode = "a" if os.path.exists(report_name) and current_month != 1 else "w"
    with open(report_name, mode, encoding="UTF8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if current_month == 1 or report_existed == 1:
            writer.writerow(field_names)
        # Write Data to File
        for command in list_commands:
            writer.writerow(
                [
                    command.id,
                    command.document_name,
                    command.run_date,
                    command.status,
                    command.ids_states,
                ]
            )

    print(
        upload_file_to_bucket(
            bucket_name=bucket_name,
            file_path="d:/1.REPO/Automation Tasks/" + report_name,
        )
    )


class SSM_command:
    def __init__(self, id, document_name, run_date, status, instances):
        self.id = id
        self.document_name = document_name
        self.run_date = run_date.strftime("%d/%b/%Y %H:%M:%S")
        self.status = status

        # return the list of instance for each cmd_id
        ids_states = []
        for instance in instances:
            state_response = ssm.list_commands(CommandId=id, InstanceId=instance)
            id_state = instance + ":" + state_response["Commands"][0]["Status"]
            ids_states.append(id_state)
        self.ids_states = ids_states


### MAIN ###
cmd_history = []

for page in pages:
    list_ex_cmds = page["Commands"]
    # filter the list_execute_commands with the document name only
    filter_list_cmds = list(
        filter(
            lambda list_ex_cmds: list_ex_cmds["DocumentName"] == document_name,
            list_ex_cmds,
        )
    )
    if len(filter_list_cmds) <= 0:
        skip
    for exe_cmd in filter_list_cmds:
        execution_command = SSM_command(
            exe_cmd["CommandId"],
            exe_cmd["DocumentName"],
            exe_cmd["RequestedDateTime"],
            exe_cmd["Status"],
            exe_cmd["Targets"][0]["Values"],
        )
        cmd_history.append(execution_command)


write_result(cmd_history)

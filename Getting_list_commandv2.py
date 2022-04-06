from collections import defaultdict
from datetime import date, datetime
from email.policy import default
import boto3
import botocore
import csv
import os
import json

## Parameters to run the script, their parameters either hard-code or put in environment variable ##
session = boto3.Session(profile_name="default")
document_name = ["gethostnamelinux"]
landing_zone = "aws_lz212_ssm_history"
env = "non-prod"
year = date.today().year
report_name = landing_zone + "-" + env + "-" + str(year) + ".csv"
bucket_name = "ses-mail1"


## Initial teh list of command SSM ##
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


def write_result():
    field_names = [
        "CommandId",
        "DocumentName",
        "RunTime",
        "Status",
        "TargetsStatus",
    ]
    current_month = date.today().month
    report_existed = download_from_s3(report_name)
    mode = "a" if os.path.exists(report_name) and current_month != 1 else "w"
    print("Writing to file in mode:", mode)
    with open(report_name, mode, encoding="UTF8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        if current_month == 1 or report_existed == 1:
            writer.writeheader()
        for k, v in ssm_history.items():
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


def target_status(instances, cmd_id):
    ids_states = []
    for id in instances:
        state_response = ssm.list_commands(CommandId=cmd_id, InstanceId=id)
        id_state = id + ":" + state_response["Commands"][0]["Status"]
        ids_states.append(id_state)
    return ids_states


### MAIN ###
## Mock Sample Json ##
f = open("sample.json")
ssm_history = json.load(f)
print(len(ssm_history))

#######################


# ssm_history = defaultdict()


# for page in pages:
#     list_ex_cmds = page["Commands"]
#     # filter the list_execute_commands with the document name only
#     filter_list_cmds = list(
#         filter(
#             lambda list_ex_cmds: list_ex_cmds["DocumentName"] in document_name,
#             list_ex_cmds,
#         )
#     )
#     if len(filter_list_cmds) <= 0:
#         continue
#     for exe_cmd in filter_list_cmds:
#         ssm_history[exe_cmd["CommandId"]] = {
#             "CommandId": exe_cmd["CommandId"],
#             "DocumentName": exe_cmd["DocumentName"],
#             "RunTime": exe_cmd["RequestedDateTime"].strftime("%d/%b/%Y %H:%M:%S"),
#             "Status": exe_cmd["Status"],
#             "TargetStatus": target_status(
#                 exe_cmd["Targets"][0]["Values"], exe_cmd["CommandId"]
#             ),
#         }


# print(json.dumps(ssm_history, indent=4))

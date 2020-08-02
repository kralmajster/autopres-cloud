from __future__ import print_function
import boto3
import json
import asyncio
import os

lambda_client = boto3.client('lambda')


def handler(event, context):
    print(event)
    input_form = json.loads(event['body'])
    if input_form is not None:
        keyword = str(input_form['text']) + '.exportcfg'
        command = str(input_form['command']).strip('/')
        response_url = str(input_form['response_url'])
        user_name = str(input_form['user_name'])
        channel_name = str(input_form['channel_name'])

    if command == "createprescloud":
        command = "createpres"

    params = {
        "keyword": keyword,
        "command": command,
        "response_url": response_url,
        "user_name": user_name,
        "channel_name": channel_name
    }

    if command == "createpreshelp":
        return {
            "statusCode": 200,
            "body": "Auto Presentation manual link: https://docs.google.com/document/d/1nbNpVm6SEU_ogtY-3HtoIYxgX5DudDdzJK6Q19wpaaE/edit?usp=sharing"
        }

    invoke_response = lambda_client.invoke(FunctionName=os.environ['CREATEPRES_LAMBDA_ARN'],
                                           InvokeArgs=json.dumps(params)
                                           )
    print(invoke_response)

    return {
        "statusCode": 200,
        "body": "Processing /" + command + " command for " + keyword +
        " config. It can take up to 1 minute."
    }

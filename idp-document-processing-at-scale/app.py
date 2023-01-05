#!/usr/bin/env python3
import aws_cdk as cdk
from workflows.demo_with_queries_stack import DemoQueries
from workflows.simple_async_workflow import SimpleAsyncWorkflow

app = cdk.App()

DemoQueries(app, "DemoQueries")
SimpleAsyncWorkflow(app, "SimpleAsyncWorkflow")

app.synth()

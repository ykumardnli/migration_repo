# Databricks notebook source
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for obj in w.workspace.list("/Users"):
    print("Object Path:",obj.path,"\nObject ID:",obj.resource_id)

# COMMAND ----------



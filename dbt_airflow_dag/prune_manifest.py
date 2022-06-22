from airflow.models import Variable
import sys
import os
import json
from typing import List


tasks = {}

DBT_RUN_GROUP_NAME = "dbt_run"
DBT_TEST_MODEL_GROUP_NAME = "dbt_test_model"
DBT_TEST_SOURCE_GROUP_NAME = "dbt_test_source"
SELECTORS = "dbt_selectors"


def get_parent_map():
    return manifest["parent_map"]

def is_ephemeral(key):
    return "ephemeral" == manifest["nodes"][key]["config"]["materialized"]

def get_fq_node_name(key):
    return ".".join(manifest["nodes"][key]["fqn"])


def seeds_or_sources(p: str):
    return not (p.startswith("source.") or p.startswith("seed."))


def get_or_create_task(task_name: str, group_id: str, command: str, upstreams: List[str], tags: List[str]):
    if task_name not in tasks:
        tasks[task_name] = {
            "group_id": group_id,
            "dbt_command": command,
            SELECTORS: [],
            "upstreams": list(filter(seeds_or_sources, upstreams)),
            "tags": tags
        }
    return tasks[task_name]


def add_selector(task: str, dbt_node_name: str):
    task[SELECTORS].append(dbt_node_name)


def add_skip_model(task_name: str, dbt_node_name: str, upstreams: List[str], tags: List[str]):
    task = get_or_create_task(task_name, DBT_RUN_GROUP_NAME, None, upstreams, tags)
    add_selector(task, dbt_node_name)


def add_run_model(task_name: str, dbt_node_name: str, upstreams: List[str], tags: List[str]):
    task = get_or_create_task(task_name, DBT_RUN_GROUP_NAME, "run", upstreams, tags)
    add_selector(task, dbt_node_name)


def add_test_model(task_name: str, dbt_node_name: str, upstreams: List[str], tags: List[str]):
    task = get_or_create_task(task_name, DBT_TEST_MODEL_GROUP_NAME, "test", upstreams, tags)
    add_selector(task, dbt_node_name)


def add_test_source(task_name: str, dbt_node_name: str, upstreams: List[str], tags: List[str]):
    task = get_or_create_task(task_name, DBT_TEST_SOURCE_GROUP_NAME, "test", upstreams, tags)
    add_selector(task, dbt_node_name)

def extract_dependencies():
    for key in get_parent_map():
        parents = get_parent_map()[key]        
        if key.startswith('model'):
            tags = manifest["nodes"][key]["config"]["tags"]
            if is_ephemeral(key):
                add_skip_model(
                    task_name=key,
                    dbt_node_name=get_fq_node_name(key),
                    upstreams=parents,
                    tags=tags)
            else:
                add_run_model(
                    task_name=key,
                    dbt_node_name=get_fq_node_name(key),
                    upstreams=parents,
                    tags=tags)
        elif key.startswith('test'):
            # Note that there are no tags directly tied to the test so we assign the tags of the parent node
            if len(parents) == 1:
                parent = parents[0]
                
                if parent.startswith('model.'):
                    tags = manifest["nodes"][parent]["config"]["tags"] 
                    add_test_model(
                        task_name='test_' + parent,
                        dbt_node_name=get_fq_node_name(key),
                        upstreams=parents,
                        tags=tags)
                elif parent.startswith('source.'):
                    add_test_source(
                        task_name='test_' + parent,
                        dbt_node_name=get_fq_node_name(key),
                        upstreams=[],
                        tags=tags)
            elif len(parents) == 2:
                # relationships test
                parents.sort()
                tags = manifest["nodes"][parents[0]]["config"]["tags"] 
                add_test_model(
                    task_name='test_' + ('__').join(parents),
                    dbt_node_name=get_fq_node_name(key),
                    upstreams=parents,
                    tags=tags)


def load_dbt_manifest(manifest_path):
    """
    Helper function to load the dbt manifest file.

    Returns: A JSON object containing the dbt manifest content.

    """
    with open(manifest_path) as f:
        file_content = json.load(f)
    return file_content


def extract_tasks() -> int:
    manifest_path = "./target/manifest.json"
    global manifest
    manifest = load_dbt_manifest(manifest_path)
    extract_dependencies()
    return tasks

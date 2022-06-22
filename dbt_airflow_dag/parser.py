from daggers.operators import GitBashOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import json
import logging
import os
from textwrap import dedent
from datetime import timedelta

class DbtDagParser:
    """
    A utility class that parses out a dbt project and creates the respective task groups
    Args:
        dag: The Airflow DAG
        dbt_global_cli_flags: Any global flags for the dbt CLI
        dbt_project_dir: The directory containing the dbt_project.yml
        dbt_profiles_dir: The directory containing the profiles.yml
        dbt_target: The dbt target profile (e.g. dev, prod)
        dbt_tag: Limit dbt models to this tag if specified.
        dbt_run_group_name: Optional override for the task group name.
        dbt_test_group_name: Optional override for the task group name.
    """

    def __init__(self,
                 dag=None,
                 repo_url=None,
                 dbt_project_dir=None,
                 dbt_global_cli_flags=None,
                 dbt_target=None,
                 dbt_dag=None,
                 dbt_tag=None,
                 data_interval_start=None,
                 data_interval_end=None,
                 ):

        self.dag = dag
        self.repo_url = repo_url
        self.dbt_project_dir = dbt_project_dir
        self.dbt_global_cli_flags = dbt_global_cli_flags
        self.dbt_target = dbt_target
        self.dbt_dag = dbt_dag
        self.dbt_tag = dbt_tag
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.dbt_groups = {}

        # Parse the manifest and populate the two task groups
        self.make_airflow_dag()

    def get_or_create_taskgroup(self, group_id):
        if group_id not in self.dbt_groups:
            self.dbt_groups[group_id] = TaskGroup(group_id)
        return self.dbt_groups[group_id]

    def make_dummy_dbt_task(self, task_id, group_id):
        # Keeping the log output, it's convenient to see when testing the python code outside of Airflow
        logging.info('Created dummy task: %s', task_id)
        return DummyOperator(
            task_id=task_id,
            task_group=self.get_or_create_taskgroup(group_id),
            dag=self.dag)

    def make_dbt_task(self, task_id, dbt_verb, dbt_selectors, group_id):
        """
        Takes the manifest JSON content and returns a BashOperator task
        to run a dbt command.

        Args:
            node_name: The name of the node
            dbt_verb: 'run' or 'test'

        Returns: A BashOperator task that runs the respective dbt command

        """

        # time by which the job is expected to succeed. Note that this represents the timedelta
        # after the period is closed. For example if you set an SLA of 1 hour, the scheduler 
        # would send an email soon after 1:00AM on the 2016-01-02 if the 2016-01-01 instance has
        # not succeeded yet. The scheduler pays special attention for jobs with an SLA and sends 
        # alert emails for SLA misses. SLA misses are also recorded in the database for future reference.
        # All tasks that share the same SLA time get bundled in a single email, sent soon after that time.
        # SLA notification are sent once and only once for each task instance.
        sla = timedelta(minutes=60)
        # max time allowed for the execution of this task instance, if it goes beyond it will raise and fail.
        execution_timeout = timedelta(hours=2)
        # When set, a task will be able to limit the concurrent runs across execution_dates.
        max_active_tis_per_dag = 1

        #inlets = self.dbt_dag[task_id]["upstreams"]
        #outlets = dbt_selectors
 
        cmd = dedent(f"""
                    cd {self.dbt_project_dir} && \
                    ./install_run_dbt.sh {self.dbt_global_cli_flags} \
                    {dbt_verb} \
                    --target {self.dbt_target} \
                    --models {" ".join(dbt_selectors)} \
                    --vars '{{ data_interval_start: {self.data_interval_start}, data_interval_end: {self.data_interval_end} }}' \
                    --profiles-dir `pwd` \
                    --project-dir `pwd`
                """)

        dbt_task = GitBashOperator(
            task_id=task_id,
            task_group=self.get_or_create_taskgroup(group_id),
            repo_url=self.repo_url,
            bash_command=cmd,
            sla=sla,
            execution_timeout=execution_timeout,
            max_active_tis_per_dag=max_active_tis_per_dag,
            #inlets=inlets,
            #outlets=outlets,
            dag=self.dag
        )

        # Keeping the log output, it's convenient to see when testing the python code outside of Airflow
        logging.info('Created task: %s', task_id)
        return dbt_task

    def get_upstream_airflow_tasks(self, ids):
        airflow_tasks = []
        for id in ids:
            airflow_tasks.append(self.dbt_tasks[id])
        return airflow_tasks

    def make_airflow_dag(self):
        """
        Parse out a JSON file and populates the task groups with dbt tasks

        Returns: None

        """
        self.dbt_tasks = {}

        # Create the tasks for each model
        for task_id in self.dbt_dag:
            dbt_command = self.dbt_dag[task_id]["dbt_command"]
            dbt_selectors = self.dbt_dag[task_id]["dbt_selectors"]
            group_id = self.dbt_dag[task_id]["group_id"]
            if dbt_command:
                self.dbt_tasks[task_id] = self.make_dbt_task(task_id, dbt_command, dbt_selectors, group_id)
            else:
                self.dbt_tasks[task_id] = self.make_dummy_dbt_task(task_id, group_id)

        for task_id in self.dbt_dag:
            task = self.dbt_dag[task_id]
            airflow_task = self.dbt_tasks[task_id]
            upstreams = self.get_upstream_airflow_tasks(task["upstreams"])
            airflow_task.set_upstream(upstreams)


    def get_dbt_groups(self):
        return self.dbt_groups


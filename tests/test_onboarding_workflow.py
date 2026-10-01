import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from src.onboarding.run_workflow import Workflow


class OnboardingWorkflowTest(unittest.TestCase):
    def test_local_spark_driver_uses_stable_loopback_binding(self):
        workflow = Workflow.__new__(Workflow)
        workflow.args = SimpleNamespace(namenode_container="namenode")
        workflow.spark_submit = "spark-submit"
        workflow._namenode_uri = Mock(return_value="hdfs://namenode:8020")
        workflow._environment = Mock(return_value={})
        workflow._run_command = Mock()

        workflow._spark("src/gold/run_gold.py")

        command = workflow._run_command.call_args.args[0]
        self.assertIn("spark.driver.bindAddress=127.0.0.1", command)
        self.assertIn("spark.driver.host=127.0.0.1", command)
        self.assertIn("spark.hadoop.fs.defaultFS=hdfs://namenode:8020", command)


if __name__ == "__main__":
    unittest.main()

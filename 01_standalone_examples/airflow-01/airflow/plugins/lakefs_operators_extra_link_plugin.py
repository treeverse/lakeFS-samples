from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperatorLink
from lakefs_provider.operators.create_branch_operator import LakeFSCreateBranchOperator
from lakefs_provider.operators.merge_operator import LakeFSMergeOperator
from lakefs_provider.operators.commit_operator import LakeFSCommitOperator
from airflow.models import XCom
from airflow.models import Variable

# define the extra link
class lakeFSLink(BaseOperatorLink):
    # name the link button in the UI
    name = "lakeFS UI"

    # add the button to one or more operators
    operators = [LakeFSCreateBranchOperator, LakeFSCommitOperator, LakeFSMergeOperator]

    # provide the link
    def get_link(self, operator, *, ti_key=None):
        if operator.task_type == 'LakeFSCreateBranchOperator' or operator.task_type == 'LakeFSCommitOperator' \
        or operator.task_type == 'LakeFSMergeOperator':
            lakeFSURL = Variable.get("lakefsUIEndPoint") + '/repositories/' \
                + Variable.get("repo") + '/commits/' \
                + XCom.get_value(key="return_value", ti_key=ti_key)
        else:
            lakeFSURL = ''
            
        return lakeFSURL

# define the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "lakefs_operators_extra_link_plugin"
    operator_extra_links = [
        lakeFSLink(),
    ]
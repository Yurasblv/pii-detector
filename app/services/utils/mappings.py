from typing import TYPE_CHECKING

from app.schemas.bitbucket import BitbucketConfig, BitBucketInputData
from app.schemas.documentdb import DocumentDBInputData
from app.schemas.dynamodb import DynamoDBInputData
from app.schemas.github import GithubConfig, GitHubInputData
from app.schemas.gitlab import GitlabConfig, GitLabInputData
from app.schemas.rds import RDSInputData
from app.schemas.redshift import RedshiftInputData
from app.schemas.s3 import S3InputData
from app.schemas.snowflake import SnowFlakeInputData, SnowflakeUser

repositories_mapper = {
    'GitHubRepository': 'GitHubBranch',
    'BitBucketRepository': 'BitBucketBranch',
    'GitLabProject': 'GitLabBranch',
}

engine_default_db = {'postgresql': 'postgres', 'mysql+pymysql': 'mysql'}


saas_config_mapper = {
    'GitHubBranch': GithubConfig,
    'GitLabBranch': GitlabConfig,
    'BitBucketBranch': BitbucketConfig,
    'SnowflakeDatabases': SnowflakeUser,
}

resource_configuration_mapper = {
    'SimpleStorageService': S3InputData,
    'RedshiftCluster': RedshiftInputData,
    'SnowflakeDatabases': SnowFlakeInputData,
    'RelationalDatabaseService': RDSInputData,
    'DynamoDB': DynamoDBInputData,
    'DocumentDBCluster': DocumentDBInputData,
    'GitHubBranch': GitHubInputData,
    'BitBucketBranch': BitBucketInputData,
    'GitLabBranch': GitLabInputData,
}

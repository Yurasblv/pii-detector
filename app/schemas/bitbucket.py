from typing import Optional

from pydantic import BaseModel


class BitbucketConfig(BaseModel):
    """
    A configuration model for connecting to a Bitbucket server.

    This class is designed to hold the necessary authentication and connection details for accessing a Bitbucket server.

    Attributes:
        application_password: The application password used for authenticating

        username: The username associated with the Bitbucket account. Used in conjunction with the application
        password for authentication purposes.

        server_url: The base URL of the Bitbucket server.
        This can be the URL of Bitbucket Cloud or a self-hosted Bitbucket server instance.
    """

    application_password: str
    username: str
    server_url: str


class BitBucketResult(BaseModel):
    """
    A model representing a result(file or object) related to BitBucket repository.

    This class encapsulates key details about a specific entity object of a source(BitBucket branch)
    including its name, repository, workspace, branch and others.

    It's used to store and convey information about branch contents.

    Attributes:
        owner: The username or account identifier of the owner of the repository.
        workspace: The workspace in which the repository is located.
        repository: The name of repository.
        source: The source path or identifier where the object is located within.
        file_name: The name of the file.
        type: The type of the entity (e.g., 'commit_file').
        branch: The repository branch where the object is located.
        size: The size of the entity in bytes, applicable for files.
    """

    owner: str
    workspace: str
    repository: str
    source: str
    file_name: str
    type: str
    branch: str
    size: int


class BitBucketInputData(BaseModel):
    """
    A model representing metadata specific to a BitBucket source(branch).

    This class is designed to store and manage general information about BitBucket branch, such as workspace,
    repository, branch.

    Attributes:
        workspace: workspace name to which branch and repository belongs to
        repository: repository name to which branch belongs to
        branch: name of source branch
    """

    workspace: Optional[str]
    repository: Optional[str]
    branch: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(BitBucket branch).

        This method provides a human-readable format of the source(BitBucket branch) metadata, primarily focusing on the
        source's name.

        Returns:
            A string representation of the BitBucket source(workspace/repository:branch).
        """
        return f"{self.workspace}/{self.repository}:{self.branch}"

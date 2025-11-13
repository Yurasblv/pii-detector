from typing import Optional

from pydantic import BaseModel


class GitlabConfig(BaseModel):
    """
    A configuration model for connecting to and interacting with a GitLab server.

    This class stores the necessary authentication and connection information required for accessing GitHub's services
    and APIs.

    Attributes:
        access_token: The personal access token used for authenticating with the GitLab server.
        server_url: The base URL of the GitLab server.
        namespace: The namespace (usually a username or group name) under which the repositories or projects are
        hosted.
    """

    access_token: str
    server_url: str
    namespace: str

class GitLabInputData(BaseModel):
    """
    A model representing general metadata specific to a GitLab source(branch).

    This class is designed to store and manage general information about GitLab branch, such as repository_id,
    repository_name, repository_owner, branch_name.

    Attributes:
        repo_id: unique identifier of repository to which branch belongs to
        repo_name: repository name to which branch belongs to
        repo_owner: owner of repository to which branch belongs to
        branch_name: name of source branch
    """

    repo_id: int
    repo_name: str
    repo_owner: Optional[str]
    branch_name: str
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(GitLab branch).

        This method provides a human-readable format of the source(GitLab branch) metadata, primarily focusing on the
        source's name.

        Returns:
            A string representation of the GitLab source (repo_name:branch_name).
        """
        return f'{self.repo_name}:{self.branch_name}'

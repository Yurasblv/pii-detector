import enum
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GithubConfig(BaseModel):
    """
    A configuration model for connecting to and interacting with a GitHub server.

    This class stores the necessary authentication and connection information required for accessing GitHub's services
    and APIs.

    Attributes:
        access_token: The personal access token used for authenticating with the GitHub server.
        server_url: The base URL of the GitHub server.
        owner: The username or organization name under which the repositories are hosted.
    """

    access_token: str
    server_url: str
    owner: str

class GitHubContentTypes(str, enum.Enum):
    """
    Enumeration for defining the content types associated with different GitHub repository archive formats.

    This enum class provides a mapping between common archive file extensions used in GitHub repositories and
    their corresponding MIME types.

    Content types:
        ZIP: Represents a ZIP archive with the MIME type 'application/zip'.
        TAR: Represents a TAR archive with the MIME type 'application/x-tar'.
        TAR_GZ: Represents a TAR.GZ (GZip compressed TAR) archive with the MIME type 'application/gzip'.
        TAR_BZ2: Represents a TAR.BZ2 (BZip2 compressed TAR) archive with the MIME type 'application/x-bzip2'.

    Each element is a tuple where the first element is the file extension and the second element is the corresponding
    MIME type.
    """

    ZIP = ('zip', 'application/zip')
    TAR = ('tar', 'application/x-tar')
    TAR_GZ = ('tar.gz', 'application/gzip')
    TAR_BZ2 = ('tar.bz2', 'application/x-bzip2')

    def __new__(cls, value: str, content_type: str) -> 'GitHubContentTypes':
        """
        Creates a new instance of the GitHubContentTypes enum.

        This method overrides is used internally by the GitHubContentTypes enum to create its elements,
        each of which represents a specific archive file format and its corresponding MIME type.

        Args:
            value: The file extension associated with the GitHub content type.
            content_type: The MIME type corresponding to the file extension.

        Returns:
            obj: A new instance of the GitHubContentTypes enum with both 'value' and 'content_type'
            attributes.
        """
        obj = str.__new__(cls)
        obj._value_ = value
        obj.content_type = content_type
        return obj


class GitHubInputData(BaseModel):
    """
    A model representing metadata specific to a GitHub source(branch).

    This class is designed to store and manage general information about GitHub branch, such as repository_name,
    repository_owner, branch_name, repository_creation_date.

    Attributes:
        repo_name: repository name to which branch belongs to
        branch_name: name of source branch
        repo_owner: owner of repository to which branch belongs to
        repo_creation_date: date of repository creation
    """

    repo_name: str
    branch_name: str
    repo_owner: str
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(GitHub branch).

        This method provides a human-readable format of the source(GitHub branch) metadata, primarily focusing on the
        source's name and owner.

        Returns:
            A string representation of the GitLab source (repo_owner/repo_name:branch_name).
        """
        return f"{self.repo_owner}/{self.repo_name}:{self.branch_name}"

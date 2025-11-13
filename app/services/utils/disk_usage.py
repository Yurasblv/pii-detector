import psutil  # type: ignore


def check_archive_size(size: int) -> bool:
    """
    Check free left size of EBS storage and compare it with incoming object's size.

    Args:
        size: size of an object

    Returns:
        boolean: False If size of an object bigger that left space else returns True
    """
    return True if psutil.disk_usage('/').free > size else False

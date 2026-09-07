import pathlib
import typing

import yaml

from ..config import get_cache_subdirectory
from ..utils.encryption import read_text_from_file, write_text_to_file


def load_ip_cache(
    *,
    cache_type: typing.Literal["region_codes_to_coordinates"],
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> dict[str, str]:
    """Load an IP cache file from the ``ips`` subdirectory of the cache directory.

    Parameters
    ----------
    cache_type : str
        The type of IP cache to load.
    cache_directory : path-like, optional
        The cache directory to use. If ``None``, the default cache directory is used.
    use_encryption : bool, optional
        If ``True`` (default), the cache file content is decrypted before parsing.
        If ``False``, the file content is read as plaintext YAML.
    """
    ip_cache_directory = get_cache_subdirectory(cache_directory=cache_directory, name="ips")
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    if not cache_file_path.exists():
        cache_file_path.touch()
        return {}

    content = read_text_from_file(file_path=cache_file_path, use_encryption=use_encryption)
    data = yaml.safe_load(stream=content) or {}
    return data


def write_ip_cache(
    *,
    data: dict,
    cache_type: typing.Literal["region_codes_to_coordinates"],
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> None:
    """Write data to an IP cache file, optionally encrypting the content.

    Parameters
    ----------
    data : dict
        The data to write to the cache file.
    cache_type : str
        The type of IP cache to write.
    cache_directory : path-like, optional
        The cache directory to use. If ``None``, the default cache directory is used.
    use_encryption : bool, optional
        If ``True`` (default), the content is encrypted before writing.
        If ``False``, the content is written as plaintext YAML.
    """
    ip_cache_directory = get_cache_subdirectory(cache_directory=cache_directory, name="ips")
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    text = yaml.dump(data=data)
    write_text_to_file(file_path=cache_file_path, text=text, use_encryption=use_encryption)

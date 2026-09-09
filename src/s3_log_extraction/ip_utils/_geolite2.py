"""Acquisition of the MaxMind GeoLite2-City database used for IP geolocation.

GeoLite2 is a free, locally-queried database rather than a metered web API. It is refreshed by MaxMind twice a
week, and downloading it requires a (free) MaxMind account: the account ID and a license key are read from the
``MAXMIND_ACCOUNT_ID`` and ``MAXMIND_LICENSE_KEY`` environment variables.
"""

import datetime
import os
import pathlib
import tarfile
import tempfile
import warnings

from ..config import get_cache_subdirectory

GEOLITE2_DATABASE_EDITION = "GeoLite2-City"
GEOLITE2_DATABASE_FILE_NAME = f"{GEOLITE2_DATABASE_EDITION}.mmdb"
GEOLITE2_DOWNLOAD_URL = (
    f"https://download.maxmind.com/geoip/databases/{GEOLITE2_DATABASE_EDITION}/download?suffix=tar.gz"
)

# MaxMind publishes new GeoLite2 data on Tuesdays and Fridays, so a copy older than this is considered stale.
GEOLITE2_MAX_DATABASE_AGE_IN_DAYS = 7

_CREDENTIAL_ENVIRONMENT_VARIABLES = ("MAXMIND_ACCOUNT_ID", "MAXMIND_LICENSE_KEY")


def get_geolite2_database_path(*, cache_directory: str | pathlib.Path | None = None) -> pathlib.Path:
    """
    Return the path of the GeoLite2-City database within the cache directory, whether or not it exists yet.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If ``None``, the default cache directory will be used.
    """
    geolite2_directory = get_cache_subdirectory(cache_directory=cache_directory, name="geolite2")
    return geolite2_directory / GEOLITE2_DATABASE_FILE_NAME


def _get_maxmind_credentials() -> tuple[str, str] | None:
    """Return the MaxMind account ID and license key from the environment, or ``None`` if either is unset."""
    credentials = tuple(os.environ.get(name, "").strip() for name in _CREDENTIAL_ENVIRONMENT_VARIABLES)
    if not all(credentials):
        return None
    return credentials


def _is_database_stale(database_path: pathlib.Path, *, _now: datetime.datetime | None = None) -> bool:
    """Return ``True`` if the database file is older than ``GEOLITE2_MAX_DATABASE_AGE_IN_DAYS``."""
    now = _now if _now is not None else datetime.datetime.now(tz=datetime.timezone.utc)
    modified = datetime.datetime.fromtimestamp(database_path.stat().st_mtime, tz=datetime.timezone.utc)
    return (now - modified) > datetime.timedelta(days=GEOLITE2_MAX_DATABASE_AGE_IN_DAYS)


def update_geolite2_database(
    *,
    cache_directory: str | pathlib.Path | None = None,
    force: bool = False,
) -> pathlib.Path:
    """
    Download the GeoLite2-City database into the cache directory if it is missing or stale.

    Requires the ``MAXMIND_ACCOUNT_ID`` and ``MAXMIND_LICENSE_KEY`` environment variables. Both come with a free
    MaxMind account (https://www.maxmind.com/en/geolite2/signup); the license key must have GeoLite2 download
    permission.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If ``None``, the default cache directory will be used.
    force : bool
        If ``True``, download a fresh copy even when the cached one is not yet stale.
        Default is ``False``.

    Returns
    -------
    pathlib.Path
        The path of the database file.
    """
    database_path = get_geolite2_database_path(cache_directory=cache_directory)
    if not force and database_path.exists() and not _is_database_stale(database_path=database_path):
        return database_path

    credentials = _get_maxmind_credentials()
    if credentials is None:
        message = (
            f"The environment variables {' and '.join(_CREDENTIAL_ENVIRONMENT_VARIABLES)} must be set to download "
            f"the {GEOLITE2_DATABASE_EDITION} database! Both come with a free MaxMind account "
            "(https://www.maxmind.com/en/geolite2/signup)."
        )
        raise ValueError(message)

    _download_geolite2_database(database_path=database_path, credentials=credentials)

    return database_path


def _download_geolite2_database(*, database_path: pathlib.Path, credentials: tuple[str, str]) -> None:
    """Stream the GeoLite2 tarball from MaxMind and replace ``database_path`` with the ``.mmdb`` inside it."""
    import requests

    timeout_in_seconds = 120
    with tempfile.TemporaryDirectory(dir=database_path.parent) as temporary_directory:
        archive_path = pathlib.Path(temporary_directory) / f"{GEOLITE2_DATABASE_EDITION}.tar.gz"

        with requests.get(
            url=GEOLITE2_DOWNLOAD_URL, auth=credentials, stream=True, timeout=timeout_in_seconds
        ) as response:
            response.raise_for_status()
            with archive_path.open(mode="wb") as file_stream:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    file_stream.write(chunk)

        with tarfile.open(name=archive_path, mode="r:gz") as archive:
            database_member = next(
                (member for member in archive.getmembers() if member.name.endswith(GEOLITE2_DATABASE_FILE_NAME)),
                None,
            )
            if database_member is None:
                message = f"No {GEOLITE2_DATABASE_FILE_NAME} was found in the archive downloaded from MaxMind!"
                raise RuntimeError(message)

            extracted_path = pathlib.Path(temporary_directory) / GEOLITE2_DATABASE_FILE_NAME
            with archive.extractfile(member=database_member) as member_stream, extracted_path.open(mode="wb") as out:
                out.write(member_stream.read())

        # Replace atomically so that a reader opened concurrently never sees a partial file
        extracted_path.replace(target=database_path)


def open_geolite2_database(*, cache_directory: str | pathlib.Path | None = None) -> "geoip2.database.Reader":
    """
    Open a reader over the GeoLite2-City database in the cache directory, downloading it first if needed.

    A missing database is always downloaded, which requires MaxMind credentials. A stale database is refreshed
    when credentials are available and otherwise used as-is with a warning.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If ``None``, the default cache directory will be used.
    """
    import geoip2.database

    database_path = get_geolite2_database_path(cache_directory=cache_directory)
    if not database_path.exists() or _get_maxmind_credentials() is not None:
        update_geolite2_database(cache_directory=cache_directory)
    elif _is_database_stale(database_path=database_path):
        warnings.warn(
            message=(
                f"The cached {GEOLITE2_DATABASE_EDITION} database is more than {GEOLITE2_MAX_DATABASE_AGE_IN_DAYS} "
                f"days old but cannot be refreshed because {' and '.join(_CREDENTIAL_ENVIRONMENT_VARIABLES)} are "
                "not set. Continuing with the stale copy."
            ),
            category=RuntimeWarning,
            stacklevel=2,
        )

    return geoip2.database.Reader(fileish=database_path)

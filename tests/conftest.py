import pathlib

import pytest
import yaml

from s3_log_extraction.ip_utils import MappingRegionResolver

_MOCKED_IP_TO_REGION_FILE_PATH = pathlib.Path(__file__).parent / "mocked_ips" / "ip_to_region.yaml"


@pytest.fixture
def mocked_region_resolver() -> MappingRegionResolver:
    """
    A stand-in geolocation of the requesters in the example logs.

    Every requester of the example logs is a documentation-range address (RFC 5737), which a real resolution labels
    ``bogon``; this resolver maps them to the invented regions of ``mocked_ips/ip_to_region.yaml`` instead, so that
    the summaries have resolved regions to report.
    """
    return MappingRegionResolver(yaml.safe_load(_MOCKED_IP_TO_REGION_FILE_PATH.read_text()))


@pytest.fixture
def use_mocked_region_resolver(
    monkeypatch: pytest.MonkeyPatch, mocked_region_resolver: MappingRegionResolver
) -> MappingRegionResolver:
    """Make the summaries resolve requesters with the mocked resolver, for tests that reach them through the CLI."""
    monkeypatch.setattr(
        "s3_log_extraction.summarize._generate_summaries.IpRegionResolver", lambda **kwargs: mocked_region_resolver
    )
    return mocked_region_resolver

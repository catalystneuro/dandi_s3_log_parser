_KNOWN_SERVICES = ("GitHub", "AWS", "GCP", "VPN")  # Azure has problems; see _ip_utils.py for more info

EXCLUDED_REGION_LABELS = frozenset(["VPN", "GitHub", "unknown", "undetermined", "missing", "bogon"])


def is_cloud_service_or_vpn_label(region_label: str, /) -> bool:
    """
    Determine whether a region/service label (as produced by ``ip_to_region``) refers to a
    known cloud service or VPN provider (e.g. ``"GitHub"``, ``"AWS/us-east-1"``, ``"GCP/us-central1"``,
    ``"VPN"``) rather than a genuine geographic requester location.

    Note that unresolved labels such as ``"unknown"``, ``"undetermined"``, ``"missing"``, or ``"bogon"``
    are NOT considered cloud service or VPN labels here; they simply mean the requester's location could
    not be determined, not that the requester is known cloud/VPN infrastructure.
    """
    return any(
        region_label == service_name or region_label.startswith(f"{service_name}/") for service_name in _KNOWN_SERVICES
    )


def is_resolved_region(region_label: str, /) -> bool:
    """
    Determine whether a region/service label (as produced by ``ip_to_region``) names an actual place.

    A resolved label always pairs a top-level code with a subdivision of it, written as ``"US/CA"``
    (ISO 3166-1 alpha-2 country code and ISO 3166-2 subdivision code) for a geographic location or as
    ``"AWS/us-east-1"`` for a cloud service region. The slash is what makes the label resolved.

    Labels without a slash name no location. Some of them are unresolved outcomes of geolocation
    (``"unknown"``, ``"undetermined"``, ``"missing"``, ``"bogon"``) and others are services whose region
    was never reported (``"GitHub"``, ``"VPN"``).
    """
    return "/" in region_label


_DEFAULT_REGION_CODES_TO_COORDINATES = {
    # Included for testing/demo purposes
    "AWS/us-east-2": {"latitude": 39.9612, "longitude": -82.9988},
    "GCP/us-central1": {"latitude": 41.2619, "longitude": -95.8608},
    # Skip unknowable entries
    **{label: {"latitude": None, "longitude": None} for label in EXCLUDED_REGION_LABELS},
}

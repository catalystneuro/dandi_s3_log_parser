# Azure has problems; see _ip_utils.py for more info.
# "GH-actions" is matched before "GitHub" so that GitHub Actions runner ranges resolve to their own
# label rather than the broader "GitHub" one; see the split in _ip_utils.py.
_KNOWN_SERVICES = ("GH-actions", "GitHub", "AWS", "GCP", "VPN")

# The label under which GitHub Actions runner ranges are recorded. Kept as a single source of truth so
# the taxonomy (_ip_utils.py), the predicates below, and the view exclusion all agree on the spelling.
GITHUB_ACTIONS_LABEL = "GH-actions"

EXCLUDED_REGION_LABELS = frozenset(["VPN", "GitHub", "GH-actions", "unknown", "undetermined", "missing", "bogon"])


def is_cloud_service_or_vpn_label(region_label: str, /) -> bool:
    """
    Determine whether a region/service label (as produced by ``ip_to_region``) refers to a
    known cloud service or VPN provider (e.g. ``"GitHub"``, ``"GH-actions"``, ``"AWS/us-east-1"``,
    ``"GCP/us-central1"``, ``"VPN"``) rather than a genuine geographic requester location.

    Note that unresolved labels such as ``"unknown"``, ``"undetermined"``, ``"missing"``, or ``"bogon"``
    are NOT considered cloud service or VPN labels here; they simply mean the requester's location could
    not be determined, not that the requester is known cloud/VPN infrastructure.
    """
    return any(
        region_label == service_name or region_label.startswith(f"{service_name}/") for service_name in _KNOWN_SERVICES
    )


def is_github_actions_label(region_label: str, /) -> bool:
    """
    Determine whether a region/service label (as produced by ``ip_to_region``) refers to a GitHub Actions
    runner range.

    This is the automated-CI subset of the broader ``"GitHub"`` label: it is unambiguously continuous
    integration traffic, whereas the remaining GitHub ranges (Codespaces, the web/API, etc.) can carry
    genuine interactive use such as a human streaming a file from a notebook in a Codespace. Views are
    excluded on this narrow label only, so those legitimate GitHub-hosted requesters are still counted.
    """
    return region_label == GITHUB_ACTIONS_LABEL or region_label.startswith(f"{GITHUB_ACTIONS_LABEL}/")


def is_resolved_region(region_label: str, /) -> bool:
    """
    Determine whether a region/service label (as produced by ``ip_to_region``) names an actual place.

    A resolved label always pairs a top-level code with a subdivision of it, written as ``"US/California"``
    for a geographic location or as ``"AWS/us-east-1"`` for a cloud service region. The slash is what makes
    the label resolved.

    Labels without a slash name no location. Some of them are unresolved outcomes of geolocation
    (``"unknown"``, ``"undetermined"``, ``"missing"``, ``"bogon"``) and others are services whose region
    was never reported (``"GitHub"``, ``"VPN"``).
    """
    return "/" in region_label


_DEFAULT_REGION_CODES_TO_COORDINATES = {
    # Included for testing/demo purposes
    "AWS/us-east-2": {"latitude": 39.9612, "longitude": -82.9988},
    "GCP/us-central1": {"latitude": 41.2619, "longitude": -95.8608},
    # Explicit entries that fail heuristic matching
    "BO/La Paz Department": {"latitude": -11.7773231, "longitude": -67.4519752},
    "CL/Valparaíso": {"latitude": -32.5976089, "longitude": -70.8529753},
    "CN/Hainan": {"latitude": 19.2000001, "longitude": 109.5999999},
    "CO/Huila Department": {"latitude": 2.53593490, "longitude": -75.52766990},
    "CO/Risaralda Department": {"latitude": 5.2102948, "longitude": -75.9842236},
    "CR/San José": {"latitude": 9.9325427, "longitude": -84.0795782},
    "ES/Castille and León": {"latitude": 42.00, "longitude": -5.5},
    "FR/Grand Est": {"latitude": 48.580002, "longitude": 7.750000},
    "IQ/Sulaymaniyah": {"latitude": 35.5574725, "longitude": 45.435202},
    "JP/Ishikawa": {"latitude": 36.9890574, "longitude": 136.8162839},
    "MO/São Francisco Xavier": {"latitude": 22.210928, "longitude": 113.552971},
    "MX/México": {"latitude": 19.4326296, "longitude": -99.1331785},
    "NI/Managua Department": {"latitude": 12.125, "longitude": -86.31},
    "NO/Vestland": {"latitude": 60.9291011, "longitude": 6.1078869},
    "PA/Panamá": {"latitude": 8.559559, "longitude": -81.1308434},
    "PA/Panamá Oeste Province": {"latitude": 8.88028, "longitude": -79.78330},
    "PE/Lima Province": {"latitude": -12.5453873, "longitude": -75.8599243},
    "PL/Greater Poland": {"latitude": 52.406374, "longitude": 16.9251681},
    "PL/Pomerania": {"latitude": 53.428543, "longitude": 14.552811},
    "PL/Silesia": {"latitude": 50.6966393, "longitude": 17.9254068},
    "PL/Subcarpathia": {"latitude": 50.0575, "longitude": 22.0896},
    "PR/San Juan": {"latitude": 18.384239, "longitude": -66.05344},
    "RU/Rostov": {"latitude": 57.2012699, "longitude": 39.4221813},
    "TW/Takao": {"latitude": 22.6226696, "longitude": 120.2764261},
    "VN/Long An Povince": {"latitude": 10.56071680, "longitude": 106.64976230},
    "UY/Montevideo Department": {"latitude": -34.9058916, "longitude": -56.1913095},
    "RU/Mordoviya Republic": {"latitude": 54.5, "longitude": 44},  # OpenCage mistakes with Missouri
    "NZ/Taranaki Region": {"latitude": -39.3848064, "longitude": 174.1973505},  # There's a Taranaki in CL apparently
    # Skip unknowable entries
    **{label: {"latitude": None, "longitude": None} for label in EXCLUDED_REGION_LABELS},
}

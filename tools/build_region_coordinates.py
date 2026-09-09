"""
Regenerate the ISO 3166 lookup tables shipped in ``s3_log_extraction/ip_utils/_data/``.

The tables give every ISO 3166-1 country its alpha-2 and alpha-3 codes and a representative coordinate, and every
ISO 3166-2 subdivision known to Natural Earth a representative coordinate. They let ``update ip coordinates`` run
entirely offline.

Sources
-------
- ISO 3166-1 codes: the ``pycountry`` package (a packaging of the Debian ``iso-codes`` data).
- Coordinates: Natural Earth (public domain, https://www.naturalearthdata.com), through its GitHub mirror at
  https://github.com/nvkelso/natural-earth-vector. Countries use the ``LABEL_X``/``LABEL_Y`` point of the 1:10m
  Admin-0 dataset; subdivisions use the ``latitude``/``longitude`` label point of the 1:10m Admin-1 dataset. Label
  points sit at a sensible interior location of each region, which is what a heat map wants.

Usage
-----
    pip install pycountry requests
    python tools/build_region_coordinates.py

Natural Earth is only needed at build time; the package itself reads the generated CSV files.
"""

import collections
import csv
import pathlib
import re
import statistics

import pycountry
import requests

NATURAL_EARTH_BASE_URL = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master"
ADMIN_0_FILE = "geojson/ne_10m_admin_0_countries.geojson"
ADMIN_1_FILE = "geojson/ne_10m_admin_1_states_provinces.geojson"

_ISO_3166_2_PATTERN = re.compile(r"^(?P<alpha_2>[A-Z]{2})-(?P<subdivision>[A-Z0-9]{1,3})$")

DATA_DIRECTORY = pathlib.Path(__file__).parent.parent / "src" / "s3_log_extraction" / "ip_utils" / "_data"


def _fetch_features(file_name: str) -> list[dict]:
    response = requests.get(url=f"{NATURAL_EARTH_BASE_URL}/{file_name}", timeout=600)
    response.raise_for_status()
    return response.json()["features"]


def _fetch_version() -> str:
    response = requests.get(url=f"{NATURAL_EARTH_BASE_URL}/VERSION", timeout=60)
    response.raise_for_status()
    return response.text.strip()


def _round(value: float) -> str:
    return f"{value:.4f}"


def build_countries(admin_0_features: list[dict]) -> list[dict[str, str]]:
    """One row per ISO 3166-1 country, with a Natural Earth label point where one exists."""
    # The plain ISO_A2 field is "-99" for a few countries (France, Norway, ...); the _EH variants fix that
    label_points: dict[str, tuple[float, float]] = {}
    for feature in admin_0_features:
        properties = feature["properties"]
        alpha_2 = properties.get("ISO_A2_EH") or properties.get("ISO_A2")
        if alpha_2 and re.fullmatch(r"[A-Z]{2}", alpha_2) and alpha_2 not in label_points:
            label_points[alpha_2] = (properties["LABEL_Y"], properties["LABEL_X"])

    rows = []
    for country in sorted(pycountry.countries, key=lambda country: country.alpha_2):
        latitude, longitude = label_points.get(country.alpha_2, (None, None))
        rows.append(
            {
                "alpha_2": country.alpha_2,
                "alpha_3": country.alpha_3,
                "latitude": _round(latitude) if latitude is not None else "",
                "longitude": _round(longitude) if longitude is not None else "",
            }
        )
    return rows


def _parent_codes(iso_3166_2_code: str) -> list[str]:
    """Walk the ISO 3166-2 hierarchy upward, e.g. ``"FR-67"`` -> ``["FR-6AE", "FR-GES"]``."""
    parents = []
    subdivision = pycountry.subdivisions.get(code=iso_3166_2_code)
    while subdivision is not None and subdivision.parent_code:
        parents.append(subdivision.parent_code)
        subdivision = pycountry.subdivisions.get(code=subdivision.parent_code)
    return parents


def build_subdivisions(admin_1_features: list[dict], alpha_2_to_alpha_3: dict[str, str]) -> list[dict[str, str]]:
    """
    One row per ISO 3166-2 subdivision, keyed by the alpha-3 country code.

    Natural Earth's units are not always first-level subdivisions: for the United Kingdom they are counties, for
    France departments, for Italy provinces. Geolocation databases report the first level (England, Grand Est,
    Lazio), so every unit's point is also credited to its ISO parents, and a parent that Natural Earth has no unit
    for gets the mean of its descendants' points.
    """
    own_points: dict[str, list[tuple[float, float]]] = collections.defaultdict(list)
    descendant_points: dict[str, list[tuple[float, float]]] = collections.defaultdict(list)
    for feature in admin_1_features:
        properties = feature["properties"]
        code = properties.get("iso_3166_2") or ""
        if _ISO_3166_2_PATTERN.match(code) is None:
            continue  # Natural Earth's placeholder codes such as "SY-X01~" are not ISO codes
        point = (properties["latitude"], properties["longitude"])
        own_points[code].append(point)
        for parent_code in _parent_codes(iso_3166_2_code=code):
            descendant_points[parent_code].append(point)

    rows = []
    for code in sorted(set(own_points) | set(descendant_points)):
        match = _ISO_3166_2_PATTERN.match(code)
        alpha_3 = alpha_2_to_alpha_3.get(match["alpha_2"]) if match is not None else None
        if alpha_3 is None:
            continue
        # A subdivision split over several features (islands, exclaves) gets the mean of its label points
        points = own_points.get(code) or descendant_points[code]
        subdivision = match["subdivision"]
        rows.append(
            {
                "alpha_3": alpha_3,
                "subdivision": subdivision,
                "latitude": _round(statistics.fmean(latitude for latitude, _ in points)),
                "longitude": _round(statistics.fmean(longitude for _, longitude in points)),
            }
        )
    return rows


def _write_csv(*, file_path: pathlib.Path, rows: list[dict[str, str]], header_comment: str) -> None:
    with file_path.open(mode="w", newline="") as file_stream:
        file_stream.write(f"# {header_comment}\n")
        writer = csv.DictWriter(file_stream, fieldnames=list(rows[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    version = _fetch_version()
    admin_0_features = _fetch_features(file_name=ADMIN_0_FILE)
    admin_1_features = _fetch_features(file_name=ADMIN_1_FILE)

    countries = build_countries(admin_0_features=admin_0_features)
    alpha_2_to_alpha_3 = {row["alpha_2"]: row["alpha_3"] for row in countries}
    subdivisions = build_subdivisions(admin_1_features=admin_1_features, alpha_2_to_alpha_3=alpha_2_to_alpha_3)

    DATA_DIRECTORY.mkdir(exist_ok=True)
    provenance = (
        f"Generated by tools/build_region_coordinates.py from pycountry {pycountry.__version__} and "
        f"Natural Earth {version} (public domain). Do not edit by hand."
    )
    _write_csv(file_path=DATA_DIRECTORY / "countries.csv", rows=countries, header_comment=provenance)
    _write_csv(file_path=DATA_DIRECTORY / "subdivisions.csv", rows=subdivisions, header_comment=provenance)

    located_countries = sum(1 for row in countries if row["latitude"])
    print(
        f"Wrote {len(countries)} countries ({located_countries} with coordinates) and {len(subdivisions)} subdivisions"
    )


if __name__ == "__main__":
    main()

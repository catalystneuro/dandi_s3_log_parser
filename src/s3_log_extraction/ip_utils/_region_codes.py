"""
Offline ISO 3166 lookups: alpha-2 to alpha-3 country codes and representative coordinates for regions.

Region labels pair an ISO 3166-1 alpha-3 country code with an ISO 3166-2 subdivision code, separated by a slash,
as in ``"USA/CA"`` for California or ``"GBR/ENG"`` for England. A label with no slash names only a country.

The tables behind these lookups are shipped with the package in ``_data/`` and regenerated with
``tools/build_region_coordinates.py``; see that script for their provenance.
"""

import csv
import functools
import importlib.resources

Coordinates = dict[str, float]


def _read_data_table(file_name: str) -> list[dict[str, str]]:
    """Read one of the shipped CSV tables, skipping its provenance comment."""
    data_file = importlib.resources.files(__package__) / "_data" / file_name
    with data_file.open(mode="r", encoding="utf-8", newline="") as file_stream:
        rows = (line for line in file_stream if not line.startswith("#"))
        return list(csv.DictReader(rows))


@functools.cache
def _load_countries() -> dict[str, dict[str, str]]:
    """Map every ISO 3166-1 alpha-2 code to its row (alpha-3 code and coordinates)."""
    return {row["alpha_2"]: row for row in _read_data_table(file_name="countries.csv")}


@functools.cache
def _load_country_coordinates() -> dict[str, Coordinates]:
    """Map every ISO 3166-1 alpha-3 code with a known location to its coordinates."""
    return {
        row["alpha_3"]: {"latitude": float(row["latitude"]), "longitude": float(row["longitude"])}
        for row in _load_countries().values()
        if row["latitude"] and row["longitude"]
    }


@functools.cache
def _load_subdivision_coordinates() -> dict[str, Coordinates]:
    """Map every known ``"USA/CA"``-style label to its coordinates."""
    return {
        f"{row['alpha_3']}/{row['subdivision']}": {
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
        }
        for row in _read_data_table(file_name="subdivisions.csv")
    }


def country_alpha_2_to_alpha_3(alpha_2: str, /) -> str:
    """
    Convert an ISO 3166-1 alpha-2 country code to its alpha-3 form.

    Codes outside ISO 3166-1, such as the user-assigned ``"XK"`` that geolocation databases use for Kosovo, have
    no alpha-3 form and are returned unchanged so that the information is not lost.
    """
    country = _load_countries().get(alpha_2.upper())
    return country["alpha_3"] if country is not None else alpha_2.upper()


def get_region_coordinates(region_label: str, /) -> Coordinates | None:
    """
    Look up a representative coordinate for a geographic region label.

    A subdivision label such as ``"USA/CA"`` resolves to the subdivision's own point when the tables know it, and
    otherwise falls back to the country's point. A bare country label such as ``"USA"`` resolves to the country's
    point. Returns ``None`` when neither is known.
    """
    coordinates = _load_subdivision_coordinates().get(region_label)
    if coordinates is not None:
        return coordinates

    alpha_3 = region_label.split("/", 1)[0]
    return _load_country_coordinates().get(alpha_3)

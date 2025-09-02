import re

class NMEAParser:
    GGA_REGEX = re.compile(r"^\$(?:GP|GN|GL|GA|GB)GGA,")
    NMEA_LINE = re.compile(r"^\$.*\*[0-9A-Fa-f]{2}\s*$")

    @staticmethod
    def checksum_ok(sentence: str) -> bool:
        """Validate NMEA checksum."""
        if not sentence.startswith("$") or "*" not in sentence:
            return False
        data, _, checksum_str = sentence[1:].partition("*")
        try:
            expected = int(checksum_str[:2], 16)
        except ValueError:
            return False
        calc = 0
        for ch in data:
            calc ^= ord(ch)
        return calc == expected

    @staticmethod
    def ddmm_to_decimal(value: str, hemi: str, is_lat: bool):
        """Convert ddmm.mmmm to decimal degrees."""
        if not value or "." not in value:
            return None
        try:
            if is_lat:
                deg = int(value[:2]); minutes = float(value[2:])
            else:
                deg = int(value[:3]); minutes = float(value[3:])
        except ValueError:
            return None
        decimal = deg + minutes / 60.0
        if (is_lat and hemi == "S") or (not is_lat and hemi == "W"):
            decimal = -decimal
        return decimal

    @classmethod
    def parse_gga(cls, sentence: str):
        """Parse GGA sentence into dict."""
        if not cls.GGA_REGEX.match(sentence):
            return None
        if not cls.checksum_ok(sentence):
            return None
        core = sentence.strip()[1:]
        data, _, _ = core.partition("*")
        parts = data.split(",")
        try:
            utc = parts[1] or ""
            lat = cls.ddmm_to_decimal(parts[2], parts[3], True)
            lon = cls.ddmm_to_decimal(parts[4], parts[5], False)
            fix_quality = int(parts[6] or 0)
            num_sats = int(parts[7] or 0)
            hdop = float(parts[8]) if parts[8] else None
            alt_m = float(parts[9]) if parts[9] else None
        except (IndexError, ValueError):
            return None
        if lat is None or lon is None:
            return None
        return {
            "type": "gga",
            "timestamp_utc": utc,
            "lat": lat,
            "lon": lon,
            "alt_m": alt_m,
            "fix_quality": fix_quality,
            "num_sats": num_sats,
            "hdop": hdop,
        }

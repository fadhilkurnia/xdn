package edu.umass.cs.nio.interfaces;

/**
 * Typed geographic coordinate for a node. Latitude is in [-90, 90] degrees,
 * longitude in [-180, 180] degrees.
 */
public record Geolocation(double latitude, double longitude) {

  public Geolocation {
    if (latitude < -90.0 || latitude > 90.0) {
      throw new IllegalArgumentException("latitude out of range [-90, 90]: " + latitude);
    }
    if (longitude < -180.0 || longitude > 180.0) {
      throw new IllegalArgumentException("longitude out of range [-180, 180]: " + longitude);
    }
  }

  /**
   * Parses {@code "lat,lon"}. Surrounding double quotes and whitespace are tolerated. Returns
   * {@code null} on malformed input (non-numeric parts, wrong cardinality, out-of-range values,
   * or {@code null} input) so callers can warn-and-skip.
   */
  public static Geolocation parse(String raw) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
      trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
    }
    String[] parts = trimmed.split(",");
    if (parts.length != 2) {
      return null;
    }
    try {
      double lat = Double.parseDouble(parts[0].trim());
      double lon = Double.parseDouble(parts[1].trim());
      return new Geolocation(lat, lon);
    } catch (IllegalArgumentException e) {
      // covers both NumberFormatException (parseDouble) and range-check failures
      return null;
    }
  }

  @Override
  public String toString() {
    return latitude + "," + longitude;
  }
}

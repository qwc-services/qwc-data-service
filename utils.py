import math

def geom_center(geomtype, coordinates):
    if geomtype.startswith("Multi"):
        return geom_center(geomtype[5:], coordinates[0])
    elif geomtype == "Point":
        return coordinates
    elif geomtype == "LineString":
        cumlengths = [0]
        for i in range(1, len(coordinates)):
            dx = coordinates[i][0] - coordinates[i - 1][0]
            dy = coordinates[i][1] - coordinates[i - 1][1]
            cumlengths.append(cumlengths[i - 1] + math.sqrt(dx * dx + dy * dy))
        halflen = 0.5 * cumlengths[-1]
        for i in range(1, len(cumlengths)):
            if cumlengths[i] > halflen:
                mu = (halflen - cumlengths[i - 1]) / (cumlengths[i] - cumlengths[i - 1])
                return [
                    coordinates[i - 1][0] + mu * (coordinates[i][0] - coordinates[i - 1][0]),
                    coordinates[i - 1][1] + mu * (coordinates[i][1] - coordinates[i - 1][1])
                ]
    elif geomtype == "Polygon":
        ring = coordinates[0]
        area = 0
        cx = cy = 0
        for i in range(0, len(ring) - 1):
            t = ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1]
            area += t
            cx += (ring[i][0] + ring[i + 1][0]) * t
            cy += (ring[i][1] + ring[i + 1][1]) * t

        return [cx / (3 * area), cy / (3 * area)]

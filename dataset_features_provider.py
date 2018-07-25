from collections import OrderedDict
import re

from flask import json
from sqlalchemy.exc import InternalError
from sqlalchemy.sql import text as sql_text


class DatasetFeaturesProvider():
    """DatasetFeaturesProvider class

    Access database to read and write features of a dataset.
    Return features as GeoJSON FeatureCollection or Feature.
    """

    def __init__(self, config, db_engine):
        """Constructor

        :param obj config: Data service config for a dataset
        :param DatabaseEngine db_engine: Database engine with DB connections
        """
        # get SQLAlchemy engine for GeoDB of dataset
        if config.get('database'):
            self.db = db_engine.db_engine(config['database'])
        else:
            # fallback to default GeoDB
            self.db = db_engine.geo_db()

        # assign values from service config
        self.table_name = "%s.%s" % (config['schema'], config['table_name'])
        self.primary_key = config['primary_key']
        # permitted attributes only
        self.attributes = config['attributes']
        self.geometry_column = config['geometry_column']
        self.geometry_type = config['geometry_type']
        self.srid = config['srid']
        # write permission
        self.writable = config['writable']

    def is_read_only(self):
        """Return whether dataset is read-only."""
        return not self.writable

    def index(self, bbox, filterexpr):
        """Find features inside bounding box.

        :param list[float] bbox: Bounding box as [<minx>,<miny>,<maxx>,<maxy>]
                                 or None for no bounding box
        :param (sql, params) filterexpr: A filter expression as a tuple
                                         (sql_expr, bind_params)
        """
        # build query SQL

        # select id and permitted attributes
        columns = (', ').join([self.primary_key] + self.attributes)

        where_clauses = []
        params = {}

        if bbox is not None:
            # bbox filter
            where_clauses.append("""
                ST_Intersects({geom},
                    ST_SetSRID(
                        'BOX3D(:minx :miny, :maxx :maxy)'::box3d, {srid}
                    )
                )
            """.format(geom=self.geometry_column, srid=self.srid))
            params.update({
                "minx": bbox[0],
                "miny": bbox[1],
                "maxx": bbox[2],
                "maxy": bbox[3]
            })

        if filterexpr is not None:
            where_clauses.append(filterexpr[0])
            params.update(filterexpr[1])

        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)

        sql = sql_text("""
            SELECT {columns},
                ST_AsGeoJSON({geom}) AS json_geom
            FROM {table}
            {where_clause};
        """.format(columns=columns, geom=self.geometry_column,
                   table=self.table_name, where_clause=where_clause))

        # connect to database and start transaction (for read-only access)
        conn = self.db.connect()
        trans = conn.begin()

        # execute query
        features = []
        result = conn.execute(sql, **params)

        for row in result:
            # NOTE: feature CRS removed by marshalling
            features.append(self.feature_from_query(row))

        # roll back transaction and close database connection
        trans.rollback()
        conn.close()

        return {
            'type': 'FeatureCollection',
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::2056'
                }
            },
            'features': features
        }

    def show(self, id):
        """Get a feature.

        :param int id: Dataset feature ID
        """
        # build query SQL

        # select id and permitted attributes
        columns = (', ').join([self.primary_key] + self.attributes)

        sql = sql_text("""
            SELECT {columns},
                ST_AsGeoJSON({geom}) AS json_geom
            FROM {table}
            WHERE {pkey} = :id
            LIMIT 1;
        """.format(columns=columns, geom=self.geometry_column,
                   table=self.table_name, pkey=self.primary_key))

        # connect to database and start transaction (for read-only access)
        conn = self.db.connect()
        trans = conn.begin()

        # execute query
        feature = None
        result = conn.execute(sql, id=id)
        for row in result:
            # NOTE: result is empty if not found
            feature = self.feature_from_query(row)

        # roll back transaction and close database connection
        trans.rollback()
        conn.close()

        return feature

    def create(self, feature):
        """Create a new feature.

        :param object feature: GeoJSON Feature
        """
        # build query SQL
        sql_params = self.sql_params_for_feature(feature)

        sql = sql_text("""
            INSERT INTO {table} ({columns})
                VALUES ({values_sql})
            RETURNING {return_columns},
                ST_AsGeoJSON({geom}) AS json_geom;
        """.format(table=self.table_name, columns=sql_params['columns'],
                   values_sql=sql_params['values_sql'],
                   return_columns=sql_params['return_columns'],
                   geom=self.geometry_column))

        # connect to database
        conn = self.db.connect()

        # execute query
        # NOTE: use bound values
        feature = None
        result = conn.execute(sql, **(sql_params['bound_values']))
        for row in result:
            feature = self.feature_from_query(row)

        # close database connection
        conn.close()

        return feature

    def update(self, id, feature):
        """Update a feature.

        :param int id: Dataset feature ID
        :param object feature: GeoJSON Feature
        """
        # build query SQL
        sql_params = self.sql_params_for_feature(feature)

        sql = sql_text("""
            UPDATE {table} SET ({columns}) =
                ({values_sql})
            WHERE {pkey} = :{pkey}
            RETURNING {return_columns},
                ST_AsGeoJSON({geom}) AS json_geom;
        """.format(table=self.table_name, columns=sql_params['columns'],
                   values_sql=sql_params['values_sql'], pkey=self.primary_key,
                   return_columns=sql_params['return_columns'],
                   geom=self.geometry_column))

        update_values = sql_params['bound_values']
        update_values[self.primary_key] = id

        # connect to database
        conn = self.db.connect()

        # execute query
        # NOTE: use bound values
        feature = None
        result = conn.execute(sql, **update_values)
        for row in result:
            # NOTE: result is empty if not found
            feature = self.feature_from_query(row)

        # close database connection
        conn.close()

        return feature

    def destroy(self, id):
        """Delete a feature.

        :param int id: Dataset feature ID
        """
        # build query SQL
        sql = sql_text("""
            DELETE FROM {table}
            WHERE {pkey} = :id
            RETURNING {pkey};
        """.format(table=self.table_name, pkey=self.primary_key))

        # connect to database
        conn = self.db.connect()

        # execute query
        success = False
        result = conn.execute(sql, id=id)
        for row in result:
            # NOTE: result is empty if not found
            success = True

        # close database connection
        conn.close()

        return success

    def parse_bbox(self, bbox):
        """Parse and validate a bounding box and return list of coordinates.

        :param str bbox: Bounding box as '<minx>,<miny>,<maxx>,<maxy>'
        """
        bbox_coords = bbox.split(',')
        if len(bbox_coords) == 4:
            try:
                # convert coordinates to numbers
                bbox_coords = [float(c) for c in bbox_coords]
                # check min <= max
                if (bbox_coords[0] <= bbox_coords[2] and
                   bbox_coords[1] <= bbox_coords[3]):
                    return bbox_coords
            except ValueError:
                # conversion failed
                pass

        # invalid bbox
        return None

    def parse_filter(self, filterexpr):
        """Parse and validate a filter expression and return a tuple
        (sql_expr, bind_params).

        :param str filterexpr: Comma-separated filter expressions as
                               '<k1> = <v1>, <k2> like <v2>, ...'
        """
        sql = []
        params = {}
        i = 0
        for expr in re.split(r"(?<!\\),", filterexpr):
            parts = [
                s.strip() for s in re.split(r"(\s*=\s*|\s+like(?i)\s+)", expr)
            ]
            if len(parts) != 3:
                # Invalid expression
                return None
            column_name = parts[0].replace(r"\,", ",")
            if column_name not in self.attributes:
                # Invalid column_name
                return None
            sql.append("%s %s :v%d" % (column_name, parts[1], i))
            params["v%d" % i] = parts[2].replace(r"\,", ",")
            i += 1
        if not sql:
            return None
        else:
            return (" AND ".join(sql), params)

    def validate(self, feature):
        """Validate a feature and return any validation errors.

        :param object feature: GeoJSON Feature
        """
        errors = OrderedDict()

        validation_errors = self.validate_geo_json(feature)
        if validation_errors:
            errors['validation_errors'] = validation_errors
        else:
            geometry_errors = self.validate_geometry(feature)
            if geometry_errors:
                errors['geometry_errors'] = geometry_errors

        return errors

    def validate_geo_json(self, feature):
        """Validate structure of GeoJSON Feature object.

        :param object feature: GeoJSON Feature
        """
        errors = []

        # validate GeoJSON
        if feature.get('type') != 'Feature':
            errors.append("GeoJSON must be of type Feature")

        # validate geometry object
        if 'geometry' not in feature:
            errors.append("Missing GeoJSON geometry")
        elif not isinstance(feature.get('geometry'), dict):
            errors.append("Invalid GeoJSON geometry")
        else:
            geo_json_geometry_types = [
                'Point',
                'MultiPoint',
                'LineString',
                'MultiLineString',
                'Polygon',
                'MultiPolygon',
                'GeometryCollection'
            ]
            geometry = feature['geometry']
            if 'type' not in geometry:
                errors.append("Missing GeoJSON geometry type")
            elif geometry.get('type') not in geo_json_geometry_types:
                errors.append("Invalid GeoJSON geometry type")
            if 'coordinates' not in geometry:
                errors.append("Missing GeoJSON geometry coordinates")
            elif not isinstance(geometry.get('coordinates'), list):
                errors.append("Invalid GeoJSON geometry coordinates")

        # validate properties
        if 'properties' not in feature:
            errors.append("Missing GeoJSON properties")
        elif not isinstance(feature.get('properties'), dict):
            errors.append("Invalid GeoJSON properties")
        else:
            # validate feature attributes
            for attr, value in feature.get('properties').items():
                if attr not in self.attributes:
                    # unknown attribute or not permitted
                    errors.append("Feature property '%s' can not be set" %
                                  attr)
                elif value is not None and not isinstance(value,
                                                          (str, int, float)):
                    errors.append("Invalid type for feature property '%s'" %
                                  attr)

        # validate CRS
        if 'crs' not in feature:
            errors.append("Missing GeoJSON CRS")
        elif not isinstance(feature.get('crs'), dict):
            errors.append("Invalid GeoJSON CRS")
        else:
            crs = feature['crs']
            if crs.get('type') != 'name':
                errors.append("GeoJSON CRS must be of type 'name'")
            if 'properties' not in crs:
                errors.append("Missing GeoJSON CRS properties")
            elif not isinstance(crs.get('properties'), dict):
                errors.append("Invalid GeoJSON CRS properties")
            elif not re.match(r'^urn:ogc:def:crs:EPSG::\d{1,6}$',
                              str(crs['properties'].get('name'))):
                errors.append("GeoJSON CRS is not an OGC CRS URN "
                              "(e.g. 'urn:ogc:def:crs:EPSG::4326')")

        return errors

    def validate_geometry(self, feature):
        """Validate geometry contents using PostGIS.

        :param object feature: GeoJSON Feature
        """
        errors = []

        json_geom = json.dumps(feature.get('geometry'))

        # connect to database and start transaction (for read-only access)
        conn = self.db.connect()
        trans = conn.begin()

        # validate GeoJSON geometry
        try:
            sql = sql_text("SELECT ST_GeomFromGeoJSON(:geom);")
            conn.execute(sql, geom=json_geom)
        except InternalError as e:
            # PostGIS error, e.g. "Too few ordinates in GeoJSON"
            errors.append({
                'reason': re.sub(r'^FEHLER:\s*', '', str(e.orig)).strip()
            })

        if not errors:
            # validate geometry
            wkt_geom = ""
            sql = sql_text("""
                WITH feature AS (SELECT ST_GeomFromGeoJSON(:geom) AS geom)
                SELECT valid, reason, ST_AsText(location) AS location,
                    ST_IsEmpty(geom) as is_empty, ST_AsText(geom) AS wkt_geom,
                    GeometryType(geom) AS geom_type
                FROM feature, ST_IsValidDetail(geom)
            """)
            result = conn.execute(sql, geom=json_geom)
            for row in result:
                if not row['valid']:
                    error = {
                        'reason': row['reason']
                    }
                    if row['location'] is not None:
                        error['location'] = row['location']
                    errors.append(error)
                elif row['is_empty']:
                    errors.append({'reason': "Empty or incomplete geometry"})

                wkt_geom = row['wkt_geom']
                geom_type = row['geom_type']

        if not errors:
            # check WKT for repeated vertices
            groups = re.findall(r'(?<=\()([\d\.,\s]+)(?=\))', wkt_geom)
            for group in groups:
                vertices = group.split(',')
                for i, v in enumerate(vertices):
                    if i > 0 and vertices[i-1] == v:
                        errors.append({
                            'reason': "Duplicated point",
                            'location': 'POINT(%s)' % v
                        })

        if not errors:
            # validate geometry type
            if (self.geometry_type != 'Geometry' and
               geom_type != self.geometry_type):
                errors.append({
                    'reason': "Invalid geometry type: %s is not a %s" %
                              (geom_type, self.geometry_type)
                })

        # roll back transaction and close database connection
        trans.rollback()
        conn.close()

        return errors

    def feature_from_query(self, row):
        """Build GeoJSON Feature from query result row.

        :param obj row: Row result from query
        """
        props = OrderedDict()
        for attr in self.attributes:
            props[attr] = row[attr]

        return {
            'type': 'Feature',
            'id': row[self.primary_key],
            'geometry': json.loads(row['json_geom']),
            'properties': props,
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::%d' % self.srid
                }
            }
        }

    def sql_params_for_feature(self, feature):
        """Build SQL fragments and values for feature INSERT or UPDATE.

        :param object feature: GeoJSON Feature
        """
        # get permitted attribute values
        bound_values = OrderedDict()
        for attr in self.attributes:
            if attr in feature['properties']:
                bound_values[attr] = feature['properties'][attr]
        attribute_columns = list(bound_values.keys())

        # get geometry value as GeoJSON string
        bound_values[self.geometry_column] = json.dumps(feature['geometry'])

        # columns for permitted attributes and geometry
        columns = (', ').join(attribute_columns + [self.geometry_column])

        # use bound parameters for attribute values and geometry
        # e.g. ['name'] + 'geom'
        #     ==>
        #      ":name, ST_SetSRID(ST_GeomFromGeoJSON(:geom), 2056)"
        bound_columns = [":%s" % attr for attr in attribute_columns]
        # build geometry from GeoJSON
        geometry_value = "ST_SetSRID(ST_GeomFromGeoJSON(:{geom}), {srid})"
        geometry_value = geometry_value.format(geom=self.geometry_column,
                                               srid=self.srid)
        values_sql = (', ').join(bound_columns + [geometry_value])

        # return id and permitted attributes
        return_columns = (', ').join([self.primary_key] + self.attributes)

        return {
            'columns': columns,
            'values_sql': values_sql,
            'return_columns': return_columns,
            'bound_values': bound_values
        }

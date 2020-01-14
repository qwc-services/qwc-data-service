from collections import OrderedDict
import re

from flask import json
from sqlalchemy.exc import DataError, InternalError, ProgrammingError
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
        # field constraints
        self.fields = config.get('fields', {})
        self.geometry_column = config['geometry_column']
        self.geometry_type = config['geometry_type']
        self.srid = config['srid']
        # write permission
        self.writable = config['writable']
        # CRUD permissions
        self.__creatable = config.get('creatable', self.writable)
        self.__readable = config.get('readable', True)
        self.__updatable = config.get('updatable', self.writable)
        self.__deletable = config.get('deletable', self.writable)

    def creatable(self):
        """Return whether dataset can be created."""
        return self.__creatable

    def readable(self):
        """Return whether dataset can be read."""
        return self.__readable

    def updatable(self):
        """Return whether dataset can be updated."""
        return self.__updatable

    def deletable(self):
        """Return whether dataset can be deleted."""
        return self.__deletable

    def index(self, bbox, client_srid, filterexpr):
        """Find features inside bounding box.

        :param list[float] bbox: Bounding box as [<minx>,<miny>,<maxx>,<maxy>]
                                 or None for no bounding box
        :param int client_srid: Client SRID or None for dataset SRID
        :param (sql, params) filterexpr: A filter expression as a tuple
                                         (sql_expr, bind_params)
        """
        srid = client_srid or self.srid

        # build query SQL

        # select id and permitted attributes
        columns = (', ').join([self.primary_key] + self.attributes)

        where_clauses = []
        params = {}

        if bbox is not None:
            # bbox filter
            bbox_geom_sql = self.transform_geom_sql("""
                ST_SetSRID(
                    'BOX3D(:minx :miny, :maxx :maxy)'::box3d,
                    {bbox_srid}
                )
            """, srid, self.srid)
            where_clauses.append(("""
                ST_Intersects({geom},
                    %s
                )
            """ % bbox_geom_sql).format(
                geom=self.geometry_column, bbox_srid=srid,
                srid=self.srid
            ))
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

        geom_sql = self.transform_geom_sql("{geom}", self.srid, srid)
        sql = sql_text(("""
            SELECT {columns},
                ST_AsGeoJSON(%s) AS json_geom
            FROM {table}
            {where_clause};
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table_name,
            where_clause=where_clause
        ))

        # connect to database and start transaction (for read-only access)
        conn = self.db.connect()
        trans = conn.begin()

        # execute query
        features = []
        result = conn.execute(sql, **params)

        for row in result:
            # NOTE: feature CRS removed by marshalling
            features.append(self.feature_from_query(row, srid))

        # roll back transaction and close database connection
        trans.rollback()
        conn.close()

        return {
            'type': 'FeatureCollection',
            'crs': {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::%s' % srid
                }
            },
            'features': features
        }

    def show(self, id, client_srid):
        """Get a feature.

        :param int id: Dataset feature ID
        :param int client_srid: Client SRID or None for dataset SRID
        """
        srid = client_srid or self.srid

        # build query SQL

        # select id and permitted attributes
        columns = (', ').join([self.primary_key] + self.attributes)

        geom_sql = self.transform_geom_sql("{geom}", self.srid, srid)
        sql = sql_text(("""
            SELECT {columns},
                ST_AsGeoJSON(%s) AS json_geom
            FROM {table}
            WHERE {pkey} = :id
            LIMIT 1;
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table_name,
            pkey=self.primary_key
        ))

        # connect to database and start transaction (for read-only access)
        conn = self.db.connect()
        trans = conn.begin()

        # execute query
        feature = None
        result = conn.execute(sql, id=id)
        for row in result:
            # NOTE: result is empty if not found
            feature = self.feature_from_query(row, srid)

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
        srid = sql_params['client_srid']

        geom_sql = self.transform_geom_sql("{geom}", self.srid, srid)
        sql = sql_text(("""
            INSERT INTO {table} ({columns})
                VALUES ({values_sql})
            RETURNING {return_columns},
                ST_AsGeoJSON(%s) AS json_geom;
        """ % geom_sql).format(
            table=self.table_name, columns=sql_params['columns'],
            values_sql=sql_params['values_sql'],
            return_columns=sql_params['return_columns'],
            geom=self.geometry_column
        ))

        # connect to database
        conn = self.db.connect()

        # execute query
        # NOTE: use bound values
        feature = None
        result = conn.execute(sql, **(sql_params['bound_values']))
        for row in result:
            feature = self.feature_from_query(row, srid)

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
        srid = sql_params['client_srid']

        geom_sql = self.transform_geom_sql("{geom}", self.srid, srid)
        sql = sql_text(("""
            UPDATE {table} SET ({columns}) =
                ({values_sql})
            WHERE {pkey} = :{pkey}
            RETURNING {return_columns},
                ST_AsGeoJSON(%s) AS json_geom;
        """ % geom_sql).format(
            table=self.table_name, columns=sql_params['columns'],
            values_sql=sql_params['values_sql'], pkey=self.primary_key,
            return_columns=sql_params['return_columns'],
            geom=self.geometry_column
        ))

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
            feature = self.feature_from_query(row, srid)

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

    def parse_crs(self, crs):
        """Parse and validate a CRS and return its SRID.

        :param str crs: Coordinate reference system as 'EPSG:<srid>'
        """
        if crs.startswith('EPSG:'):
            try:
                # extract SRID
                srid = int(crs.split(':')[1])
                return srid
            except ValueError:
                # conversion failed
                pass

        # invalid CRS
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
            else:
                fields_errors = self.validate_fields(feature)
                if fields_errors:
                    errors['data_errors'] = fields_errors

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

    def validate_fields(self, feature):
        """Validate data types and constraints of GeoJSON Feature properties.

        :param object feature: GeoJSON Feature
        """
        errors = []

        if not self.fields:
            # skip validation if fields metadata is empty
            return errors

        # connect to database
        conn = self.db.connect()

        for attr in feature['properties']:
            constraints = self.fields.get(attr, {}).get('constraints', {})
            data_type = self.fields.get(attr, {}).get('data_type')
            if data_type == 'numeric':
                data_type = 'numeric(%d,%d)' % (
                    constraints.get('numeric_precision', 1),
                    constraints.get('numeric_scale', 0)
                )
            elif data_type == 'bigint':
                # parse bigint constraints from string
                if 'min' in constraints:
                    constraints['min'] = int(constraints['min'])
                if 'max' in constraints:
                    constraints['max'] = int(constraints['max'])

            input_value = feature['properties'][attr]
            value = None

            # readOnly
            if constraints.get('readOnly', False):
                # skip read-only fields and remove below
                continue

            # validate data type

            # start transaction (for read-only access)
            trans = conn.begin()

            try:
                # try to parse value on DB
                sql = sql_text("SELECT (:value):: %s AS value;" % data_type)
                result = conn.execute(sql, value=input_value)
                for row in result:
                    value = row['value']
            except (DataError, ProgrammingError) as e:
                # NOTE: current transaction is aborted
                errors.append("Invalid value for '%s' for type %s" %
                              (attr, data_type))
            finally:
                # roll back transaction
                trans.rollback()

            if value is None:
                # invalid value type
                continue

            if data_type == 'boolean' and type(input_value) is int:
                # prevent 'column "..." is of type boolean but expression is of
                #          type integer'
                errors.append("Invalid value for '%s' for type %s" %
                              (attr, data_type))
                continue

            # validate constraints

            # maxlength
            maxlength = constraints.get('maxlength')
            if maxlength is not None and len(str(value)) > int(maxlength):
                errors.append(
                    "Value for '%s' must be shorter than %d characters" %
                    (attr, maxlength)
                )

            # min
            minimum = constraints.get('min')
            if minimum is not None and value < minimum:
                errors.append(
                    "Value for '%s' must be greater than or equal to %s" %
                    (attr, minimum)
                )

            # max
            maximum = constraints.get('max')
            if maximum is not None and value > maximum:
                errors.append(
                    "Value for '%s' must be less than or equal to %s" %
                    (attr, maximum)
                )

            # values
            values = constraints.get('values', {})
            if values and str(value) not in [v['value'] for v in values]:
                errors.append("Invalid value for '%s'" % (attr))

        # close database connection
        conn.close()

        # remove read-only properties and check required values
        for attr in self.fields:
            constraints = self.fields.get(attr, {}).get('constraints', {})
            if constraints.get('readOnly', False):
                if attr in feature['properties']:
                    # remove read-only property from feature
                    feature['properties'].pop(attr, None)
            elif constraints.get('required', False):
                # check if required value is present and not blank
                if attr not in feature['properties']:
                    errors.append("Missing required value for '%s'" % (attr))
                elif feature['properties'][attr] is None:
                    errors.append("Missing required value for '%s'" % (attr))
                elif feature['properties'][attr] == "":
                    errors.append("Value for '%s' can not be blank" % (attr))

        return errors

    def transform_geom_sql(self, geom_sql, geom_srid, target_srid):
        """Generate SQL fragment for transforming input geometry geom_sql
        from geom_srid to target_srid.

        :param str geom_sql: SQL fragment for input geometry
        :param str geom_srid: SRID of input geometry
        :param str target_srid: target SRID
        """
        if geom_sql is None or geom_srid is None or geom_srid == target_srid:
            # no transformation
            pass
        else:
            # transform to target SRID
            geom_sql = "ST_Transform(%s, %s)" % (geom_sql, target_srid)

        return geom_sql

    def feature_from_query(self, row, client_srid):
        """Build GeoJSON Feature from query result row.

        :param obj row: Row result from query
        :param int client_srid: Client SRID or None for dataset SRID
        """
        srid = client_srid or self.srid

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
                    'name': 'urn:ogc:def:crs:EPSG::%d' % srid
                }
            }
        }

    def sql_params_for_feature(self, feature):
        """Build SQL fragments and values for feature INSERT or UPDATE and
        get client SRID from GeoJSON CRS.

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

        # get client SRID from GeoJSON CRS
        if 'crs' not in feature:
            srid = self.srid
        else:
            srid = feature['crs']['properties']['name'].split(':')[-1]
            if srid == 'CRS84':
                # use EPSG:4326 for 'urn:ogc:def:crs:OGC:1.3:CRS84'
                srid = 4326
            else:
                srid = int(srid)

        # columns for permitted attributes and geometry
        columns = (', ').join(attribute_columns + [self.geometry_column])

        # use bound parameters for attribute values and geometry
        # e.g. ['name'] + 'geom'
        #     ==>
        #      ":name, ST_SetSRID(ST_GeomFromGeoJSON(:geom), 2056)"
        bound_columns = [":%s" % attr for attr in attribute_columns]
        # build geometry from GeoJSON, transformed to dataset CRS
        geometry_value = self.transform_geom_sql(
            "ST_SetSRID(ST_GeomFromGeoJSON(:{geom}), {srid})", srid, self.srid
        ).format(geom=self.geometry_column, srid=srid)
        values_sql = (', ').join(bound_columns + [geometry_value])

        # return id and permitted attributes
        return_columns = (', ').join([self.primary_key] + self.attributes)

        return {
            'columns': columns,
            'values_sql': values_sql,
            'return_columns': return_columns,
            'bound_values': bound_values,
            'client_srid': srid
        }

from collections import OrderedDict
import re
from json.decoder import JSONDecodeError

from flask import json
from sqlalchemy.exc import DataError, InternalError, ProgrammingError
from sqlalchemy.sql import text as sql_text


class DatasetFeaturesProvider():
    """DatasetFeaturesProvider class

    Access database to read and write features of a dataset.
    Return features as GeoJSON FeatureCollection or Feature.
    """

    def __init__(self, config, db_engine, logger):
        """Constructor

        :param obj config: Data service config for a dataset
        :param DatabaseEngine db_engine: Database engine with DB connections
        :param Logger logger: Application logger
        """
        # get SQLAlchemy engine for GeoDB of dataset for read actions
        if config.get('database_read'):
            self.db_read = db_engine.db_engine(config['database_read'])
        else:
            # fallback to default GeoDB
            self.db_read = db_engine.geo_db()

        # get SQLAlchemy engine for GeoDB of dataset for write actions
        if config.get('database_write'):
            self.db_write = db_engine.db_engine(config['database_write'])
        else:
            # fallback to GeoDB for read actions
            self.db_write = self.db_read

        self.logger = logger

        # assign values from service config
        self.schema = config['schema']
        self.table_name = config['table_name']
        self.table = '"%s"."%s"' % (self.schema, self.table_name)
        self.primary_key = config['primary_key']
        # permitted attributes only
        self.attributes = config['attributes']
        # field constraints
        self.fields = config.get('fields', {})
        # NOTE: geometry_column is None for datasets without geometry
        self.geometry_column = config['geometry_column']
        self.geometry_type = config['geometry_type']
        self.srid = config['srid']
        self.allow_null_geometry = config['allow_null_geometry']
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
        columns = (', ').join(
            self.escape_column_names([self.primary_key] + self.attributes)
        )

        where_clauses = []
        params = {}

        if self.geometry_column and bbox is not None:
            # bbox filter
            bbox_geom_sql = self.transform_geom_sql("""
                ST_SetSRID(
                    'BOX3D(:minx :miny, :maxx :maxy)'::box3d,
                    {bbox_srid}
                )
            """, srid, self.srid)
            where_clauses.append(("""
                ST_Intersects("{geom}",
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

        geom_sql = self.geom_column_sql(srid, with_bbox=False)
        if self.geometry_column:
            # select overall extent
            geom_sql += (
                ', ST_Extent(%s) OVER () AS _overall_bbox_' %
                self.transform_geom_sql('"{geom}"', self.srid, srid)
            )

        sql = sql_text(("""
            SELECT {columns}%s
            FROM {table}
            {where_clause};
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table,
            where_clause=where_clause
        ))

        # connect to database and start transaction (for read-only access)
        conn = self.db_read.connect()
        trans = conn.begin()

        # execute query
        features = []
        result = conn.execute(sql, **params)

        overall_bbox = None
        for row in result:
            # NOTE: feature CRS removed by marshalling
            features.append(self.feature_from_query(row, srid))
            if '_overall_bbox_' in row:
                overall_bbox = row['_overall_bbox_']

        # roll back transaction and close database connection
        trans.rollback()
        conn.close()

        crs = None
        if self.geometry_column:
            crs = {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::%s' % srid
                }
            }
            if overall_bbox:
                overall_bbox = self.parse_box2d(overall_bbox)

        return {
            'type': 'FeatureCollection',
            'features': features,
            'crs': crs,
            'bbox': overall_bbox
        }

    def show(self, id, client_srid):
        """Get a feature.

        :param int id: Dataset feature ID
        :param int client_srid: Client SRID or None for dataset SRID
        """
        srid = client_srid or self.srid

        # build query SQL

        # select id and permitted attributes
        columns = (', ').join(
            self.escape_column_names([self.primary_key] + self.attributes)
        )

        geom_sql = self.geom_column_sql(srid)
        sql = sql_text(("""
            SELECT {columns}%s
            FROM {table}
            WHERE {pkey} = :id
            LIMIT 1;
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table,
            pkey=self.primary_key
        ))

        # connect to database and start transaction (for read-only access)
        conn = self.db_read.connect()
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
        # connect to database
        conn = self.db_write.connect()

        # build query SQL
        sql_params = self.sql_params_for_feature(feature, conn)
        srid = sql_params['client_srid']

        geom_sql = self.geom_column_sql(srid)
        sql = sql_text(("""
            INSERT INTO {table} ({columns})
                VALUES ({values_sql})
            RETURNING {return_columns}%s;
        """ % geom_sql).format(
            table=self.table, columns=sql_params['columns'],
            values_sql=sql_params['values_sql'],
            return_columns=sql_params['return_columns'],
            geom=self.geometry_column
        ))

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
        # connect to database
        conn = self.db_write.connect()

        # build query SQL
        sql_params = self.sql_params_for_feature(feature, conn)
        srid = sql_params['client_srid']

        geom_sql = self.geom_column_sql(srid)
        sql = sql_text(("""
            UPDATE {table} SET ({columns}) =
                ({values_sql})
            WHERE {pkey} = :{pkey}
            RETURNING {return_columns}%s;
        """ % geom_sql).format(
            table=self.table, columns=sql_params['columns'],
            values_sql=sql_params['values_sql'], pkey=self.primary_key,
            return_columns=sql_params['return_columns'],
            geom=self.geometry_column
        ))

        update_values = sql_params['bound_values']
        update_values[self.primary_key] = id

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
            WHERE "{pkey}" = :id
            RETURNING "{pkey}";
        """.format(table=self.table, pkey=self.primary_key))

        # connect to database
        conn = self.db_write.connect()

        # execute query
        success = False
        result = conn.execute(sql, id=id)
        for row in result:
            # NOTE: result is empty if not found
            success = True

        # close database connection
        conn.close()

        return success

    def exists(self, id):
        """Check if a feature exists.
        :param int id: Dataset feature ID
        """
        sql = sql_text(("""
            SELECT EXISTS(SELECT 1 FROM {table} WHERE {pkey}=:id)
        """).format(
            table=self.table, pkey=self.primary_key
        ))

        # connect to database
        conn = self.db_read.connect()

        # execute query
        result = conn.execute(sql, id=id)
        exists = result.fetchone()[0]

        # close database connection
        conn.close()

        return exists

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

        :param str filterexpr: JSON serialized array of filter expressions:
        [["<attr>", "<op>", "<value>"], "and|or", ["<attr>", "<op>", "<value>"]]
        """
        if not filterexpr:
            return (None, "Empty expression")
        try:
            filterarray = json.loads(filterexpr)
        except JSONDecodeError as e:
            return (None, "Invalid JSON")
        if type(filterarray) is not list:
            return (None, "Not an array")

        CONCAT_OPERATORS = ["AND", "OR"]
        OPERATORS = [
            "=", "!=", "<>", "<", ">", "<=", ">=",
            "LIKE", "ILIKE",
            "IS", "IS NOT"
        ]
        VALUE_TYPES = [int, float, str, type(None)]

        sql = []
        params = {}
        i = 0
        for entry in filterarray:
            if type(entry) is str:
                entry = entry.upper()
                if entry not in CONCAT_OPERATORS:
                    return (
                        None, "Invalid concatenation operator '%s'" % entry)
                if i % 2 != 1 or i == len(filterarray) - 1:
                    # filter concatenation operators must be at odd-numbered
                    # positions in the array and cannot appear last
                    return (
                        None,
                        "Incorrect concatenation operator position for '%s'" %
                        entry
                    )
                sql.append(entry)
            elif type(entry) is list:
                if len(entry) != 3:
                    # filter entry must have exactly three parts
                    return (None, "Incorrect number of entries in %s" % entry)

                # column
                column_name = entry[0]
                if type(column_name) is not str:
                    return (None, "Invalid column name in %s" % entry)

                if (
                    column_name != self.primary_key
                    and column_name not in self.attributes
                ):
                    # column not available or not permitted
                    return (
                        None,
                        "Column name not found or permission error in %s" %
                        entry
                    )

                # operator
                op = entry[1].upper().strip()
                if type(entry[1]) is not str or op not in OPERATORS:
                    return (None, "Invalid operator in %s" % entry)

                # value
                value = entry[2]
                if type(value) not in VALUE_TYPES:
                    return (None, "Invalid value type in %s" % entry)

                if value is None:
                    # modify operator for NULL value
                    if op == "=":
                        op = "IS"
                    elif op == "!=":
                        op = "IS NOT"
                elif op in ["IS", "IS NOT"]:
                    return (None, "Invalid operator in %s" % entry)

                # add SQL fragment for filter
                # e.g. '"type" >= :v0'
                sql.append('"%s" %s :v%d' % (column_name, op, i))
                # add value
                params["v%d" % i] = value
            else:
                # invalid entry
                return (None, "%s" % entry)

            i += 1

        if not sql:
            return (None, "Empty expression")
        else:
            return ("(%s)" % " ".join(sql), params)

    def parse_box2d(self, box2d):
        """Parse Box2D string and return bounding box
        as [<minx>,<miny>,<maxx>,<maxy>].

        :param str box2d: Box2D string
        """
        bbox = None

        if box2d is None:
            # bounding box is empty
            return None

        # extract coords from Box2D string
        # e.g. "BOX(950598.12 6003950.34,950758.567 6004010.8)"
        # truncate brackets and split into coord pairs
        parts = box2d[4:-1].split(',')
        if len(parts) == 2:
            # split coords, e.g. "950598.12 6003950.34"
            minx, miny = parts[0].split(' ')
            maxx, maxy = parts[1].split(' ')
            bbox = [float(minx), float(miny), float(maxx), float(maxy)]

        return bbox

    def validate(self, feature, new_feature=False):
        """Validate a feature and return any validation errors.

        :param object feature: GeoJSON Feature
        :param bool new_feature: Set if this is a new feature
        """
        errors = OrderedDict()

        validation_errors = self.validate_geo_json(feature, new_feature)
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

    def validate_geo_json(self, feature, new_feature):
        """Validate structure of GeoJSON Feature object.

        :param object feature: GeoJSON Feature
        :param bool new_feature: Set if this is a new feature
        """
        errors = []

        # validate GeoJSON
        if feature.get('type') != 'Feature':
            errors.append("GeoJSON must be of type Feature")

        crs_required = True

        # validate geometry object
        if not self.geometry_column:
            # skip geometry validation for datasets without geometry
            crs_required = False
        elif 'geometry' not in feature:
            if new_feature and not self.allow_null_geometry:
                # geometry required on create,
                # unless NULL geometries are allowed
                errors.append("Missing GeoJSON geometry")
            else:
                # geometry always optional on update
                crs_required = False
        elif feature.get('geometry') is None:
            if not self.allow_null_geometry:
                errors.append("Geometry may not be NULL")
            else:
                # geometry is NULL
                crs_required = False
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
                elif value is not None:
                    data_type = self.fields.get(attr, {}).get('data_type')
                    # NOTE: allow any data type for fields of type json
                    if (
                        data_type not in ['json', 'jsonb']
                        and not isinstance(value, (str, int, float, bool))
                    ):
                        errors.append(
                            "Invalid type for feature property '%s'" % attr
                        )

        # validate CRS
        if not self.geometry_column:
            # skip CRS validation for datasets without geometry
            pass
        elif 'crs' not in feature:
            # CRS not required if geometry is omitted or NULL
            if crs_required:
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

        if not self.geometry_column:
            # skip geometry validation for dataset without geometry
            return []
        elif feature.get('geometry') is None:
            # skip geometry validation if geometry is omitted or NULL
            return []

        json_geom = json.dumps(feature.get('geometry'))

        # connect to database and start transaction (for read-only access)
        conn = self.db_read.connect()
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
        conn = self.db_read.connect()

        for attr in feature['properties']:
            constraints = self.fields.get(attr, {}).get('constraints', {})
            data_type = self.fields.get(attr, {}).get('data_type')
            input_value = feature['properties'][attr]
            value = None

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
            elif data_type in ['json', 'jsonb']:
                # convert values for fields of type json to string
                input_value = json.dumps(input_value)
            elif data_type == 'file':
                data_type = 'text'

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
            if values and str(value) not in [str(v['value']) for v in values]:
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

    def escape_column_names(self, columns):
        """Return escaped column names by converting them to
        quoted identifiers.

        :param list(str) columns: Column names
        """
        return [
            '"%s"' % column for column in columns
        ]

    def geom_column_sql(self, srid, with_bbox=True):
        """Generate SQL fragment for GeoJSON of transformed geometry
        as additional GeoJSON column 'json_geom' and optional Box2D '_bbox_',
        or empty string if dataset has no geometry.

        :param str target_srid: Target SRID
        :param bool with_bbox: Whether to add bounding boxes for each feature
                               (default: True)
        """
        geom_sql = ""

        if self.geometry_column:
            transform_geom_sql = self.transform_geom_sql(
                '"{geom}"', self.srid, srid
            )
            # add GeoJSON column
            geom_sql = ", ST_AsGeoJSON(ST_CurveToLine(%s)) AS json_geom" \
                       % transform_geom_sql
            if with_bbox:
                # add Box2D column
                geom_sql += ", Box2D(%s) AS _bbox_" % transform_geom_sql

        return geom_sql

    def transform_geom_sql(self, geom_sql, geom_srid, target_srid):
        """Generate SQL fragment for transforming input geometry geom_sql
        from geom_srid to target_srid.

        :param str geom_sql: SQL fragment for input geometry
        :param str geom_srid: SRID of input geometry
        :param str target_srid: Target SRID
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
        props = OrderedDict()
        for attr in self.attributes:
            only_if_exists = self.fields.get(attr, {}).get('only_if_exists', False)
            if only_if_exists and not attr in row:
                continue
            props[attr] = row[attr]

        geometry = None
        crs = None
        bbox = None
        if self.geometry_column:
            if row['json_geom'] is not None:
                geometry = json.loads(row['json_geom'])
            else:
                # geometry is NULL
                geometry = None
            srid = client_srid or self.srid
            crs = {
                'type': 'name',
                'properties': {
                    'name': 'urn:ogc:def:crs:EPSG::%d' % srid
                }
            }
            if '_bbox_' in row:
                bbox = self.parse_box2d(row['_bbox_'])

        return {
            'type': 'Feature',
            'id': row[self.primary_key],
            'properties': props,
            'geometry': geometry,
            'crs': crs,
            'bbox': bbox
        }

    def sql_params_for_feature(self, feature, conn):
        """Build SQL fragments and values for feature INSERT or UPDATE and
        get client SRID from GeoJSON CRS.

        :param object feature: GeoJSON Feature
        """

        # get permitted attribute values
        bound_values = OrderedDict()
        attribute_columns = []
        return_columns = list(self.attributes)
        placeholdercount = 0

        for attr in self.attributes:
            if attr in feature['properties']:
                only_if_exists = self.fields.get(attr, {}).get('only_if_exists', False)
                if only_if_exists:
                    sql = sql_text(("""
                        SELECT EXISTS (SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema='{schema}' AND table_name='{table}' AND column_name='{column}');
                    """).format(
                        schema=self.schema, table=self.table_name, column=attr
                    ))
                    result = conn.execute(sql)
                    exists = result.fetchone()[0]
                    if not exists:
                        return_columns.remove(attr)
                        self.logger.info("Skipping field %s which does not exist in %s.%s" % (attr, self.schema, self.table_name))
                        continue


                data_type = self.fields.get(attr, {}).get('data_type')
                attribute_columns.append(attr)
                placeholder_name = "__val%d" % placeholdercount
                placeholdercount += 1
                if data_type in ['json', 'jsonb']:
                    # convert values for fields of type json to string
                    bound_values[placeholder_name] = json.dumps(
                        feature['properties'][attr]
                    )
                else:
                    bound_values[placeholder_name] = feature['properties'][attr]

        placeholder_names = list(bound_values.keys())

        # columns for permitted attributes
        columns = (', ').join(self.escape_column_names(attribute_columns))

        srid = None
        if self.geometry_column:
            if 'geometry' in feature:
                if feature['geometry'] is not None:
                    # get geometry value as GeoJSON string
                    bound_values[self.geometry_column] = json.dumps(
                        feature['geometry']
                    )
                else:
                    # geometry is NULL
                    bound_values[self.geometry_column] = None

                # columns for permitted attributes and geometry
                columns = (', ').join(
                    self.escape_column_names(
                        attribute_columns + [self.geometry_column]
                    )
                )

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

        # use bound parameters for attribute values and geometry
        # e.g. ['name'] + 'geom'
        #     ==>
        #      ":name, ST_SetSRID(ST_GeomFromGeoJSON(:geom), 2056)"
        bound_columns = [":%s" % placeholder_name for placeholder_name in placeholder_names]
        if self.geometry_column and 'geometry' in feature:
            # build geometry from GeoJSON, transformed to dataset CRS
            geometry_value = self.transform_geom_sql(
                "ST_SetSRID(ST_GeomFromGeoJSON(:{geom}), {srid})", srid,
                self.srid
            ).format(geom=self.geometry_column, srid=srid)
            bound_columns += [geometry_value]
        values_sql = (', ').join(bound_columns)

        # return id and permitted attributes
        return_columns = (', ').join(
            self.escape_column_names([self.primary_key] + return_columns)
        )

        return {
            'columns': columns,
            'values_sql': values_sql,
            'return_columns': return_columns,
            'bound_values': bound_values,
            'client_srid': srid
        }

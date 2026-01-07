import ast
from collections import OrderedDict
import re
from json.decoder import JSONDecodeError
from datetime import date
from decimal import Decimal
from uuid import UUID

from flask import json
from sqlalchemy.exc import DataError, InternalError, ProgrammingError
from sqlalchemy.sql import text as sql_text


class DatasetFeaturesProvider():
    """DatasetFeaturesProvider class

    Access database to read and write features of a dataset.
    Return features as GeoJSON FeatureCollection or Feature.
    """

    def __init__(self, config, db_engine, logger, translator):
        """Constructor

        :param obj config: Data service config for a dataset
        :param DatabaseEngine db_engine: Database engine with DB connections
        :param Logger logger: Application logger
        :param obj translator: Translator
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

        self.datasource_filter = config.get('datasource_filter', None)
        self.db_engine = db_engine
        self.logger = logger
        self.translator = translator

        # assign values from service config
        self.schema = config['schema']
        self.table_name = config['table_name']
        self.table = '"%s"."%s"' % (self.schema, self.table_name)
        self.primary_key = config['primary_key']
        # permitted attributes only
        self.attributes = config['attributes']
        # field constraints
        self.fields = config.get('fields', {})
        self.jointables = config['jointables']
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

    def index(self, bbox, client_srid, filterexpr, filter_geom, filter_fields, limit=None, offset=None, sortby=None):
        """Find features inside bounding box.

        :param list[float] bbox: Bounding box as [<minx>,<miny>,<maxx>,<maxy>]
                                 or None for no bounding box
        :param int client_srid: Client SRID or None for dataset SRID
        :param (sql, params) filterexpr: A filter expression as a tuple
                                         (sql_expr, bind_params)
        :param str filter_geom: JSON serialized GeoJSON geometry
        :param list[string] filter_fields: Field names to return
        :params number limit: Feature count limit
        :params number offset: Feature count offset
        :params sortby string: Feature sort order by fieldnames
        """
        srid = client_srid or self.srid

        attributes, join_query = self.__prepare_join_query(filter_fields)

        # build query SQL

        # select columns list
        columns = (', ').join(self.escape_column_names(attributes))

        where_clauses = []
        params = {}

        if self.datasource_filter:
            where_clauses.append(self.datasource_filter)

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

        if filterexpr is not None and filterexpr[0]:
            where_clauses.append(filterexpr[0])
            params.update(filterexpr[1])

        if filter_geom is not None:
            where_clauses.append("ST_Intersects(%s, ST_GeomFromGeoJSON(:filter_geom))" % self.geometry_column)
            params.update({"filter_geom": filter_geom})

        where_clause = ""
        if where_clauses:
            where_clause = "WHERE (" + ") AND (".join(where_clauses) + ")"

        order_clause = ""
        if sortby:
            order_clause_fragments = []
            for field in sortby.split(","):
                order = "ASC"
                if field.startswith("-"):
                    order = "DESC"
                    field = field[1:]
                elif field.startswith("+"):
                    field = field[1:]
                if field == "<id>":
                    field = self.primary_key

                if (
                    field == self.primary_key
                    or field in self.attributes
                ):
                    order_clause_fragments.append('"%s" %s' % (field, order))
                else:
                    self.logger.debug("Omitting non-existing sort columng %s" % field)
            if order_clause_fragments:
                order_clause = "ORDER BY %s" % ",".join(order_clause_fragments)

        geom_sql = ""
        if not filter_fields or "geometry" in filter_fields:
            geom_sql = self.geom_column_sql(srid, with_bbox=False)
            if self.geometry_column:
                # select overall extent
                geom_sql += (
                    ', ST_Extent(%s) OVER () AS _overall_bbox_' %
                    self.transform_geom_sql('"{geom}"', self.srid, srid)
                )

        sql = sql_text(("""
            SELECT {columns}%s
            FROM {table} __J0
            {join_query}
            {where_clause}
            {order_clause};
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table,
            join_query=join_query, where_clause=where_clause, order_clause=order_clause
        ))

        self.logger.debug(f"index query: {sql}")
        self.logger.debug(f"params: {params}")

        # Subset range
        start = offset or 0
        end = (offset + limit) if limit else None

        features = []
        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # execute query
            result = conn.execute(sql, params).mappings()

            overall_bbox = None
            for (idx, row) in enumerate(result):
                if idx < start or (end is not None and idx >= end):
                    # Just append dummy feature
                    features.append({})
                    continue

                features.append(self.feature_from_query(dict(row), srid, filter_fields))
                if '_overall_bbox_' in row:
                    overall_bbox = row['_overall_bbox_']

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

        features_subset = features[start:end]
        return {
            'type': 'FeatureCollection',
            'features': features_subset,
            'crs': crs,
            'bbox': overall_bbox,
            'numberMatched': len(features),
            'numberReturned': len(features_subset)
        }

    def keyvals(self, key, value):
        """ Get key-value pairs.

        :param key: The key column name
        :param value: The value column name
        """

        where_clause = ""
        if self.datasource_filter:
            where_clause = "WHERE " + self.datasource_filter

        columns = (', ').join(
            self.escape_column_names([key, value])
        )
        sql = sql_text(("""
            SELECT {columns}
            FROM {table}
            {where_clause};
        """).format(
            columns=columns, table=self.table, where_clause=where_clause
        ))
        self.logger.debug(f"keyvals query: {sql}")

        records = []
        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            result = conn.execute(sql).mappings()
            for row in result:
                records.append({'value': row[key], 'label': row[value]})

        return records

    def show(self, id, client_srid, filter_fields):
        """Get a feature.

        :param int id: Dataset feature ID
        :param int client_srid: Client SRID or None for dataset SRID
        :param list[string] filter_fields: Field names to return
        """
        srid = client_srid or self.srid

        attributes, join_query = self.__prepare_join_query(filter_fields)

        # build query SQL

        # select columns list
        columns = (', ').join(self.escape_column_names(attributes))

        where_clause = ""
        if self.datasource_filter:
            where_clause = "AND (" + self.datasource_filter + ")"

        geom_sql = self.geom_column_sql(srid)
        sql = sql_text(("""
            SELECT {columns}%s
            FROM {table} __J0
            {join_query}
            WHERE {pkey} = :id {where_clause}
            LIMIT 1;
        """ % geom_sql).format(
            columns=columns, geom=self.geometry_column, table=self.table,
            pkey=self.primary_key, join_query=join_query, where_clause=where_clause
        ))
        params = {"id": id}

        self.logger.debug(f"show query: {sql}")
        self.logger.debug(f"params: {params}")

        feature = None
        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # execute query
            result = conn.execute(sql, params).mappings()
            for row in result:
                # NOTE: result is empty if not found
                feature = self.feature_from_query(dict(row), srid, filter_fields)

        return feature

    def create(self, feature):
        """Create a new feature.

        :param object feature: GeoJSON Feature
        """
        # build query SQL
        sql_params = self.sql_params_for_feature(feature)
        srid = sql_params['client_srid']
        attributes, join_query = self.__prepare_join_query()

        geom_sql = self.geom_column_sql(srid, True)
        if geom_sql:
            attributes.extend(['_json_geom_', '_bbox_'])

        # select columns list
        columns = (', ').join(self.escape_column_names(attributes))

        # connect to database
        with self.db_write.begin() as conn:
            sql = sql_text(("""
                WITH __I AS (
                    INSERT INTO {table} ({insert_columns})
                        VALUES ({values_sql})
                    RETURNING {return_columns}%s
                ) SELECT {columns}
                FROM __I __J0
                {join_query}
            """ % geom_sql).format(
                table=self.table, insert_columns=sql_params['columns'],
                values_sql=sql_params['values_sql'],
                return_columns=sql_params['return_columns'],
                geom=self.geometry_column,
                columns=columns,
                join_query=join_query
            ))
            params = sql_params['bound_values']

            self.logger.debug(f"create query: {sql}")
            self.logger.debug(f"params: {params}")

            # execute query
            # NOTE: use bound values
            feature = None
            result = conn.execute(sql, params).mappings()
            for row in result:
                feature = self.feature_from_query(dict(row), srid)

        return feature

    def update(self, id, feature):
        """Update a feature.

        :param int id: Dataset feature ID
        :param object feature: GeoJSON Feature
        """
        # build query SQL
        sql_params = self.sql_params_for_feature(feature)
        srid = sql_params['client_srid']
        attributes, join_query = self.__prepare_join_query()

        geom_sql = self.geom_column_sql(srid, True)
        if geom_sql:
            attributes.extend(['_json_geom_', '_bbox_'])

        # select columns list
        columns = (', ').join(self.escape_column_names(attributes))

        # connect to database
        with self.db_write.begin() as conn:
            sql = sql_text(("""
                WITH __U AS (
                    UPDATE {table} SET ({update_columns}) =
                        ROW({values_sql})
                    WHERE {pkey} = :{pkey}
                    RETURNING {return_columns}%s
                ) SELECT {columns}
                FROM __U __J0
                {join_query}
            """ % geom_sql).format(
                table=self.table, update_columns=sql_params['columns'],
                values_sql=sql_params['values_sql'], pkey=self.primary_key,
                return_columns=sql_params['return_columns'],
                geom=self.geometry_column,
                columns=columns,
                join_query=join_query
            ))

            update_values = sql_params['bound_values']
            update_values[self.primary_key] = id

            self.logger.debug(f"update query: {sql}")
            self.logger.debug(f"params: {update_values}")

            # execute query
            # NOTE: use bound values
            feature = None
            result = conn.execute(sql, update_values).mappings()
            for row in result:
                # NOTE: result is empty if not found
                feature = self.feature_from_query(dict(row), srid)

        return feature

    def destroy(self, id):
        """Delete a feature.

        :param int id: Dataset feature ID
        """

        where_clause = ""
        if self.datasource_filter:
            add_where_clause = "AND (" + self.datasource_filter + ")"

        # build query SQL
        sql = sql_text("""
            DELETE FROM {table}
            WHERE "{pkey}" = :id {where_clause}
            RETURNING "{pkey}";
        """.format(table=self.table, pkey=self.primary_key, where_clause=where_clause))
        params = {"id": id}

        self.logger.debug(f"destroy query: {sql}")
        self.logger.debug(f"params: {params}")

        # connect to database
        with self.db_write.begin() as conn:
            # execute query
            success = False
            result = conn.execute(sql, params)
            if result.one_or_none():
                # NOTE: result is empty if not found
                success = True

        return success

    def exists(self, id):
        """Check if a feature exists.
        :param int id: Dataset feature ID
        """

        where_clause = ""
        if self.datasource_filter:
            where_clause = "AND (" + self.datasource_filter + ")"

        sql = sql_text(("""
            SELECT EXISTS(SELECT 1 FROM {table} WHERE {pkey}=:id {where_clause})
        """).format(
            table=self.table, pkey=self.primary_key,
            where_clause=where_clause
        ))

        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # execute query
            result = conn.execute(sql, {"id": id})
            exists = result.fetchone()[0]

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

        sql = []
        params = {}
        errors = []

        if type(filterarray[0]) is not list:
            # Parser expects first child to be an array
            filterarray = [filterarray]
        self.__parse_filter_inner(filterarray, sql, params, errors)

        if errors:
            return (None, ";".join(errors))
        if not sql:
            return ("", [])
        else:
            return ("(%s)" % " ".join(sql), params)

    def __parse_filter_inner(self, filterarray, sql, params, errors, pad = ""):
        CONCAT_OPERATORS = ["AND", "OR"]
        OPERATORS = [
            "=", "!=", "<>", "<", ">", "<=", ">=",
            "LIKE", "ILIKE",
            "IS", "IS NOT"
        ]
        VALUE_TYPES = [int, float, str, type(None), bool]

        i = 0
        for entry in filterarray:
            if type(entry) is str:
                entry = entry.upper()
                if entry not in CONCAT_OPERATORS:
                    errors.append("Invalid concatenation operator '%s'" % entry)
                    return
                if i % 2 != 1 or i == len(filterarray) - 1:
                    # filter concatenation operators must be at odd-numbered
                    # positions in the array and cannot appear last
                    errors.append("Incorrect concatenation operator position for '%s'" % entry)
                    return
                sql.append(entry)
            elif type(entry) is list:
                if len(entry) == 0:
                    errors.append("Empty list in expression")
                    return
                if type(entry[0]) is list:
                    # nested expression
                    sql.append("(")
                    self.__parse_filter_inner(entry, sql, params, errors, pad + "  ")
                    sql.append(")")
                elif len(entry) != 3:
                    # filter entry must have exactly three parts
                    errors.append("Incorrect number of entries in %s" % entry)
                    return
                else:
                    # column
                    column_name = entry[0]
                    if type(column_name) is not str:
                        errors.append("Invalid column name in %s" % entry)
                        return

                    ignore_if_not_exists = False
                    if column_name.startswith("?"):
                        ignore_if_not_exists = True
                        column_name = column_name[1:]

                    if (
                        column_name != self.primary_key
                        and column_name not in self.attributes
                    ):
                        if ignore_if_not_exists:
                            # Skip filter if column does not exists
                            continue
                        else:
                            # column not available or not permitted
                            errors.append("Column name not found or permission error in %s" % entry)
                            return

                    # operator
                    op = entry[1].upper().strip()
                    if type(entry[1]) is not str or op not in OPERATORS:
                        errors.append("Invalid operator in %s" % entry)
                        return

                    # value
                    value = entry[2]
                    if type(value) not in VALUE_TYPES:
                        errors.append("Invalid value type in %s" % entry)
                        return

                    if value is None:
                        # modify operator for NULL value
                        if op == "=":
                            op = "IS"
                        elif op == "!=":
                            op = "IS NOT"
                    elif op in ["IS", "IS NOT"]:
                        errors.append("Invalid operator in %s" % entry)
                        return

                    # add SQL fragment for filter
                    # e.g. '"type" >= :v0'
                    idx = len(params)
                    sql.append('"%s" %s :v%d' % (column_name, op, idx))
                    # add value
                    params["v%d" % idx] = value
            else:
                # invalid entry
                errors.append("Invalid entry: %s" % entry)

            i += 1

        if not sql:
            return ("", [])
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
            errors.append(self.translator.tr("validation.geojson_must_be_feature"))

        crs_required = True

        # validate geometry object
        if not self.geometry_column:
            # skip geometry validation for datasets without geometry
            crs_required = False
        elif 'geometry' not in feature:
            if new_feature and not self.allow_null_geometry:
                # geometry required on create,
                # unless NULL geometries are allowed
                errors.append(self.translator.tr("validation.missing_geojson_geom"))
            else:
                # geometry always optional on update
                crs_required = False
        elif feature.get('geometry') is None:
            if not self.allow_null_geometry:
                errors.append(self.translator.tr("validation.geom_not_null"))
            else:
                # geometry is NULL
                crs_required = False
        elif not isinstance(feature.get('geometry'), dict):
            errors.append(self.translator.tr("validation.invalid_geojson_geom"))
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
                errors.append(self.translator.tr("validation.missing_geojson_geom_type"))
            elif geometry.get('type') not in geo_json_geometry_types:
                errors.append(self.translator.tr("validation.invalid_geojson_geom_type"))
            if 'coordinates' not in geometry:
                errors.append(self.translator.tr("validation.missing_geojson_geom_coo"))
            elif not isinstance(geometry.get('coordinates'), list):
                errors.append(self.translator.tr("validation.invalid_geojson_geom_coo"))

        # validate properties
        if 'properties' not in feature:
            errors.append(self.translator.tr("validation.missing_geojson_props"))
        elif not isinstance(feature.get('properties'), dict):
            errors.append(self.translator.tr("validation.invalid_geojson_props"))
        else:
            # validate feature attributes
            for attr, value in feature.get('properties').items():
                if attr not in self.attributes:
                    # unknown attribute or not permitted
                    errors.append(self.translator.tr("validation.feature_prop_cannot_be_set") %
                                  attr)
                elif value is not None:
                    data_type = self.fields.get(attr, {}).get('data_type')
                    # NOTE: allow any data type for fields of type json
                    if (
                        data_type not in ['json', 'jsonb']
                        and not isinstance(value, (str, int, float, bool))
                    ):
                        errors.append(
                            self.translator.tr("validation.invalid_type_for_prop") % attr
                        )

        # validate CRS
        if not self.geometry_column:
            # skip CRS validation for datasets without geometry
            pass
        elif 'crs' not in feature:
            # CRS not required if geometry is omitted or NULL
            if crs_required:
                errors.append(self.translator.tr("validation.missing_geojson_crs"))
        elif not isinstance(feature.get('crs'), dict):
            errors.append(self.translator.tr("validation.invalid_geojson_crs"))
        else:
            crs = feature['crs']
            if crs.get('type') != 'name':
                errors.append(self.translator.tr("validation.geojson_crs_must_be_type_name"))
            if 'properties' not in crs:
                errors.append(self.translator.tr("validation.missing_geojson_crs_props"))
            elif not isinstance(crs.get('properties'), dict):
                errors.append(self.translator.tr("validation.invalid_geojson_crs_props"))
            elif not re.match(r'^urn:ogc:def:crs:EPSG::\d{1,6}$',
                              str(crs['properties'].get('name'))):
                errors.append(self.translator.tr("validation.geojson_crs_is_not_ogc_urn"))

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

        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # validate GeoJSON geometry
            try:
                sql = sql_text("SELECT ST_GeomFromGeoJSON(:geom);")
                conn.execute(sql, {"geom": json_geom})
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
                result = conn.execute(sql, {"geom": json_geom}).mappings()
                for row in result:
                    if not row['valid']:
                        error = {
                            'reason': row['reason']
                        }
                        if row['location'] is not None:
                            error['location'] = row['location']
                        errors.append(error)
                    elif row['is_empty']:
                        errors.append({'reason': self.translator.tr("validation.empty_or_incomplete_geom")})

                    wkt_geom = row['wkt_geom']
                    geom_type = row['geom_type']

                    # GeoJSON geometry type does not specify whether there is a Z coordinate, need
                    # to look at the length of a coordinate
                    if self.has_z(feature.get('geometry')['coordinates']):
                        geom_type += "Z"

            if not errors:
                # check WKT for repeated vertices
                groups = re.findall(r'(?<=\()([\d\.,\s]+)(?=\))', wkt_geom)
                for group in groups:
                    vertices = group.split(',')
                    for i, v in enumerate(vertices):
                        if i > 0 and vertices[i-1] == v:
                            errors.append({
                                'reason': self.translator.tr("validation.duplicate_point"),
                                'location': 'POINT(%s)' % v
                            })

            if not errors:
                # validate geometry type
                if (self.geometry_type != 'Geometry' and
                geom_type != self.geometry_type):
                    errors.append({
                        'reason': self.translator.tr("validation.invalid_geom_type") %
                                (geom_type, self.geometry_type)
                    })

        return errors

    def has_z(self, coordinates):
        if type(coordinates[0]) is list:
            return self.has_z(coordinates[0])
        else:
            return len(coordinates) == 3

    def validate_fields(self, feature):
        """Validate data types and constraints of GeoJSON Feature properties.

        :param object feature: GeoJSON Feature
        """
        errors = []

        if not self.fields:
            # skip validation if fields metadata is empty
            return errors

        # connect to database
        with self.db_read.connect() as conn:

            for attr in feature['properties']:
                constraints = self.fields.get(attr, {}).get('constraints', {})
                data_type = self.fields.get(attr, {}).get('data_type')
                input_value = feature['properties'][attr]
                value = None

                # query the correct type name for user-defined columns
                if data_type == 'USER-DEFINED': 
                    sql =  sql_text(("""
                    SELECT udt_schema::text ||'.'|| udt_name::text as defined_type
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND column_name = '{column}' and table_name = '{table}'
                    GROUP BY defined_type
                    LIMIT 1;
                    """).format(schema = self.schema, table = self.table_name, column = attr))
                    result = conn.execute(sql).mappings()
                    for row in result:
                        data_type = row['defined_type']

                if data_type == 'numeric' and \
                    constraints.get('numeric_precision', None) and \
                    constraints.get('numeric_scale', None):
                    data_type = 'numeric(%d,%d)' % (
                        constraints['numeric_precision'],
                        constraints['numeric_scale']
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

                # readOnly
                if constraints.get('readOnly', False):
                    # skip read-only fields and remove below
                    continue

                # validate data type

                conn.execute(sql_text("SAVEPOINT before_validation"))
                try:
                    # try to parse value on DB
                    sql = sql_text("SELECT (:value):: %s AS value;" % data_type)
                    result = conn.execute(sql, {"value": input_value}).mappings()
                    for row in result:
                        value = row['value']
                    conn.execute(sql_text("RELEASE SAVEPOINT before_validation"))
                except (DataError, ProgrammingError) as e:
                    conn.execute(sql_text("ROLLBACK TO SAVEPOINT before_validation"))
                    # NOTE: current transaction is aborted
                    errors.append(self.translator.tr("validation.invalid_value") %
                                (attr, data_type))

                if value is None:
                    # invalid value type
                    continue

                if data_type == 'boolean' and type(input_value) is int:
                    # prevent 'column "..." is of type boolean but expression is of
                    #          type integer'
                    errors.append(self.translator.tr("validation.invalid_value") %
                                (attr, data_type))
                    continue

                # validate constraints

                # maxlength
                maxlength = constraints.get('maxlength')
                if maxlength is not None and len(str(value)) > int(maxlength):
                    errors.append(
                        self.translator.tr("validation.value_must_be_shorter_than") %
                        (attr, maxlength)
                    )

                # min
                minimum = constraints.get('min')
                if minimum is not None and float(value) < minimum:
                    errors.append(
                        self.translator.tr("validation.value_must_be_geq_to") %
                        (attr, minimum)
                    )

                # max
                maximum = constraints.get('max')
                if maximum is not None and float(value) > maximum:
                    errors.append(
                        self.translator.tr("validation.value_must_be_leq_to") %
                        (attr, maximum)
                    )

                # values
                allowed_values = [str(v['value']) for v in constraints.get('values', [])]
                if allowed_values and value:
                    if constraints.get('allowMulti', False):
                        try:
                            value_set = ast.literal_eval(value)
                            for val in value_set:
                                if str(val) not in allowed_values:
                                    errors.append(self.translator.tr("validation.invalid_value_for") % (attr))
                        except:
                            errors.append(self.translator.tr("validation.invalid_value_for") % (attr))
                    else:
                        if str(value) not in allowed_values:
                            errors.append(self.translator.tr("validation.invalid_value_for") % (attr))

        # remove read-only properties and hidden fields without a value and check required values
        for attr in self.fields:
            constraints = self.fields.get(attr, {}).get('constraints', {})
            if constraints.get('readOnly', False) or (
                constraints.get('hidden', False) and not feature['properties'].get(attr, None)
            ):
                if attr in feature['properties']:
                    # remove read-only property from feature
                    feature['properties'].pop(attr, None)
            elif constraints.get('required', False):
                # check if required value is present and not blank
                if attr not in feature['properties']:
                    errors.append(self.translator.tr("validation.missing_required_value_for") % (attr))
                elif feature['properties'][attr] is None:
                    errors.append(self.translator.tr("validation.missing_required_value_for") % (attr))
                elif feature['properties'][attr] == "":
                    errors.append(self.translator.tr("validation.value_for_cannot_be_blank") % (attr))

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
        as additional GeoJSON column '_json_geom_' and optional Box2D '_bbox_',
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
            geom_sql = ", ST_AsGeoJSON(ST_CurveToLine(%s)) AS _json_geom_" \
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

    def feature_from_query(self, row, client_srid, filter_fields=None):
        """Build GeoJSON Feature from query result row.

        :param obj row: Row result from query
        :param int client_srid: Client SRID or None for dataset SRID
        """
        props = OrderedDict()
        for attr in self.attributes:
            if filter_fields and not attr in filter_fields:
                continue
            if attr == self.primary_key:
                continue
            value = row[attr]
            # Ensure values are JSON serializable
            if isinstance(value, date):
                props[attr] = value.isoformat()
            elif isinstance(value, Decimal):
                props[attr] = float(value)
            elif isinstance(value, UUID):
                props[attr] = str(value)
            else:
                props[attr] = value

        geometry = None
        crs = None
        bbox = None
        if self.geometry_column and (not filter_fields or "geometry" in filter_fields):
            if row['_json_geom_'] is not None:
                geometry = json.loads(row['_json_geom_'])
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

        pk = row[self.primary_key]
        # Ensure UUID primary key is JSON serializable
        if isinstance(pk, UUID):
            pk = str(pk)

        return {
            'type': 'Feature',
            'id': pk,
            'properties': props,
            'geometry': geometry,
            'crs': crs,
            'bbox': bbox
        }

    def sql_params_for_feature(self, feature):
        """Build SQL fragments and values for feature INSERT or UPDATE and
        get client SRID from GeoJSON CRS.

        :param object feature: GeoJSON Feature
        """

        # get permitted attribute values
        bound_values = OrderedDict()
        attribute_columns = []
        defaulted_attribute_columns = []
        own_attributes = [attribute for attribute in self.attributes if not self.fields[attribute].get('joinfield')]
        return_columns = list(own_attributes)
        placeholdercount = 0
        defaultedProperties = feature.get('defaultedProperties', [])

        for attr in self.attributes:
            if attr in feature['properties']:
                if attr in defaultedProperties:
                    defaulted_attribute_columns.append(attr)
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
        if not self.primary_key in return_columns:
            return_columns.prepend(self.primary_key)

        return_columns = (', ').join(
            self.escape_column_names(return_columns)
        )

        if defaulted_attribute_columns:
            columns += ', ' + ', '.join(self.escape_column_names(defaulted_attribute_columns))
            values_sql += ', ' + ', '.join(map(lambda x: "default", defaulted_attribute_columns))

        return {
            'columns': columns,
            'values_sql': values_sql,
            'return_columns': return_columns,
            'bound_values': bound_values,
            'client_srid': srid
        }

    def __prepare_join_query(self, filter_fields=None):
        """ Builds the JOIN query fragments from the dataset fields and returns the select attributes)

        :param list filter_fields: Optional list of attributes which should be selected
        """

        attributes = []
        join_queries = {}
        ident = 1
        for attribute in self.attributes:
            if filter_fields and attribute not in filter_fields:
                continue
            attributes.append(attribute)

            joinfield = self.fields[attribute].get('joinfield')
            if joinfield and joinfield['table'] not in join_queries:
                jointableconfig = self.jointables[joinfield['table']]
                join_queries[joinfield['table']] = 'LEFT JOIN "{table}" {ident} ON __J0.{tagetfield} = {ident}.{joinfield}'.format(
                    table = joinfield['table'],
                    ident = "__J%d" % (ident),
                    tagetfield = jointableconfig["targetField"],
                    joinfield = jointableconfig["joinField"]
                )
                ident += 1

        # Ensure primary key is always returned
        if not self.primary_key in attributes:
            attributes.prepend(self.primary_key)

        return attributes, "\n".join(join_queries.values())

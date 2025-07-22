from .dataset_features_provider_base import DatasetFeaturesProviderBase
from sqlalchemy.sql import text as sql_text
from flask import json
import re


class DatasetFeaturesProviderPostgres(DatasetFeaturesProviderBase):
    """PostgreSQL-specific implementation of DatasetFeaturesProvider"""

    def _init_db_connections(self, config):
        """Initialize PostgreSQL database connections"""
        # get SQLAlchemy engine for GeoDB of dataset for read actions
        if config.get('database_read'):
            self.db_read = self.db_engine.db_engine(config['database_read'])
        else:
            # fallback to default GeoDB
            self.db_read = self.db_engine.geo_db()

        # get SQLAlchemy engine for GeoDB of dataset for write actions
        if config.get('database_write'):
            self.db_write = self.db_engine.db_engine(config['database_write'])
        else:
            # fallback to GeoDB for read actions
            self.db_write = self.db_read

        self.datasource_filter = config.get('datasource_filter', None)

    def _get_table_name(self):
        """Get properly escaped table name for PostgreSQL"""
        return '"%s"."%s"' % (self.schema, self.table_name)

    def escape_column_name(self, column):
        """Escape column name for PostgreSQL"""
        return '"%s"' % column

    def escape_column_names(self, columns):
        """Escape column names for PostgreSQL"""
        return ['"%s"' % column for column in columns]

    def geom_column_sql(self, srid, with_bbox=True):
        """Generate SQL fragment for GeoJSON using PostGIS functions"""
        geom_sql = ""

        if self.geometry_column:
            transform_geom_sql = self.transform_geom_sql(
                '"%s"' % self.geometry_column, self.srid, srid
            )
            # add GeoJSON column using PostGIS ST_AsGeoJSON
            geom_sql = ", ST_AsGeoJSON(ST_CurveToLine(%s)) AS json_geom" % transform_geom_sql
            if with_bbox:
                # add Box2D column using PostGIS Box2D function
                geom_sql += ", Box2D(%s) AS _bbox_" % transform_geom_sql

        return geom_sql

    def transform_geom_sql(self, geom_sql, geom_srid, target_srid):
        """Generate SQL fragment for transforming geometry using PostGIS ST_Transform"""
        if geom_sql is None or geom_srid is None or geom_srid == target_srid:
            return geom_sql
        else:
            return "ST_Transform(%s, %s)" % (geom_sql, target_srid)

    def build_where_clauses(self, bbox, client_srid, filterexpr, filter_geom):
        """Build WHERE clauses for PostgreSQL"""
        where_clauses = []
        params = {}
        srid = client_srid or self.srid

        if self.datasource_filter:
            where_clauses.append(self.datasource_filter)

        if self.geometry_column and bbox is not None:
            # bbox filter using PostGIS
            bbox_geom_sql = self.transform_geom_sql("""
                ST_SetSRID(
                    'BOX3D(:minx :miny, :maxx :maxy)'::box3d,
                    {bbox_srid}
                )
            """.format(bbox_srid=srid), srid, self.srid)
            
            where_clauses.append(("""
                ST_Intersects("{geom}",
                    %s
                )
            """ % bbox_geom_sql).format(geom=self.geometry_column))
            
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
            where_clauses.append(f"ST_Intersects({self.escape_column_name(self.geometry_column)}, ST_GeomFromGeoJSON(:filter_geom))")
            params.update({"filter_geom": filter_geom})

        return where_clauses, params

    def calculate_overall_bbox(self, result, srid):
        """Calculate overall bbox using PostGIS ST_Extent"""
        # For PostgreSQL, we can use ST_Extent in the original query
        # This method would be called but the bbox is already calculated in the SQL
        return None

    def parse_box2d(self, box2d):
        """Parse PostgreSQL Box2D format"""
        if box2d is None:
            return None
        
        try:
            # Parse PostgreSQL Box2D format: BOX(xmin ymin, xmax ymax)
            box2d = box2d[4:-1]  # remove BOX( and closing )
            pairs = box2d.split(',')
            xmin, ymin = pairs[0].split()
            xmax, ymax = pairs[1].split()
            return [float(xmin), float(ymin), float(xmax), float(ymax)]
        except Exception:
            return None

    def validate_geometry(self, feature):
        """Validate geometry contents using PostGIS functions"""
        errors = []

        if not self.geometry_column or feature.get('geometry') is None:
            return []

        json_geom = json.dumps(feature.get('geometry'))

        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # validate GeoJSON geometry using PostGIS
            try:
                sql = sql_text("SELECT ST_GeomFromGeoJSON(:geom);")
                conn.execute(sql, {"geom": json_geom})
            except InternalError as e:
                errors.append({
                    'reason': re.sub(r'^FEHLER:\s*', '', str(e.orig)).strip()
                })

            if not errors:
                # validate geometry using PostGIS ST_IsValidDetail
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
                        error = {'reason': row['reason']}
                        if row['location'] is not None:
                            error['location'] = row['location']
                        errors.append(error)
                    elif row['is_empty']:
                        errors.append({'reason': self.translator.tr("validation.empty_or_incomplete_geom")})

        return errors

    def validate_fields(self, feature):
        """Validate data types and constraints using PostgreSQL-specific types"""
        errors = []

        if not self.fields:
            return errors

        # PostgreSQL-specific field validation implementation
        # Move the existing PostgreSQL validation logic here
        
        return errors
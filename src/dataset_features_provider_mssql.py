from .dataset_features_provider_base import DatasetFeaturesProviderBase
from sqlalchemy.sql import text as sql_text
from flask import json
import re


class DatasetFeaturesProviderMssql(DatasetFeaturesProviderBase):
    """SQL Server-specific implementation of DatasetFeaturesProvider"""

    def _init_db_connections(self, config):
        """Initialize SQL Server database connections"""
        # get SQLAlchemy engine for SQL Server database for read actions
        if config.get('database_read'):
            self.db_read = self.db_engine.db_engine(config['database_read'])
        else:
            # fallback to default database
            self.db_read = self.db_engine.geo_db()

        # get SQLAlchemy engine for SQL Server database for write actions
        if config.get('database_write'):
            self.db_write = self.db_engine.db_engine(config['database_write'])
        else:
            # fallback to read database
            self.db_write = self.db_read

        self.datasource_filter = config.get('datasource_filter', None)

    def _get_table_name(self):
        """Get properly escaped table name for SQL Server"""
        return '[%s].[%s]' % (self.schema, self.table_name)

    def escape_column_name(self, column):
        """Escape column name for SQL Server"""
        return '[%s]' % column

    def escape_column_names(self, columns):
        """Escape column names for SQL Server"""
        return ['[%s]' % column for column in columns]

    def geom_column_sql(self, srid, with_bbox=True):
        """Generate SQL fragment for GeoJSON using SQL Server spatial functions"""
        geom_sql = ""

        if self.geometry_column:
            transform_geom_sql = self.transform_geom_sql(
                self.escape_column_name(self.geometry_column), self.srid, srid
            )
            # add GeoJSON column using SQL Server AsGeoJSON
            geom_sql = ", %s.AsGeoJSON() AS json_geom" % transform_geom_sql
            if with_bbox:
                # add bounding box using SQL Server STEnvelope
                geom_sql += ", %s AS _bbox_" % transform_geom_sql

        return geom_sql

    def transform_geom_sql(self, geom_sql, geom_srid, target_srid):
        """Generate SQL fragment for transforming geometry using SQL Server STTransform"""
        if geom_sql is None or geom_srid is None or geom_srid == target_srid:
            return geom_sql
        else:
            return "%s.STTransform(%s)" % (geom_sql, target_srid)

    def build_where_clauses(self, bbox, client_srid, filterexpr, filter_geom):
        """Build WHERE clauses for SQL Server"""
        where_clauses = []
        params = {}
        srid = client_srid or self.srid

        if self.datasource_filter:
            where_clauses.append(self.datasource_filter)

        if self.geometry_column and bbox is not None:
            # bbox filter using SQL Server spatial functions
            bbox_geom_sql = self.transform_geom_sql("""
                geometry::STGeomFromText(
                    'POLYGON((@minx @miny, @maxx @miny, @maxx @maxy, @minx @maxy, @minx @miny))',
                    {bbox_srid}
                )
            """.format(bbox_srid=srid), srid, self.srid)
            
            where_clauses.append(("""
                {geom}.STIntersects(%s) = 1
            """ % bbox_geom_sql).format(geom=self.escape_column_name(self.geometry_column)))
            
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
            where_clauses.append(f"{self.escape_column_name(self.geometry_column)}.STIntersects(geometry::STGeomFromGeoJSON(@filter_geom)) = 1")
            params.update({"filter_geom": filter_geom})

        return where_clauses, params

    def calculate_overall_bbox(self, result, srid):
        """Calculate overall bbox for SQL Server using pyodbc client-side calculation"""
        if not self.geometry_column:
            return None
            
        min_x = min_y = float('inf')
        max_x = max_y = float('-inf')
        has_valid_geometries = False
        
        for row in result:
            if '_bbox_' not in row or row['_bbox_'] is None:
                continue
            
            try:
                # Method 1: Try to access SQL Server geometry methods directly
                try:
                    geom = row['_bbox_']
                    env = geom.STEnvelope()
                    x_min = env.STPointN(1).STX
                    y_min = env.STPointN(1).STY
                    x_max = env.STPointN(3).STX
                    y_max = env.STPointN(3).STY
                    
                    min_x = min(min_x, x_min)
                    min_y = min(min_y, y_min)
                    max_x = max(max_x, x_max)
                    max_y = max(max_y, y_max)
                    has_valid_geometries = True
                    
                except (AttributeError, TypeError):
                    # Method 2: If direct access fails, use a separate query
                    with self.db_read.connect() as conn2:
                        sql = sql_text("SELECT @geom.STEnvelope().STPointN(1).STX AS x_min, @geom.STEnvelope().STPointN(1).STY AS y_min, @geom.STEnvelope().STPointN(3).STX AS x_max, @geom.STEnvelope().STPointN(3).STY AS y_max")
                        env_result = conn2.execute(sql, {"geom": row['_bbox_']}).mappings()
                        bbox_row = env_result.fetchone()
                        
                        if bbox_row and None not in [bbox_row['x_min'], bbox_row['y_min'], bbox_row['x_max'], bbox_row['y_max']]:
                            min_x = min(min_x, bbox_row['x_min'])
                            min_y = min(min_y, bbox_row['y_min'])
                            max_x = max(max_x, bbox_row['x_max'])
                            max_y = max(max_y, bbox_row['y_max'])
                            has_valid_geometries = True
            
            except Exception as e:
                self.logger.warning(f"Failed to extract envelope from geometry: {str(e)}")
        
        return [min_x, min_y, max_x, max_y] if has_valid_geometries else None

    def parse_box2d(self, box2d):
        """Parse SQL Server bounding box"""
        if box2d is None:
            return None
        
        try:
            # For SQL Server, box2d is a geometry object with properties
            return [box2d.XMin, box2d.YMin, box2d.XMax, box2d.YMax]
        except Exception:
            return None

    def validate_geometry(self, feature):
        """Validate geometry contents using SQL Server spatial functions"""
        errors = []

        if not self.geometry_column or feature.get('geometry') is None:
            return []

        json_geom = json.dumps(feature.get('geometry'))

        # connect to database (for read-only access)
        with self.db_read.connect() as conn:
            # SQL Server validation
            try:
                sql = sql_text("SELECT geometry::STGeomFromGeoJSON(@geom) AS geom;")
                conn.execute(sql, {"geom": json_geom})
            except Exception as e:
                errors.append({'reason': str(e).strip()})
                
            if not errors:
                # validate geometry using SQL Server functions
                sql = sql_text("""
                    SELECT 
                        geom.STIsValid() as valid, 
                        geom.STIsValidReason() as reason,
                        geom.STAsText() as wkt_geom,
                        geom.STGeometryType() as geom_type,
                        geom.STIsEmpty() as is_empty
                    FROM (SELECT geometry::STGeomFromGeoJSON(@geom) as geom) as T
                """)
                result = conn.execute(sql, {"geom": json_geom}).mappings()
                for row in result:
                    if not bool(row['valid']):
                        errors.append({'reason': row['reason']})
                    elif bool(row['is_empty']):
                        errors.append({'reason': self.translator.tr("validation.empty_or_incomplete_geom")})

        return errors

    def validate_fields(self, feature):
        """Validate data types and constraints using SQL Server-specific types"""
        errors = []

        if not self.fields:
            return errors

        # SQL Server-specific field validation implementation
        # Move the existing SQL Server validation logic here
        
        return errors
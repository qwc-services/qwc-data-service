class SpatialAdapter:
    """Adapter for database-specific spatial operations
    
    Provides translations between PostgreSQL/PostGIS and SQL Server spatial functions.
    """
    
    def __init__(self, dialect):
        """Constructor
        
        :param str dialect: Database dialect ('postgresql' or 'mssql')
        """
        self.dialect = dialect
    
    def geom_from_geojson(self, geojson_param, srid):
        """Convert GeoJSON to native geometry
        
        :param str geojson_param: Parameter name for GeoJSON string
        :param int srid: Target SRID
        """
        if self.dialect == 'postgresql':
            return f"ST_SetSRID(ST_GeomFromGeoJSON(:{geojson_param}), {srid})"
        elif self.dialect == 'mssql':
            return f"geography::STGeomFromText(geometry::STGeomFromGeoJSON(:{geojson_param}).STAsText(), {srid})"
    
    def geom_to_geojson(self, geom_column):
        """Convert native geometry to GeoJSON
        
        :param str geom_column: Geometry column name
        """
        if self.dialect == 'postgresql':
            return f"ST_AsGeoJSON({geom_column})"
        elif self.dialect == 'mssql':
            return f"{geom_column}.AsGeoJSON()"
    
    def transform_geom(self, geom_column, target_srid):
        """Transform geometry to different SRID
        
        :param str geom_column: Geometry column name
        :param int target_srid: Target SRID
        """
        if self.dialect == 'postgresql':
            return f"ST_Transform({geom_column}, {target_srid})"
        elif self.dialect == 'mssql':
            return f"{geom_column}.STTransform({target_srid})"
    
    def bbox(self, geom_column):
        """Get bounding box of geometry
        
        :param str geom_column: Geometry column name
        """
        if self.dialect == 'postgresql':
            return f"ST_Envelope({geom_column})"
        elif self.dialect == 'mssql':
            return f"{geom_column}.STEnvelope()"
    
    def intersects(self, geom_column, filter_geom):
        """Check if geometry intersects with filter geometry
        
        :param str geom_column: Geometry column name
        :param str filter_geom: Filter geometry expression
        """
        if self.dialect == 'postgresql':
            return f"ST_Intersects({geom_column}, {filter_geom})"
        elif self.dialect == 'mssql':
            return f"{geom_column}.STIntersects({filter_geom}) = 1"
    
    def is_valid(self, geom):
        """Check if geometry is valid
        
        :param str geom: Geometry expression
        """
        if self.dialect == 'postgresql':
            return f"ST_IsValid({geom})"
        elif self.dialect == 'mssql':
            return f"{geom}.STIsValid()"
    
    def validation_reason(self, geom):
        """Get geometry validation error reason
        
        :param str geom: Geometry expression
        """
        if self.dialect == 'postgresql':
            return f"ST_IsValidReason({geom})"
        elif self.dialect == 'mssql':
            return f"{geom}.STIsValidReason()"
    
    def geometry_type(self, geom):
        """Get geometry type
        
        :param str geom: Geometry expression
        """
        if self.dialect == 'postgresql':
            return f"ST_GeometryType({geom})"
        elif self.dialect == 'mssql':
            return f"{geom}.STGeometryType()"
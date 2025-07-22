import importlib
from .dataset_features_provider_postgres import DatasetFeaturesProviderPostgres


def create_dataset_features_provider(config, db_engine, logger, translator):
    """Factory function to create the appropriate DatasetFeaturesProvider based on backend
    
    :param obj config: Data service config for a dataset
    :param DatabaseEngine db_engine: Database engine with DB connections
    :param Logger logger: Application logger
    :param obj translator: Translator
    """
    
    # Determine backend from config or database URL
    backend = config.get('backend', 'postgres')
    
    # You could also detect backend from database URL
    if hasattr(db_engine, 'dialect') and hasattr(db_engine.dialect, 'name'):
        dialect_name = db_engine.dialect.name
        if 'mssql' in dialect_name or 'sqlserver' in dialect_name:
            backend = 'mssql'
        elif 'postgresql' in dialect_name or 'postgres' in dialect_name:
            backend = 'postgres'
    
    if backend == 'mssql':
        try:
            # Dynamically import MSSQL provider only when needed
            from .dataset_features_provider_mssql import DatasetFeaturesProviderMssql
            return DatasetFeaturesProviderMssql(config, db_engine, logger, translator)
        except ImportError as e:
            logger.error(f"MSSQL provider not available: {e}")
            raise ImportError("MSSQL provider requires additional dependencies (pyodbc)")
    else:
        # Default to PostgreSQL
        return DatasetFeaturesProviderPostgres(config, db_engine, logger, translator)
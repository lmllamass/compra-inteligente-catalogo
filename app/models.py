from sqlalchemy import Table, Column, Text, MetaData, ForeignKey
from .settings import settings

metadata = MetaData(schema=settings.DB_SCHEMA)

marcas = Table(
    "marcas", metadata,
    Column("id", Text, primary_key=True),
    Column("nombre", Text, nullable=False),
)

familias = Table(
    "familias", metadata,
    Column("id", Text, primary_key=True),
    Column("nombre", Text, nullable=False),
)

subfamilias = Table(
    "subfamilias", metadata,
    Column("id", Text, primary_key=True),
    Column("familia_id", Text, ForeignKey(f"{settings.DB_SCHEMA}.familias.id"), nullable=False),
    Column("nombre", Text, nullable=False),
)

productos = Table(
    "productos", metadata,
    Column("id_daterium", Text, primary_key=True),
    Column("nombre", Text, nullable=False),
    Column("marca_id", Text, ForeignKey(f"{settings.DB_SCHEMA}.marcas.id")),
    Column("familia_id", Text, ForeignKey(f"{settings.DB_SCHEMA}.familias.id")),
    Column("subfamilia_id", Text, ForeignKey(f"{settings.DB_SCHEMA}.subfamilias.id")),
    Column("descripcion", Text),
)

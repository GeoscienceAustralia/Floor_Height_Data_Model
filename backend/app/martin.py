"""
Utilities for setting up some special SQL functions used by
Martin (the vector tile server)
"""

import logging
import sqlalchemy
from sqlalchemy.sql import text

logger = logging.getLogger("floorheights")

BUILDING_LAYER_FN = """
CREATE OR REPLACE
    FUNCTION building_query(z integer, x integer, y integer, query_params jsonb)
    RETURNS bytea AS $$
DECLARE
  mvt bytea;
BEGIN
  SELECT INTO mvt ST_AsMVT(tile.*, 'building_query', 4096, 'geom') FROM (
    SELECT
      building.id as id,
      "min_height_ahd",
      "max_height_ahd",
      STRING_AGG(DISTINCT m.name, ', ' ORDER BY m.name) AS method_names,
      STRING_AGG(DISTINCT d.name, ', ' ORDER BY d.name) AS dataset_names,
      ARRAY_AGG(fm.height) AS heights,
      AVG(fm.height) as avg_ffh,
      MIN(fm.height) as min_ffh,
      MAX(fm.height) as max_ffh,
      ST_AsMVTGeom(
          ST_Transform(ST_CurveToLine(outline), 3857),
          ST_TileEnvelope(z, x, y),
          4096, 64, true) AS geom
    FROM
      building
      LEFT JOIN floor_measure as fm ON fm.building_id = building.id
      LEFT JOIN method AS m on m.id = fm.method_id
      LEFT JOIN floor_measure_dataset_association AS fmda ON fmda.floor_measure_id = fm.id
      LEFT JOIN dataset as d ON d.id = fmda.dataset_id
    WHERE outline && ST_Transform(ST_TileEnvelope(z, x, y), 7844)
      -- Optional query parameters
      AND (
        NOT jsonb_exists(query_params, 'method_filter') 
        OR m.name = ANY(string_to_array(query_params->>'method_filter', ','))
      )
      AND (
        NOT jsonb_exists(query_params, 'dataset_filter') 
        OR d.name = ANY(string_to_array(query_params->>'dataset_filter', ','))
      )
      AND (
        NOT jsonb_exists(query_params, 'bbox') 
        OR building.outline @ ST_MakeEnvelope (
            (string_to_array(query_params->>'bbox', ','))[1]::float,
            (string_to_array(query_params->>'bbox', ','))[2]::float,
            (string_to_array(query_params->>'bbox', ','))[3]::float,
            (string_to_array(query_params->>'bbox', ','))[4]::float,
            7844)
      )
    GROUP BY building.id
  ) as tile WHERE geom IS NOT NULL;

  RETURN mvt;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
"""


def setup_building_layer_fn(db: sqlalchemy.orm.Session):
    logger.info("Setting up building layer SQL function")
    db.execute(text(BUILDING_LAYER_FN))
    db.commit()

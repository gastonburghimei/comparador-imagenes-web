-- Primero elimina la tabla si existe
DROP TABLE IF EXISTS `meli-bi-data.SBOX_PFFINTECHATO.ato_bq_pq`;

-- Crea la nueva tabla sin las columnas no deseadas
CREATE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq_pq` AS (
    SELECT
        a.*,
        CAST('2023-01-01' AS DATETIME) AS action_date,
        "sin_dato" as action_id
    FROM
        `meli-bi-data.SBOX_PFFINTECHATO.ato_bq` a
);

-- Ahora eliminar las columnas adicionales (action_date y action_id)
DROP TABLE IF EXISTS `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`;

-- Crea la tabla ATO_BQ a partir de ATO_BQ_PQ
CREATE TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
PARTITION BY op_dt AS (
    SELECT
        a.*
    FROM
        `meli-bi-data.SBOX_PFFINTECHATO.ato_bq_pq` a
);


-- Para eliminar columnas y que corra bien el job
ALTER TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
DROP COLUMN origen_final,
DROP COLUMN origen_subfinal;

-- Reordenar columnas
-- Agregar nueva columna temporal
ALTER TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
ADD COLUMN operation_id_temp NUMERIC;

-- Actualizar la nueva columna temporal con los datos convertidos
UPDATE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
SET operation_id_temp = CAST(operation_id AS NUMERIC)
WHERE 1=1;

-- Eliminar la columna original
ALTER TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
DROP COLUMN operation_id;

-- Renombrar la columna temporal a operation_id
ALTER TABLE `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
RENAME COLUMN operation_id_temp TO operation_id;

SELECT * FROM `meli-bi-data.SBOX_PFFINTECHATO.ato_bq`
LIMIT 10
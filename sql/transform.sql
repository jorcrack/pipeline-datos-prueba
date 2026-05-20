-- Creación automática de la tabla si no existe en el dataset INTEGRATION
CREATE TABLE IF NOT EXISTS `prueba-tecnica-data.INTEGRATION.integration_prueba_tecnica` (
  id INT64,
  user_id INT64,
  title STRING,
  body STRING,
  processed_at TIMESTAMP
);

-- Operación MERGE para garantizar la idempotencia
MERGE `prueba-tecnica-data.INTEGRATION.integration_prueba_tecnica` T
USING `prueba-tecnica-data.SANDBOX_crypto_api.raw_api_data` S
ON T.id = S.id
WHEN MATCHED THEN
  UPDATE SET 
    T.user_id = S.userId,
    T.title = UPPER(S.title),
    T.body = S.body,
    T.processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (id, user_id, title, body, processed_at)
  VALUES (S.id, S.userId, UPPER(S.title), S.body, CURRENT_TIMESTAMP());
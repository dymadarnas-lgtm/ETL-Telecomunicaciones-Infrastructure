-- ================================================================
--  ETL Telecomunicaciones — Consultas SQL
--  Base de datos: MySQL
--  Objetivo: Cruzar clientes con tabla de antenas y detectar errores
-- ================================================================


-- ────────────────────────────────────────────────────────────────
-- PASO 1: Crear la tabla de antenas de red
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS antenas (
    antena_id       VARCHAR(10)  PRIMARY KEY,
    zona            VARCHAR(50)  NOT NULL,
    capacidad_max   INT          NOT NULL,   -- máx. clientes simultáneos
    tecnologia      VARCHAR(10)  NOT NULL,   -- FIBRA o 4G
    estado_red      VARCHAR(15)  NOT NULL    -- OPERATIVA / MANTENIMIENTO
);

INSERT INTO antenas VALUES
  ('ANT_001', 'Centro',        150, 'FIBRA',  'OPERATIVA'),
  ('ANT_002', 'Norte',         200, 'FIBRA',  'OPERATIVA'),
  ('ANT_003', 'Sur',           100, '4G',     'OPERATIVA'),
  ('ANT_004', 'Este',          180, '4G',     'OPERATIVA'),
  ('ANT_005', 'Oeste',          80, 'FIBRA',  'MANTENIMIENTO'),
  ('ANT_006', 'Gran_Buenos',   250, '4G',     'OPERATIVA'),
  ('ANT_007', 'Conurbano',     120, 'FIBRA',  'OPERATIVA'),
  ('ANT_008', 'Córdoba',        90, 'FIBRA',  'MANTENIMIENTO');
-- NOTA: ANT_999 NO existe → clientes asignados a ella son errores críticos


-- ────────────────────────────────────────────────────────────────
-- PASO 2: Crear la tabla de clientes (ya limpios)
-- ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS clientes (
    cliente_id      INT          PRIMARY KEY,
    nombre          VARCHAR(50),
    apellido        VARCHAR(50),
    dni             VARCHAR(10),
    telefono        VARCHAR(15),
    email           VARCHAR(100),
    plan            VARCHAR(20),
    antena_id       VARCHAR(10),
    fecha_alta      DATE,
    estado          VARCHAR(15),
    monto_factura   DECIMAL(10,2),
    FOREIGN KEY (antena_id) REFERENCES antenas(antena_id)
        ON DELETE SET NULL
);


-- ════════════════════════════════════════════════════════════════
-- CONSULTA 1: Clientes asignados a antenas inexistentes (error crítico)
-- ════════════════════════════════════════════════════════════════
SELECT
    c.cliente_id,
    CONCAT(c.nombre, ' ', c.apellido) AS cliente,
    c.plan,
    c.antena_id                        AS antena_asignada,
    c.estado,
    c.monto_factura
FROM clientes c
LEFT JOIN antenas a ON c.antena_id = a.antena_id
WHERE a.antena_id IS NULL
ORDER BY c.cliente_id;

-- Resultado esperado: clientes con ANT_999 → antena ficticia
-- Impacto: sin servicio real, pero con factura activa


-- ════════════════════════════════════════════════════════════════
-- CONSULTA 2: Clientes activos en antenas en mantenimiento
-- ════════════════════════════════════════════════════════════════
SELECT
    c.cliente_id,
    CONCAT(c.nombre, ' ', c.apellido) AS cliente,
    c.plan,
    a.antena_id,
    a.zona,
    a.estado_red,
    c.monto_factura
FROM clientes  c
JOIN antenas   a ON c.antena_id = a.antena_id
WHERE c.estado     = 'ACTIVO'
  AND a.estado_red = 'MANTENIMIENTO'
ORDER BY a.antena_id;

-- Resultado esperado: clientes pagando por servicio interrumpido


-- ════════════════════════════════════════════════════════════════
-- CONSULTA 3: Inconsistencias en facturación por estado
-- ════════════════════════════════════════════════════════════════
SELECT
    estado,
    COUNT(*)                          AS total_clientes,
    SUM(CASE WHEN monto_factura = 0   THEN 1 ELSE 0 END) AS sin_cobro,
    SUM(CASE WHEN monto_factura > 0   THEN 1 ELSE 0 END) AS con_cobro,
    SUM(monto_factura)                AS facturacion_total
FROM clientes
GROUP BY estado
ORDER BY estado;

-- Detecta: ACTIVOS con $0 y SUSPENDIDOS con cobro


-- ════════════════════════════════════════════════════════════════
-- CONSULTA 4: Resumen ejecutivo de inconsistencias por antena
-- ════════════════════════════════════════════════════════════════
SELECT
    COALESCE(a.antena_id, 'SIN_ANTENA')  AS antena,
    COALESCE(a.zona, 'DESCONOCIDA')      AS zona,
    COALESCE(a.estado_red, 'N/A')        AS estado_red,
    COUNT(c.cliente_id)                  AS total_clientes,
    SUM(CASE
        WHEN a.antena_id IS NULL
          OR a.estado_red = 'MANTENIMIENTO'
          OR (c.estado = 'ACTIVO' AND c.monto_factura = 0)
          OR (c.estado = 'SUSPENDIDO' AND c.monto_factura > 0)
        THEN 1 ELSE 0
    END)                                 AS clientes_con_error,
    ROUND(
        SUM(CASE
            WHEN a.antena_id IS NULL
              OR a.estado_red = 'MANTENIMIENTO'
              OR (c.estado = 'ACTIVO' AND c.monto_factura = 0)
              OR (c.estado = 'SUSPENDIDO' AND c.monto_factura > 0)
            THEN 1 ELSE 0
        END) * 100.0 / COUNT(c.cliente_id), 1
    )                                    AS pct_error,
    SUM(c.monto_factura)                 AS monto_en_riesgo
FROM clientes  c
LEFT JOIN antenas a ON c.antena_id = a.antena_id
GROUP BY a.antena_id, a.zona, a.estado_red
ORDER BY clientes_con_error DESC;

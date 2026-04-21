# Monitoreo de Infraestructura: Detección de brechas en facturación 📡

> **ETL · Python · SQL · Grafana · Power BI**

---

## 📊 Resultados de la corrida

| Métrica | Valor |
|---|---|
| 🔍 Registros analizados | **30 clientes** |
| ⚠️ Inconsistencias detectadas | **15%** del total |
| 🔴 Antenas inválidas | **2 clientes** en ANT\_999 (inexistente) |
| 🔧 Antenas en mantenimiento | **2 clientes** cobrando sin servicio |
| 💸 Monto en riesgo | **$15.600 ARS/mes** |

---

## 🔴 Errores detectados por severidad

```
CRÍTICO  ██████████  Antena inexistente (ANT_999)        → 2 casos
CRÍTICO  ██████████  Activo en antena en mantenimiento   → 2 casos
CRÍTICO  ██████████  Suspendido con cobro activo         → 1 caso
MEDIO    ██████░░░░  Teléfono inválido o ausente         → 3 casos
MEDIO    ████░░░░░░  Email faltante                      → 4 casos
MEDIO    ███░░░░░░░  DNI faltante                        → 2 casos
MEDIO    ███░░░░░░░  Formato de fecha incorrecto         → 3 casos
```

---

## 📡 Distribución de clientes por antena

```
ANT_001  █████████████████████████████░  5 clientes   ✅ Operativa
ANT_002  ████████████████████████░░░░░░  4 clientes   ✅ Operativa
ANT_003  ████████████████████████░░░░░░  4 clientes   ✅ Operativa
ANT_999  ████████░░░░░░░░░░░░░░░░░░░░░░  2 clientes   ❌ NO EXISTE
Otros    ██████████████████████████████  15 clientes
```

---

## El Problema (Contexto)

En el sector de telecomunicaciones, es común que la base de datos comercial no coincida con la técnica. Esto genera que clientes tengan servicios activos en antenas que no existen o que están en mantenimiento, resultando en cobros indebidos o falta de señal sin previo aviso.

## ¿Cómo lo resolví?

Diseñé un proceso ETL para cruzar los registros de **30 clientes** contra la base de infraestructura, detectando que un **15% de los datos presentaba inconsistencias críticas**.

## Stack Técnico

* **Python (Pandas):** Limpieza de strings, normalización de fechas y manejo de valores nulos en el archivo `clientes_sucio.csv`.
* **SQL:** Ejecución de JOINs para identificar usuarios colgados a la antena `ANT_999` (inexistente).
* **Visualización:** Reporte en HTML/CSS y Dashboards en **Grafana** y Power BI para mostrar el impacto financiero en tiempo real (estimado en **$15.6K**).

## Estructura del Repo

* `etl_limpieza.py`: El script donde sucede la magia de la limpieza.
* `consultas_etl.sql`: Las queries que usé para segmentar los errores por severidad.
* `reporte_final.html`: El entregable visual para el equipo de operaciones.

---

*Este proyecto fue parte de mi experiencia técnica en el sector de datos y telecomunicaciones.*


import psycopg2
import json
import os
from datetime import datetime

conn = psycopg2.connect(
      host=os.environ["REDSHIFT_HOST"],
      port=os.environ.get("REDSHIFT_PORT", 5439),
      dbname=os.environ["REDSHIFT_DB"],
      user=os.environ["REDSHIFT_USER"],
      password=os.environ["REDSHIFT_PASSWORD"]
 )
cur = conn.cursor()

# ── Query 1: tendencia semanal agregada ────────────────────────────────────
cur.execute("""
SELECT
    DATE_TRUNC('week', mes) as semana,
    SUM(tickets_hubspot) as tickets_hubspot,
    SUM(tickets_orbita)   as tickets_orbita,
    SUM(ordenes)          as ordenes
FROM (
    SELECT DATE_TRUNC('week', ht.create_date) as mes,
      COUNT(DISTINCT ht.ticket_id) as tickets_hubspot, 0 as tickets_orbita, 0 as ordenes
    FROM hubspot.ticket ht
    JOIN hubspot.company hc ON LOWER(TRIM(ht.company)) = LOWER(TRIM(hc.company_name))
    JOIN orbita.seller_buyer_support sbs ON sbs.id::varchar = hc.orbita_seller_id
    WHERE sbs.is_active = 1 AND ht.create_date >= '2026-03-01'
    GROUP BY 1
    UNION ALL
    SELECT DATE_TRUNC('week', t.created_at), 0, COUNT(DISTINCT t.id), 0
    FROM orbita.ticket t
    JOIN orbita.seller_buyer_support sbs ON sbs.id = t.seller_id
    WHERE sbs.is_active = 1 AND t.created_at >= '2026-03-01'
    GROUP BY 1

    UNION ALL

    SELECT DATE_TRUNC('week', so.creation_date), 0, 0, COUNT(DISTINCT so.id)
    FROM orbita.sell_order so
    JOIN orbita.seller_buyer_support sbs ON sbs.id = so.seller_id
    WHERE sbs.is_active = 1 AND so.creation_date >= '2026-03-01'
    GROUP BY 1
) combined
GROUP BY 1
ORDER BY 1
""")
cols = ["semana","tickets_hubspot","tickets_orbita","ordenes"]
tendencia_semanal = [dict(zip(cols, row)) for row in cur.fetchall()]
for r in tendencia_semanal:
      r["semana"] = r["semana"].strftime("%Y-%m-%d")

# ── Query 2: por seller ayer ───────────────────────────────────────────────
 cur.execute("""
 SELECT
    sbs.name as seller,
    COUNT(DISTINCT so.id)        as ordenes,
    COUNT(DISTINCT t_orb.id)     as tickets_orbita,
    COUNT(DISTINCT ht.ticket_id) as tickets_hubspot
 FROM orbita.seller_buyer_support sbs
 LEFT JOIN orbita.sell_order so
    ON sbs.id = so.seller_id AND DATE(so.creation_date) = CURRENT_DATE - 1
 LEFT JOIN orbita.ticket t_orb
    ON sbs.id = t_orb.seller_id AND DATE(t_orb.created_at) = CURRENT_DATE - 1
 LEFT JOIN hubspot.company hc
    ON sbs.id::varchar = hc.orbita_seller_id
 LEFT JOIN hubspot.ticket ht
    ON LOWER(TRIM(ht.company)) = LOWER(TRIM(hc.company_name))
    AND DATE(ht.create_date) = CURRENT_DATE - 1
 WHERE sbs.is_active = 1
 GROUP BY sbs.name
 HAVING COUNT(DISTINCT so.id) > 0
      OR COUNT(DISTINCT t_orb.id) > 0
      OR COUNT(DISTINCT ht.ticket_id) > 0
ORDER BY (COUNT(DISTINCT t_orb.id) + COUNT(DISTINCT ht.ticket_id)) DESC
 """)
cols = ["seller","ordenes","tickets_orbita","tickets_hubspot"]
por_seller = [dict(zip(cols, row)) for row in cur.fetchall()]

# ── Query 3: KPIs generales ────────────────────────────────────────────────
cur.execute("""
SELECT
    COUNT(*) as total_tickets,
    SUM(CASE WHEN status IN ('CLOSED','RESOLVED') THEN 1 ELSE 0 END) as cerrados,
    SUM(CASE WHEN status = 'AWAITING_SELLER_SELECTION' THEN 1 ELSE 0 END) as en_espera
FROM orbita.ticket
""")
row = cur.fetchone()
kpis_tickets = {"total": row[0], "cerrados": row[1], "en_espera": row[2]}

cur.execute("""
SELECT
    SUM(CASE WHEN event_type = 'AI_RESOLVED'  THEN 1 ELSE 0 END) as ai_resolvio,
    SUM(CASE WHEN event_type = 'AI_ESCALATED' THEN 1 ELSE 0 END) as ai_escalo,
    SUM(CASE WHEN event_type = 'AI_ASSIGNED'  THEN 1 ELSE 0 END) as ai_asignado
FROM orbita.ticket_event
WHERE event_type LIKE 'AI%'
 """)
row = cur.fetchone()
kpis_ia = {"ai_resolvio": row[0], "ai_escalo": row[1], "ai_asignado": row[2]}

# ── Query 4: sentimiento ───────────────────────────────────────────────────
cur.execute("""
SELECT
    CASE
      WHEN LOWER(sentiment) LIKE '%frustrad%' OR LOWER(sentiment) LIKE '%muy frustrad%' THEN 'Frustrado'
      WHEN LOWER(sentiment) LIKE '%negativ%'  THEN 'Negativo'
      WHEN LOWER(sentiment) LIKE '%preocupad%' THEN 'Preocupado'
      WHEN LOWER(sentiment) LIKE '%insatisf%' OR LOWER(sentiment) LIKE '%molest%' THEN 'Insatisfecho'
      WHEN LOWER(sentiment) LIKE '%positiv%' OR LOWER(sentiment) LIKE '%amable%' THEN 'Positivo'
      ELSE 'Neutral'
END as grupo,
 COUNT(*) as total
  FROM orbita.conversation_summary
  GROUP BY 1
  ORDER BY 2 DESC
  """)
  sentimiento = [{"grupo": r[0], "total": r[1]} for r in cur.fetchall()]

  # ── Query 5: tendencia semanal por seller ──────────────────────────────────
  cur.execute("""
  SELECT
    sbs.name                             AS seller,
    DATE_TRUNC('week', combined.semana)::date AS semana,
    SUM(combined.tickets_hubspot)        AS tickets_hubspot,
    SUM(combined.tickets_orbita)         AS tickets_orbita,
    SUM(combined.ordenes)                AS ordenes,
    SUM(combined.ai_resolvio)            AS ai_resolvio,
    SUM(combined.ai_escalo)              AS ai_escalo,
    SUM(combined.en_espera)              AS en_espera
  FROM (
    SELECT
      hc.orbita_seller_id::varchar       AS seller_id,
      DATE_TRUNC('week', ht.create_date) AS semana,
      COUNT(DISTINCT ht.ticket_id)       AS tickets_hubspot,
      0 AS tickets_orbita,
      0 AS ordenes,
      0 AS ai_resolvio,
      0 AS ai_escalo,
      0 AS en_espera
    FROM hubspot.ticket ht
    JOIN hubspot.company hc
      ON LOWER(TRIM(ht.company)) = LOWER(TRIM(hc.company_name))
    WHERE ht.create_date >= '2026-03-01'
    GROUP BY 1, 2

    UNION ALL

    SELECT
      t.seller_id::varchar,
      DATE_TRUNC('week', t.created_at),
      0,
      COUNT(DISTINCT t.id),
      0,
      COUNT(DISTINCT CASE WHEN te.event_type = 'AI_RESOLVED'  THEN t.id END),
      COUNT(DISTINCT CASE WHEN te.event_type = 'AI_ESCALATED' THEN t.id END),
      COUNT(DISTINCT CASE WHEN t.status = 'AWAITING_SELLER_SELECTION' THEN t.id END)
    FROM orbita.ticket t
    LEFT JOIN orbita.ticket_event te
      ON te.ticket_id = t.id
     AND te.event_type IN ('AI_RESOLVED', 'AI_ESCALATED')
    WHERE t.created_at >= '2026-03-01'
    GROUP BY 1, 2

    UNION ALL

    SELECT
      so.seller_id::varchar,
      DATE_TRUNC('week', so.creation_date),
      0, 0, COUNT(DISTINCT so.id),
      0, 0, 0
    FROM orbita.sell_order so
    WHERE so.creation_date >= '2026-03-01'
    GROUP BY 1, 2
  ) combined
  JOIN orbita.seller_buyer_support sbs
    ON sbs.id::varchar = combined.seller_id
  WHERE sbs.is_active = 1
  GROUP BY 1, 2
  ORDER BY 2, 1
  """)
  cols = ["seller","semana","tickets_hubspot","tickets_orbita","ordenes","ai_resolvio","ai_escalo","en_espera"]
  tendencia_por_seller = [dict(zip(cols, row)) for row in cur.fetchall()]
  for r in tendencia_por_seller:
      r["semana"] = r["semana"].strftime("%Y-%m-%d")

  # ── Empacar todo ───────────────────────────────────────────────────────────
  output = {
      "actualizado_en": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
      "tendencia_semanal": tendencia_semanal,
      "tendencia_por_seller": tendencia_por_seller,
      "por_seller_ayer": por_seller,
      "kpis": {**kpis_tickets, **kpis_ia},
      "sentimiento": sentimiento
  }

  cur.close()
  conn.close()

  with open("docs/data.json", "w") as f:
      json.dump(output, f, indent=2, default=str)

  print("data.json generado correctamente")


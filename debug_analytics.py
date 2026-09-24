from app.database import connect_db, init_db, upsert_campaign, upsert_contact, fetch_attempt_rows
from app.analytics import compute_analytics
from app.models import Contact, Disposition
from pathlib import Path
p = Path('debug_analytics.db')
conn = connect_db(p)
init_db(conn)
conn.close()

campaign_id='cmp_debug'
upsert_campaign(connect_db(p), campaign_id, 'test')
contact_a=Contact(campaign_id,'c_001','+1',{})
contact_b=Contact(campaign_id,'c_002','+2',{})
contact_c=Contact(campaign_id,'c_003','+3',{})
for c in (contact_a,contact_b,contact_c): upsert_contact(connect_db(p), c)

conn = connect_db(p)
conn.execute("INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:00Z', 500)", (campaign_id, 'c_001', 1, Disposition.ANSWERED.value))
conn.execute("INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:20Z', 500)", (campaign_id, 'c_002', 1, Disposition.NO_ANSWER.value))
conn.execute("INSERT INTO call_attempts (campaign_id, contact_id, attempt_no, status, disposition, attempt_ts, latency_ms) VALUES (?, ?, ?, 'completed', ?, '2026-01-01T00:00:50Z', 500)", (campaign_id, 'c_003', 1, Disposition.ANSWERED.value))
conn.commit()
rows = fetch_attempt_rows(conn, campaign_id)
print('Fetched rows:')
for r in rows:
    print(dict(r))

print('\ncompute_analytics result:')
print(compute_analytics(conn, campaign_id))
conn.close()

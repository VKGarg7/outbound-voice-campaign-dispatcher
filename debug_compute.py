from app.models import Disposition
from app.database import connect_db
from datetime import datetime

conn = connect_db('debug_analytics.db')
campaign_id='cmp_debug'
rows = conn.execute("""
SELECT contact_id, attempt_no, disposition, attempt_ts
FROM call_attempts
WHERE campaign_id = ?
ORDER BY contact_id, attempt_no
""", (campaign_id,)).fetchall()
print('raw rows:')
for r in rows:
    print(dict(r))

by_contact = {}
for row in rows:
    by_contact.setdefault(row['contact_id'], []).append(row)

for contact_id, rows in by_contact.items():
    print('\ncontact', contact_id)
    print('rows for contact:', rows)
    any_answered = any(r['disposition'] == Disposition.ANSWERED.value for r in rows)
    print('any_answered', any_answered)
    terminal = rows[-1]
    print('terminal', dict(terminal))
    answered_row = next((r for r in rows if r['disposition'] == Disposition.ANSWERED.value), None)
    print('answered_row', answered_row)
    if answered_row is not None and answered_row['attempt_ts']:
        first_row = rows[0]
        print('first_row', first_row)
        if first_row['attempt_ts'] and answered_row['attempt_ts']:
            first_dt = datetime.fromisoformat(first_row['attempt_ts'].replace('Z','+00:00'))
            answered_dt = datetime.fromisoformat(answered_row['attempt_ts'].replace('Z','+00:00'))
            print('first_dt', first_dt, 'answered_dt', answered_dt, 'delta', (answered_dt-first_dt).total_seconds())

conn.close()

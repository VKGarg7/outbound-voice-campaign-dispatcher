import asyncio
from pathlib import Path
from scripts.run_campaign import run_campaign

async def main():
    campaign_id = 'cmp_final'
    db_path = Path('campaign_final.db')
    contacts_count = 300
    concurrency = 10
    max_attempts = 4
    seed = 42

    print(f'Running campaign {campaign_id} with {contacts_count} contacts, concurrency={concurrency}, max_attempts={max_attempts}')
    analytics, total_attempts, max_observed_concurrency, duration, provider = await run_campaign(
        campaign_id=campaign_id,
        db_path=db_path,
        contacts_count=contacts_count,
        concurrency=concurrency,
        max_attempts=max_attempts,
        seed=seed,
    )

    print('\nResults:')
    print('Total attempts (from function):', total_attempts)
    print('Provider max observed concurrency:', max_observed_concurrency)
    print('Analytics connection_rate:', analytics.get('connection_rate'))
    # Validate DB total attempts matches returned value
    from app.database import connect_db
    conn = connect_db(db_path)
    db_count = conn.execute('SELECT COUNT(*) FROM call_attempts WHERE campaign_id = ?', (campaign_id,)).fetchone()[0]
    print('Total attempts (DB count):', db_count)
    conn.close()

    # Basic checks
    assert max_observed_concurrency <= concurrency, 'Observed concurrency exceeds configured concurrency'
    assert total_attempts == db_count, 'Mismatch between returned total_attempts and DB count'

    print('\nAll validations passed for the 300-contact run.')

if __name__ == '__main__':
    asyncio.run(main())

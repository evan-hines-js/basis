use crate::config::IpPoolConfig;
use crate::db::{Db, DbError};

/// Seed configured IP pools into the database on startup.
pub async fn seed_ip_pools(db: &Db, pools: &[IpPoolConfig]) -> Result<(), DbError> {
    for pool in pools {
        db.upsert_ip_pool(pool).await?;
    }
    Ok(())
}

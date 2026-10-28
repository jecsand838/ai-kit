use crate::StateStoreError;

use super::{codec::RECORDS, error::RedbErrorContext};

pub(super) struct RedbSchemaInitializer<'db> {
    db: &'db redb::Database,
    quick_repair: bool,
}

impl<'db> RedbSchemaInitializer<'db> {
    pub(super) fn new(db: &'db redb::Database, quick_repair: bool) -> Self {
        Self { db, quick_repair }
    }

    pub(super) fn initialize(&self) -> Result<(), StateStoreError> {
        let context = RedbErrorContext::new("schema");
        let mut tx = self.db.begin_write().map_err(|err| context.backend(err))?;
        {
            let _table = tx.open_table(RECORDS).map_err(|err| context.backend(err))?;
        }
        if self.quick_repair {
            tx.set_quick_repair(true);
        }
        tx.commit().map_err(|err| context.backend(err))
    }
}

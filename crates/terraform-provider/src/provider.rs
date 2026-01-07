use tf_provider::{schema::Schema, value::ValueEmpty, Diagnostics, Provider};

#[derive(Default)]
pub struct PgmoldProvider;

impl Provider for PgmoldProvider {
    type Config<'a> = ValueEmpty;
    type MetaState<'a> = ValueEmpty;

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        None
    }
}

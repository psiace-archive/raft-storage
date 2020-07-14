pub type CustomStorageError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Error = CustomStorageError;

use std::sync::Arc;
use zarrs::{
    array::{Array, ArrayBuilder},
    storage::AsyncReadableWritableListableStorage,
};
use zarrs_storage::AsyncReadableWritableListableStorageTraits;
const PATH: &str = "examples/example.zarrdb";

async fn make_store(first: bool) -> AsyncReadableWritableListableStorage {
    let mut builder = zarrs_sqlite::TursoStoreBuilder::new(PATH);
    if first {
        builder.create().truncate().created_by("zarrs_sqlite");
    }
    let store = builder.build().await.expect("Failed to build TursoStore");
    let metadata = store
        .read_metadata()
        .await
        .expect("Failed to read metadata");
    println!("Metadata: {:?}", metadata);
    Arc::new(store)
}

async fn make_array(
    store: AsyncReadableWritableListableStorage,
) -> zarrs::array::Array<dyn AsyncReadableWritableListableStorageTraits + 'static> {
    let array = ArrayBuilder::new([16, 16], [8, 8], zarrs::array::data_type::uint8(), 0)
        .build(store, "/")
        .unwrap();
    let data: Vec<_> = (0u8..=255).collect();
    array
        .async_store_array_subset(&[0..16, 0..16], data)
        .await
        .unwrap();
    array.async_store_metadata().await.unwrap();
    array
}

#[tokio::main]
async fn main() {
    env_logger::init();
    {
        let store = make_store(true).await;
        make_array(store.clone()).await;
    }

    let store2 = make_store(false).await;
    let keys = store2.list().await.unwrap();
    println!("Keys: {:?}", keys);
    let array = Array::async_open(store2, "/").await.unwrap();
    let out: Vec<u8> = array
        .async_retrieve_array_subset(&[0..16, 0..16])
        .await
        .unwrap();
    println!("Retrieved data: {:?}", out);
}

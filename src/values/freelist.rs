use lru::LruCache;

type FreelistShard = LruCache<FreelistId, Arc<FreelistBlock>>;
pub struct ValueFreelist {}

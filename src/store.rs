use crate::error::{QuePaxaError, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

pub trait ValueStore<V, B> {
    fn contains(&self, value_id: &V) -> bool;
    fn get(&self, value_id: &V) -> Option<&B>;
    fn insert(&mut self, value_id: V, batch: B);
}

pub trait ValueFetcher<V, B> {
    fn fetch_values(&mut self, value_ids: &[V]) -> Result<Vec<(V, B)>>;
}

/// Verifies that a recorder can safely acknowledge a proposal's value IDs.
///
/// A production implementation should fetch missing values before returning
/// `Ok(())`; returning an error prevents consensus from deciding data the
/// recorder cannot execute.
pub trait ValueAvailability<V>: Send + Sync {
    fn ensure_available(&self, value_ids: &[V]) -> Result<()>;
}

impl<V, F> ValueAvailability<V> for F
where
    F: Fn(&[V]) -> Result<()> + Send + Sync,
{
    fn ensure_available(&self, value_ids: &[V]) -> Result<()> {
        self(value_ids)
    }
}

/// Explicitly permissive availability guard for deterministic simulations.
/// Do not use this in a deployment that decides IDs rather than payloads.
#[derive(Debug, Default)]
pub struct AllowAllAvailability;

impl<V> ValueAvailability<V> for AllowAllAvailability {
    fn ensure_available(&self, _value_ids: &[V]) -> Result<()> {
        Ok(())
    }
}

/// A value-availability guard backed by a shared store and a fetcher.
///
/// Missing IDs are fetched once per call and inserted into `store`. The
/// fetcher must authenticate and verify that each returned payload belongs to
/// its value ID; this adapter rejects unrequested or duplicate returned IDs
/// and refuses to acknowledge a proposal until every requested ID is stored.
pub struct FetchingAvailability<V, B, S, F> {
    store: Arc<Mutex<S>>,
    fetcher: Mutex<F>,
    marker: PhantomData<fn(V, B)>,
}

impl<V, B, S, F> FetchingAvailability<V, B, S, F> {
    pub fn new(store: Arc<Mutex<S>>, fetcher: F) -> Self {
        Self {
            store,
            fetcher: Mutex::new(fetcher),
            marker: PhantomData,
        }
    }
}

impl<V, B, S, F> ValueAvailability<V> for FetchingAvailability<V, B, S, F>
where
    V: Clone + Ord,
    S: ValueStore<V, B> + Send,
    F: ValueFetcher<V, B> + Send,
{
    fn ensure_available(&self, value_ids: &[V]) -> Result<()> {
        let missing = {
            let store = self.store.lock().map_err(|_| {
                QuePaxaError::TransportError("value store lock was poisoned".into())
            })?;
            value_ids
                .iter()
                .filter(|value_id| !store.contains(value_id))
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return Ok(());
        }

        let fetched = self
            .fetcher
            .lock()
            .map_err(|_| QuePaxaError::TransportError("value fetcher lock was poisoned".into()))?
            .fetch_values(&missing)?;
        let requested = missing.iter().cloned().collect::<BTreeSet<_>>();
        let mut returned = BTreeSet::new();
        let mut store = self
            .store
            .lock()
            .map_err(|_| QuePaxaError::TransportError("value store lock was poisoned".into()))?;

        for (value_id, batch) in fetched {
            if !requested.contains(&value_id) || !returned.insert(value_id.clone()) {
                return Err(QuePaxaError::InvalidProposal(
                    "value fetcher returned an unexpected or duplicate value ID".into(),
                ));
            }
            store.insert(value_id, batch);
        }

        if missing.iter().all(|value_id| store.contains(value_id)) {
            Ok(())
        } else {
            Err(crate::QuePaxaError::MissingValue)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryValueStore<V, B> {
    values: BTreeMap<V, B>,
}

impl<V: Ord, B> InMemoryValueStore<V, B> {
    pub fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl<V: Ord, B> ValueStore<V, B> for InMemoryValueStore<V, B> {
    fn contains(&self, value_id: &V) -> bool {
        self.values.contains_key(value_id)
    }

    fn get(&self, value_id: &V) -> Option<&B> {
        self.values.get(value_id)
    }

    fn insert(&mut self, value_id: V, batch: B) {
        self.values.entry(value_id).or_insert(batch);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QuePaxaError;

    #[derive(Default)]
    struct StaticFetcher {
        values: BTreeMap<u64, &'static str>,
        requests: Vec<Vec<u64>>,
    }

    impl ValueFetcher<u64, &'static str> for StaticFetcher {
        fn fetch_values(&mut self, value_ids: &[u64]) -> Result<Vec<(u64, &'static str)>> {
            self.requests.push(value_ids.to_vec());
            Ok(value_ids
                .iter()
                .filter_map(|value_id| self.values.get(value_id).map(|batch| (*value_id, *batch)))
                .collect())
        }
    }

    #[test]
    fn fetching_availability_uses_the_store_before_fetching_missing_values() {
        let store = Arc::new(Mutex::new(InMemoryValueStore::new()));
        store.lock().unwrap().insert(1, "already present");
        let availability = FetchingAvailability::new(
            Arc::clone(&store),
            StaticFetcher {
                values: BTreeMap::from([(2, "fetched")]),
                ..StaticFetcher::default()
            },
        );

        availability.ensure_available(&[1, 2]).unwrap();

        let store = store.lock().unwrap();
        assert!(store.contains(&1));
        assert!(store.contains(&2));
        drop(store);
        assert_eq!(availability.fetcher.lock().unwrap().requests, vec![vec![2]]);
    }

    #[test]
    fn fetching_availability_rejects_an_incomplete_fetch() {
        let store = Arc::new(Mutex::new(InMemoryValueStore::<u64, &'static str>::new()));
        let availability = FetchingAvailability::new(Arc::clone(&store), StaticFetcher::default());

        assert_eq!(
            availability.ensure_available(&[7]).unwrap_err(),
            QuePaxaError::MissingValue
        );
        assert!(!store.lock().unwrap().contains(&7));
    }

    struct UnexpectedFetcher;

    impl ValueFetcher<u64, &'static str> for UnexpectedFetcher {
        fn fetch_values(&mut self, _value_ids: &[u64]) -> Result<Vec<(u64, &'static str)>> {
            Ok(vec![(99, "unrequested")])
        }
    }

    #[test]
    fn fetching_availability_rejects_unrequested_values() {
        let store = Arc::new(Mutex::new(InMemoryValueStore::<u64, &'static str>::new()));
        let availability = FetchingAvailability::new(Arc::clone(&store), UnexpectedFetcher);

        assert!(matches!(
            availability.ensure_available(&[7]),
            Err(QuePaxaError::InvalidProposal(_))
        ));
        assert!(store.lock().unwrap().is_empty());
    }
}

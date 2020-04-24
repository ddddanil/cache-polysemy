{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE TemplateHaskell #-}

module Polysemy.Cache where

import qualified Data.Cache as C
import Data.Function ((&))
import Data.Hashable
import Data.IORef
import Polysemy
import Polysemy.AtomicState
import System.Clock

data Cache k v (m :: * -> *) a where
  Insert :: (Eq k, Hashable k) => k -> v -> Cache k v m ()
  Insert' :: (Eq k, Hashable k) => Maybe TimeSpec -> k -> v -> Cache k v m ()
  Lookup :: (Eq k, Hashable k) => k -> Cache k v m (Maybe v)
  Lookup' :: (Eq k, Hashable k) => k -> Cache k v m (Maybe v)
  Keys :: (Eq k, Hashable k) => Cache k v m [k]
  Delete :: (Eq k, Hashable k) => k -> Cache k v m ()
  FilterWithKey :: (Eq k, Hashable k) => (k -> v -> Bool) -> Cache k v m ()
  Purge :: (Eq k, Hashable k) => Cache k v m ()
  PurgeExpired :: (Eq k, Hashable k) => Cache k v m ()
  Size :: (Eq k, Hashable k) => Cache k v m Int
  DefaultExipration :: (Eq k, Hashable k) => Cache k v m (Maybe TimeSpec)
  SetDefaultExpiration :: (Eq k, Hashable k) => Maybe TimeSpec -> Cache k v m ()

makeSem_ ''Cache

-- | Insert an item into the cache, using the default expiration value of the cache.
insert :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => k -> v -> Sem r ()

-- | Insert an item in the cache, with an explicit expiration value.
insert' :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Maybe TimeSpec -> k -> v -> Sem r ()

-- | Lookup an item with the given key, and delete it if it is expired.
--
-- The function will only return a value if it is present in the cache and if the item is not expired.
--
-- The function will eagerly delete the item from the cache if it is expired.
lookup :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => k -> Sem r (Maybe v)

-- | Lookup an item with the given key, but don't delete it if it is expired.
--
-- The function will only return a value if it is present in the cache and if the item is not expired.
--
-- The function will not delete the item from the cache.
lookup' :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => k -> Sem r (Maybe v)

-- | Return all keys present in the cache.
keys :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Sem r [k]

-- | Delete an item from the cache. Won't do anything if the item is not present.
delete :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => k -> Sem r ()

-- | Keeps elements that satify a predicate (used for cache invalidation). Note that the predicate might be called for expired items.
filterWithKey :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => (k -> v -> Bool) -> Sem r ()

-- | Delete all elements (cache invalidation).
purge :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Sem r ()

-- | Delete all items that are expired.
--
-- This is one big atomic operation.
purgeExpired :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Sem r ()

-- | Return the size of the cache, including expired items.
size :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Sem r Int

-- | Get the default expiration value of newly added cache items.
defaultExipration :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Sem r (Maybe TimeSpec)

-- | Change the default expiration value of newly added cache items.
setDefaultExpiration :: forall k v r. (Eq k, Hashable k, Member (Cache k v) r) => Maybe TimeSpec -> Sem r ()

-- | Run a 'Cache' using 'AtomicState'
runCacheAtomicState ::
  forall k v r a.
  Members '[Embed IO, AtomicState (C.Cache k v)] r =>
  Sem (Cache k v ': r) a ->
  Sem r a
runCacheAtomicState = interpret $ \case
  Insert k v -> do
    cache <- atomicGet
    embed $ C.insert cache k v
  Insert' ts k v -> do
    cache <- atomicGet
    embed $ C.insert' cache ts k v
  Lookup k -> do
    cache <- atomicGet
    embed $ C.lookup cache k
  Lookup' k -> do
    cache <- atomicGet
    embed $ C.lookup cache k
  Keys -> do
    cache <- atomicGet
    embed $ C.keys cache
  Delete k -> do
    cache <- atomicGet
    embed $ C.delete cache k
  FilterWithKey pred -> do
    cache <- atomicGet
    embed $ C.filterWithKey pred cache
  Purge -> do
    cache <- atomicGet
    embed $ C.purge cache
  PurgeExpired -> do
    cache <- atomicGet
    embed $ C.purgeExpired cache
  Size -> do
    cache <- atomicGet
    embed $ C.size cache
  DefaultExipration -> do
    cache <- atomicGet
    pure $ C.defaultExpiration cache
  SetDefaultExpiration ts -> do
    cache <- atomicGet
    atomicPut $ C.setDefaultExpiration cache ts

-- | Alternative version of 'runCacheAtomicState' that uses 'Final' instead of 'Embed'
runCacheAtomicState' ::
  forall k v r a.
  Members '[Final IO, AtomicState (C.Cache k v)] r =>
  Sem (Cache k v ': r) a ->
  Sem r a
runCacheAtomicState' = interpret $ \case
  Insert k v -> do
    cache <- atomicGet
    embedFinal $ C.insert cache k v
  Insert' ts k v -> do
    cache <- atomicGet
    embedFinal $ C.insert' cache ts k v
  Lookup k -> do
    cache <- atomicGet
    embedFinal $ C.lookup cache k
  Lookup' k -> do
    cache <- atomicGet
    embedFinal $ C.lookup cache k
  Keys -> do
    cache <- atomicGet
    embedFinal $ C.keys cache
  Delete k -> do
    cache <- atomicGet
    embedFinal $ C.delete cache k
  FilterWithKey pred -> do
    cache <- atomicGet
    embedFinal $ C.filterWithKey pred cache
  Purge -> do
    cache <- atomicGet
    embedFinal $ C.purge cache
  PurgeExpired -> do
    cache <- atomicGet
    embedFinal $ C.purgeExpired cache
  Size -> do
    cache <- atomicGet
    embedFinal $ C.size cache
  DefaultExipration -> do
    cache <- atomicGet
    pure $ C.defaultExpiration cache
  SetDefaultExpiration ts -> do
    cache <- atomicGet
    atomicPut $ C.setDefaultExpiration cache ts

-- | Run a 'Cache', given a default expiration time.
runCache ::
  forall k v r a.
  Members '[Embed IO] r =>
  Maybe TimeSpec ->
  Sem (Cache k v ': AtomicState (C.Cache k v) ': r) a ->
  Sem r a
runCache ts eff = do
  cache <- embed $ C.newCache ts
  ref <- embed $ newIORef cache
  eff
    & runCacheAtomicState
    & runAtomicStateIORef ref

-- | Alternative version of 'runCache' that uses 'Final' instead of 'Embed'
runCache' ::
  forall k v r a.
  Members '[Final IO] r =>
  Maybe TimeSpec ->
  Sem (Cache k v ': AtomicState (C.Cache k v) ': Embed IO ': r) a ->
  Sem r a
runCache' ts eff = do
  cache <- embedFinal $ C.newCache ts
  ref <- embedFinal $ newIORef cache
  eff
    & runCacheAtomicState
    & runAtomicStateIORef ref
    & embedToFinal

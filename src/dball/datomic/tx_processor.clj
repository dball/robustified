(ns dball.datomic.tx-processor
  (:require [clojure.core.async :as async]
            [datomic.api :as d]
            [io.pedestal.log :as log])
  (:import [java.time Duration]
           [java.util.concurrent BlockingQueue TimeUnit]))

;; 1. reasons txn application txn duplicate id
;; 2. don't throw
;; 3. timeout: resubmit? derive uuid? write to durable queue
;; 4. pass in control
(defn build-txn-submitter
  "Spawns a thread that consumes request maps from the txns channel,
   attempting to apply the :txn to the conn. If a txn fails to apply,
   it logs and error and terminates.

   If the request map contains a :txrs channel, this writes the result
   of applying the transaction to it, either the dereferenced return value from
   transact or the exception thrown in calling or derefencing it.

   This returns a channel which closes when the thread completes."
  [conn reqs]
  (async/thread
    (loop []
      (let [req (async/<!! reqs)]
        (when (some? req)
          (let [{:keys [txn txrs]} req
                txr (try
                      (let [txf (d/transact conn txn)]
                        (try (deref txf)
                             (catch Exception e
                               (log/error :event ::txn-abort
                                          :context :txn-submitter
                                          :error e)
                               e)))
                      (catch Exception e
                        (log/error :event ::txn-timeout
                                   :context ::txn-submitter
                                   :error e)
                        e))]
            (when txrs
              (async/put! txrs txr))
            (when-not (instance? Exception txr)
              (recur))))))))

(defn transact
  "Submits the txn for application, returning true if the txn was accepted,
   though not necessarily applied."
  [reqs txn]
  (async/>!! reqs {:txn txn}))

;; Tim has weird feels
(defn transact!
  "Submits the txn for application and returns the txr if the txn was verifiably
   applied or nil if it was not accepted. If it was accepted but not verifiably
   applied, this raises an exception."
  [reqs txn]
  (let [txrs (async/promise-chan)]
    (when (some? (async/>!! reqs {:txn txn :txrs txrs}))
      (let [txr (async/<!! txrs)]
        (when (instance? Exception txr)
          (throw (ex-info "Transaction error" {:txn txn} txr)))
        txr))))

(defn ensure-marker-id!
  "Ensures the tx report consumer marker attribute exists in the
   database and returns its id"
  [conn marker]
  (or (get (d/attribute (d/db conn) marker) :id)
      (let [txn [{:db/id "marker"
                  :db/ident marker
                  :db/valueType :db.type/long
                  :db/cardinality :db.cardinality/one
                  :db/doc "The latest t value processed by a tx report consumer"}]
            txr @(d/transact conn txn)]
        (get-in txr [:tempids "marker"]))))

(defn find-skipped-txrs
  "Returns a lazy seq of all transactions that occurred after the last one
   consumed by this consumer and before the given transaction report, in
   the same format used by the transaction report queue."
  [marker-id eid conn txr]
  (let [this-t (d/tx->t (get (first (get txr :tx-data)) :tx))
        db (d/db conn)
        log (d/log conn)
        last-t (d/q '[:find ?v . :where [?e ?a ?v] :in $ ?e ?a] db eid marker-id)]
    (for [log-tx (d/tx-range log (inc last-t) this-t)]
      (let [{:keys [t data]} log-tx]
        {:tx-data data
         :db-before (d/as-of db (dec t))
         :db-after (d/as-of db t)}))))

;; consider using offer! instead of onto-chan
;; add top-level try-catch in each async/thread
;; add debug logs
;; spinlock until you've caught up
;; basis-t -> next-t
(defn build-tx-report-consumer
  "Spawns a thread that consumes the transaction report queue and writes
   the reports to the txrs channel, polling at the given interval. When
   the shutdown promise is delivered or this cannot write to the txrs
   channel, the thread terminates.

   After receiving the first transaction report, this will first deliver
   any transaction reports received since the last one it consumed to
   the txrs channel.

   This returns a channel which closes when the thread completes."
  [marker-id mark-id shutdown ^Duration interval conn txrs]
  (let [queue (d/tx-report-queue conn)
        interval-ms (.toMillis ^Duration interval)
        ms-unit TimeUnit/MILLISECONDS
        first? (volatile! true)]
    (async/thread
      (loop []
        (let [txr (try
                    (.poll queue interval-ms ms-unit)
                    (catch InterruptedException e))]
          (when (not (realized? shutdown))
            (when (some? txr)
              (when @first?
                (let [skipped-txrs (find-skipped-txrs marker-id mark-id conn txr)]
                  (async/<!! (async/onto-chan txrs skipped-txrs false)))
                (vreset! first? false))
              (when (some? (async/>!! txrs txr))
                (recur)))))))))

;; uml blocks with channels (buffer, etc.) and data structures
(defn build-tx-report-processor
  "Spawns a thread that processes transaction reports from the txrs channel
   by applying them to the process fn. If it completes without error, this
   transacts the t value as the marker attribute on the mark entity if the
   transaction itself does not assert the marker attribute on the mark
   entity. (This avoids an infinite feedback cycle).

   When the txrs channel closes, or the process throws an exception, or it
   cannot submit the consumer mark txn for transaction, the thread terminates.

   This returns a channel which closes when the thread completes."
  [process marker-id mark-id txrs reqs]
  (let [marking-txr? (fn [txr]
                      (some (fn [[e a v]]
                              (and (= mark-id e) (= marker-id a)))
                            (get txr :tx-data)))]
    (async/thread
      (loop []
        (let [txr (async/<!! txrs)]
          (when (and (some? txr)
                     (try (do (process txr) true)
                          (catch Exception e
                            (log/error :event ::tx-report-processor
                                       :txr txr
                                       :error e))))
            (if (marking-txr? txr)
              (recur)
              (let [t (d/tx->t (get (first (get txr :tx-data)) :tx))]
                (when (transact reqs [[:db/add mark-id marker-id t]])
                  (recur))))))))))

(defn build-tx-system
  [process interval marker-id mark-id conn]
  (let [reqs (async/chan)
        txrs (async/chan)
        shutdown (promise)
        txn-submitter (build-txn-submitter conn reqs)
        txr-consumer (build-tx-report-consumer marker-id mark-id shutdown interval conn txrs)
        txr-processor (build-tx-report-processor process marker-id mark-id txrs reqs)]
    {:close! (fn []
               (deliver shutdown true)
               (async/<!! txr-consumer)
               (async/close! txrs)
               (async/<!! txr-processor)
               (async/close! reqs)
               (async/<!! txn-submitter))}))

(defn create-mark!
  [conn marker]
  (get-in @(d/transact conn [[:db/add "mark" marker 0]]) [:tempids "mark"]))

(defn move-mark!
  [conn marker mark-id t]
  (boolean @(d/transact conn [[:db/add mark-id marker t]])))

(defn get-mark
  [db marker mark-id]
  (get (d/pull db [marker] mark-id) marker))

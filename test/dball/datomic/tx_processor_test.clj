(ns dball.datomic.tx-processor-test
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [datomic.api :as d]
            [dball.datomic.tx-processor :refer :all])
  (:import [java.time Duration]))

(def epoch-t
  1000)

(defn add-datom
  [db eavs datom]
  (let [{:keys [e a v tx added]} datom
        cardinality (get (d/attribute db a) :cardinality)]
    (case added
      true
      (case cardinality
        :db.cardinality/one
        (assoc-in eavs [e a] v)
        :db.cardinality/many
        (update-in eavs [e a] (fnil conj #{}) v))
      false
      (let [entity (case cardinality
                     :db.cardinality/one
                     (dissoc (get eavs e) a)
                     :db.cardinality/many
                     (update (get eavs e) a disj v))]
        (if (seq entity)
          (assoc eavs e entity)
          (dissoc eavs e))))))

(defn get-db-eavs
  [db]
  (let [datoms (remove (fn [datom]
                         (< (d/tx->t (get datom :tx)) epoch-t))
                       (d/datoms db :eavt))]
    (reduce (partial add-datom db) {} datoms)))

(defn get-txrs-eavs
  [txrs]
  (reduce (fn [eavs [db datom]]
            (add-datom db eavs datom))
          {}
          (for [txr txrs
                datom (get txr :tx-data)]
            [(get txr :db-before) datom])))

(def db
  nil)

(deftest test-tx-system
  (let [uri (str "datomic:mem:" (gensym))
        _ (d/create-database uri)
        conn (d/connect uri)
        schema [{:db/ident :player/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db/unique :db.unique/value}]
        _ @(d/transact conn schema)
        marker :sparkfund.datomic/tx-report-consumer-marker
        marker-id (ensure-marker-id! conn marker)
        mark-id (create-mark! conn marker)
        txrs (atom [])
        process (fn [txr] (swap! txrs conj txr))
        system (build-tx-system process (Duration/ofSeconds 1) marker-id mark-id conn)]
    @(d/transact conn [{:player/name "Starbuck"}])
    (Thread/sleep 1000)
    ((get system :close!))
    (is (= 7 (count @txrs)))
    (alter-var-root #'db (constantly (d/db conn)))
    (let [db-eavs (get-db-eavs (d/db conn))
          txr-eavs (add-datom (d/db conn)
                              (get-txrs-eavs @txrs)
                              {:e mark-id
                               :a marker-id
                               :v (get-mark (d/db conn) marker mark-id)
                               :added true})]
      (is (= db-eavs txr-eavs)))))

(s/def ::attr
  (s/keys :req [:db/ident
                :db/cardinality
                :db/valueType]))

(defn user-ident?
  [ident]
  (not= "db" (namespace ident)))

(s/def ::user-attr
  (s/and ::attr
         (fn [attr] (user-ident? (get attr :db/ident)))))

(s/def :db/ident
  keyword?)

(s/def :db/cardinality
  #{:db.cardinality/one :db.cardinality/many})

(s/def :db/valueType
  #{:db.type/string :db.type/long :db.type/boolean})

(s/def ::operation
  #{:db/add :db/retract})

;; Properties
;; 1. if every datom in a complete report processor is transacted to another
;;    datomic, as well as the marker entity, the two current database e-a-v
;;    values should be identical
;; 2. for each txr, the db-before is equivalent to the db-after of the preceding
;;    txr, if any
;; 3. for each txr, the applying the tx-data to the db-before yields the db-after

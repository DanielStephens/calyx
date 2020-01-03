(ns calyx.core
  (:require [com.walmartlabs.lacinia.executor :as executor]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.set :refer [rename-keys]]))


;enum OrderingDirection { ASC, DESC }

;type PageInfo {
;  hasNextPage: Boolean!
;  hasPreviousPage: Boolean!
;  startCursor: String
;  endCursor: String
;}
;
;type X {
;  ...
;}
;
;type XEdge {
;  cursor: String!               // generic
;  node: X!                      // generic
;}
;
;type XConnection {
;  pageInfo: PageInfo!           // generic
;  edges: [XEdge]                // generic
;}
;
;input XFilter {
;  foo: Int,
;  bar: Int,
;  and: XFilter,                 // generic
;  or: XFilter,                  // generic
;
;enum XSort {
;  ...
;}
;
;input XOrdering {               // generic
;  sort: XSort!                  // generic
;  direction: OrderingDirection! // generic
;}
;
;xs(filter: XFilter,             // generic
;   orderBy: [XOrdering!],       // generic
;   first: Int,                  // generic
;   last: Int,                   // generic
;   before: String,              // generic
;   after: String                // generic
;): XConnection!                 // generic

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn spy [x]
  (clojure.pprint/pprint x)
  x)

(defn- distinct-by
  ([by-fn coll]
   (let [step (fn step [xs seen]
                (lazy-seq
                  ((fn [[f :as xs] seen]
                     (when-let [s (seq xs)]
                       (let [seen-f (by-fn f)]
                         (if (contains? seen seen-f)
                           (recur (rest s) seen)
                           (cons f (step (rest s) (conj seen seen-f)))))))
                   xs seen)))]
     (step coll #{}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- with-ns [ns kw]
  (->> kw name (vector ns) (remove nil?) (apply keyword)))

(defn- without-ns [kw]
  (with-ns nil kw))

(defn- calyx-connection-type
  [type]
  (with-ns "calyx.connection" type))

(defn- calyx-edge-type
  [type]
  (with-ns "calyx.edge" type))

(defn- calyx-many-type
  [type]
  (with-ns "calyx.many" type))

(defn- calyx-one-type
  [type]
  (with-ns "calyx.one" type))

(defn- connection-type
  [type]
  (-> type without-ns name (str "Connection") keyword))

(defn- many-type
  [type]
  (-> type without-ns name (str "Many") keyword))

(defn- edge-type
  [type]
  (-> type without-ns name (str "Edge") keyword))

;; external
(def paged connection-type)
(def many many-type)
(def one identity)

(defn- fillout-table-schema
  [table-schema]
  (merge {:resolver (fn [_ctx _args v] v)
          :where (fn [_ctx _args] nil)
          :select (fn [_ctx _args] "*")}
         table-schema))

(defn- fillout-type-schema
  [type-schema]
  (merge {:resolver (fn [_ctx _args v] v)
          :forward (fn [_ctx args parent] (concat (when (map? parent) (into [] parent))
                                                  args))}
         type-schema))

(declare make-type-explicit)

(defn- make-fields-explicit
  [fields]
  (reduce
    (fn [fields [type type-schema]]
      (assoc fields type (make-type-explicit type type-schema)))
    fields
    fields))

(defn- fillout-fields-schema
  [fields-schema]
  (-> {:resolver (fn [_ctx _args v] v)
       :forward (fn [_ctx args parent] (concat (when (map? parent) (into [] parent))
                                               args))}
      (merge fields-schema)
      (update :fields make-fields-explicit)))

(defn- schema-type
  [type-schema]
  (cond
    (:type type-schema) :type
    (:fields type-schema) :fields
    (:table type-schema) :table
    :else :fn))

(defn- make-type-explicit
  [type type-schema]
  (let [t (schema-type type-schema)]
    (cond-> type-schema
            (= :table t) fillout-table-schema
            (= :fields t) fillout-fields-schema
            (-> type-schema :type vector?) (update :type (comp paged first))

            (and (-> type-schema :type some?)
                 (-> type-schema :type keyword? not)
                 (-> type-schema :type vector? not))
            (do (throw (ex-info "reference type was not understood." {:schema-type type
                                                                      :schema type-schema})))

            (= :type t) fillout-type-schema)))

(def is-special-arg?
  (comp #{"filter"
          "orderBy"
          "first"
          "last"
          "before"
          "after"}
        first))

(defn- make-explicit
  [schema]
  (let [new-schema
        (reduce
          (fn [agg [type type-schema]]
            (let [type-schema (make-type-explicit type type-schema)
                  t (schema-type type-schema)
                  [underwriting-m
                   overwriting-m] (case t
                                    :table [{(calyx-connection-type type) type-schema
                                             (calyx-many-type type) {:type (calyx-connection-type type)
                                                                     :resolver (fn [_ctx _args v] (->> v
                                                                                                       :edges
                                                                                                       (map :node)))}
                                             (calyx-edge-type type) {:type (calyx-connection-type type)
                                                                     :forward (fn [_ctx args parent]
                                                                                (concat (when (map? parent) (into [] parent))
                                                                                        args
                                                                                        [[:count 1]]))
                                                                     :resolver (fn [_ctx _args {edges :edges
                                                                                                page-info :pageInfo}]
                                                                                 (if (:hasNextPage page-info)
                                                                                   (throw (ex-info "Edge resolver cannot handle multiple edge results." {:edges edges}))
                                                                                   (first edges)))}
                                             (calyx-one-type type) {:type (calyx-edge-type type)
                                                                    :resolver (fn [_ctx _args edge] (:node edge))}

                                             (connection-type type) {:type (calyx-connection-type type)
                                                                     :forward (fn [_ctx args parent]
                                                                                (concat (when (map? parent) (into [] parent))
                                                                                        (->> args
                                                                                             (remove is-special-arg?))
                                                                                        (->> args
                                                                                             (filter is-special-arg?)
                                                                                             (map (fn [[k v]] [(keyword k) v])))))}
                                             (edge-type type) {:type (calyx-edge-type type)}
                                             (many-type type) {:type (calyx-many-type type)}}

                                            {(without-ns type) {:type (calyx-one-type type)}}]
                                    [{} {type type-schema}])]
              (merge underwriting-m
                     agg
                     overwriting-m)))
          schema
          schema)]
    (if (= new-schema schema)
      schema
      (recur new-schema))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- immediate-dependencies
  [type-schema]
  (case (schema-type type-schema)
    :type [(:type type-schema)]
    :fields (mapcat (comp immediate-dependencies val) (-> type-schema :fields))
    []))

(defn dependencies
  [schema type-schema]
  (let [n (immediate-dependencies type-schema)]
    (lazy-cat n (mapcat #(dependencies schema (get schema %)) n))))

(defn- get-cycles
  [schema]
  (->> schema
       (map (fn [[type type-schema]] (dependencies schema type-schema)
              (conj (dependencies schema type-schema) type)))
       (remove (partial apply distinct?))
       (map (partial take 100))
       (map (fn [[f & more]] (conj (distinct more) f)))
       seq))

(defn- fail-cycles
  [schema]
  (if-let [cycles (get-cycles schema)]
    (throw (ex-info "Dependency cycles detected" {:cycles cycles}))
    schema))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare make-resolver)

(defn- make-type-resolver
  [schema _type type-schema]
  (let [dep (-> type-schema immediate-dependencies first)
        nested-resolver (get schema dep)]
    (fn [ctx args parent]
      (let [args-v (into [] args)
            forward-args ((:forward type-schema) ctx args-v parent)
            nested-value (nested-resolver ctx forward-args nil)]
        ((:resolver type-schema) ctx args-v nested-value)))))

(defn- make-fields-resolver
  [query-fn schema _type type-schema]
  (let [fields (:fields type-schema)
        field-resolvers (->> fields
                             (map (partial apply make-resolver query-fn schema))
                             (map vector (keys fields))
                             (into {}))]
    (fn [ctx args parent]
      (let [args-v (into [] args)
            forward-args ((:forward type-schema) ctx args-v parent)
            field-values (->> field-resolvers
                              (map (fn [[k v]] [k (v ctx forward-args nil)]))
                              (into {}))]
        ((:resolver type-schema) ctx args-v field-values)))))

(defn- decode-cursor
  [cursor]
  (try (json/read-str cursor)
       (catch Exception e nil)))

(defn- encode-cursor
  [cursor]
  (json/write-str cursor))

(defn- make-sql-clauses
  [table & {:keys [select-clauses where-clauses ordering limit]}]
  [(str "SELECT " (if (seq select-clauses)
                    (first select-clauses)
                    "*")
        " FROM " table
        (when (seq (first where-clauses)) " WHERE ") (first where-clauses)
        (when (seq ordering) " ORDER BY ") ordering
        (when limit " LIMIT ?"))
   (concat (second select-clauses)
           (second where-clauses)
           [limit])])

(defn- make-table-resolver
  [query-fn _type type-schema]
  (let [order-schema (:order-by type-schema)
        order-schema-lookup (->> order-schema
                                 (filter #(= 3 (count %)))
                                 (map (fn [[column dir mapping]] [mapping [column dir]]))
                                 (into {}))
        default-ordering (map (partial take 2) order-schema)
        default-ordering-columns (map first default-ordering)
        make-cursor (fn [sql-value] (-> sql-value
                                        (select-keys default-ordering-columns)
                                        encode-cursor))]
    (fn [ctx args parent]
      (let [args-m (into {} args)
            args-v (into [] args)

            default-count 50
            forward-count (some args-m [:count
                                        :first])
            backward-count (:last args-m)
            limit (or forward-count backward-count default-count)
            backwards? (boolean (and (not forward-count)
                                     backward-count))

            ;; ORDER
            order-by-filter (:orderBy args-m)
            defined-ordering (->> order-by-filter
                                  (map (fn [{s :sort d :direction}] (when-let [[col _] (get order-schema-lookup s)]
                                                                      [col d])))
                                  (remove nil?))
            ordering-str (fn [orders]
                           (str/join "," (map (fn [[col dir]] (str col " " (name dir))) orders)))
            forward-ordering (->> (concat defined-ordering
                                          default-ordering)
                                  (distinct-by first))
            backward-ordering (->> forward-ordering
                                   (map (fn [[col dir]] [col ({:ASC :DESC :DESC :ASC} dir)])))
            [major-ordering-str minor-ordering-str] (if backwards?
                                                      [(ordering-str backward-ordering) (ordering-str forward-ordering)]
                                                      [(ordering-str forward-ordering) (ordering-str backward-ordering)])

            ;; WHERE
            ordering-cols (map first forward-ordering)
            get-cursor #(-> args-m
                            %
                            decode-cursor
                            (select-keys ordering-cols))
            after-cursor (get-cursor :after)
            before-cursor (get-cursor :before)
            make-cursor-where-clauses (fn [cursor equality-check]
                                        (when (seq cursor)
                                          [(str/join " AND " (map (fn [[k _]] (str k equality-check "?")) cursor))
                                           (map second cursor)]))
            [major-where-clause
             minor-where-clause
             secondary-where] (if backwards?
                                [(make-cursor-where-clauses before-cursor "<")
                                 (make-cursor-where-clauses before-cursor ">=")
                                 (make-cursor-where-clauses after-cursor ">")]
                                [(make-cursor-where-clauses after-cursor ">")
                                 (make-cursor-where-clauses after-cursor "<=")
                                 (make-cursor-where-clauses before-cursor "<")])
            general-where-clauses ((:where type-schema) ctx args-v)

            ;; LIMIT
            limit-str (when limit (str " LIMIT " limit))

            ;; COMPLETE
            table (:table type-schema)
            major-where [(str/join " AND " (concat (some-> (first general-where-clauses)
                                                           vector)
                                                   (some-> (first major-where-clause)
                                                           vector)))
                         (concat (second general-where-clauses)
                                 (second major-where-clause))]
            minor-where [(str/join " AND " (concat (some-> (first general-where-clauses)
                                                           vector)
                                                   (some-> (first minor-where-clause)
                                                           vector)))
                         (concat (second general-where-clauses)
                                 (second minor-where-clause))]

            base-select ((:select type-schema) ctx args-v)
            included-s "calyx_included"
            major-s "calyx_major"
            [major-select-str
             major-select-args] (make-sql-clauses table
                                                  :select-clauses (if secondary-where
                                                                    [(str base-select
                                                                          ",(" (first secondary-where) ") AS " included-s
                                                                          ",true AS " major-s)
                                                                     (second secondary-where)]
                                                                    [(str base-select
                                                                          ",true AS " included-s
                                                                          ",true AS " major-s)])
                                                  :where-clauses major-where
                                                  :ordering major-ordering-str
                                                  :limit (inc limit))
            [minor-select-str
             minor-select-args] (make-sql-clauses table
                                                  :select-clauses [(str base-select
                                                                        ",false AS " included-s
                                                                        ",false AS " major-s)]
                                                  :where-clauses minor-where
                                                  :ordering minor-ordering-str
                                                  :limit 1)

            select-str (str "(" major-select-str ") UNION (" minor-select-str ")"
                            (when (seq major-ordering-str) " ORDER BY ") major-ordering-str)
            select-args (concat major-select-args minor-select-args)

            rows (query-fn select-str select-args)
            rows (if backwards? (reverse rows) rows)
            has-next-page? (->> rows
                                (filter (fn [r] (and (get r major-s)
                                                     (get r included-s))))
                                count
                                (< limit))
            has-previous-page? (->> rows
                                    (filter (fn [r] (not (get r major-s))))
                                    first
                                    some?)
            edges (->> rows
                       (filter (fn [r] (and (get r major-s)
                                            (get r included-s))))
                       (take limit)
                       (map (fn [r] (dissoc r
                                            major-s
                                            included-s)))
                       (map (fn [r] (let [node ((:resolver type-schema) ctx args-v r)]
                                      {:cursor (make-cursor r)
                                       :node node}))))
            page-info {:hasNextPage has-next-page?
                       :hasPreviousPage has-previous-page?
                       :startCursor (-> edges first :cursor)
                       :endCursor (-> edges last :cursor)}]
        {:pageInfo page-info
         :edges edges}))))

(defn- make-resolver
  [query-fn schema type type-schema]
  (cond
    (:type type-schema) (make-type-resolver schema type type-schema)
    (:fields type-schema) (make-fields-resolver query-fn schema type type-schema)
    (:table type-schema) (make-table-resolver query-fn type type-schema)
    :else type-schema))

(defn- make-resolvers
  [query-fn schema]
  (->> schema
       (mapcat (fn [[type type-schema]]
                 (conj (dependencies schema type-schema) type)))
       reverse
       distinct
       (reduce
         (fn [agg type]
           (assoc agg type (make-resolver query-fn agg type (get agg type))))
         schema)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn resolver
  [query-fn schema]
  (->> schema
       make-explicit
       fail-cycles
       (make-resolvers query-fn)))

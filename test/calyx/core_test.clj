(ns calyx.core-test
  (:require [clojure.test :refer :all]
            [calyx.core :as sut]
            [clojure.java.io :as io]
            [com.rpl.specter :refer :all]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [picomock.core :as pico])
  (:import [clojure.lang ExceptionInfo]))

(defn return-values
  [excluded-minor included-major excluded-major]
  (concat (map #(assoc % "calyx_included" true
                         "calyx_major" true)
               included-major)
          (map #(assoc % "calyx_included" false
                         "calyx_major" true)
               excluded-major)
          (map #(assoc % "calyx_included" false
                         "calyx_major" false)
               excluded-minor)))

(defn make-resolvers
  [schema db-mock db-response]
  (sut/resolver (fn [sql-str sql-args]
                  (db-mock sql-str sql-args)
                  db-response)
                schema))

(defn query
  [calyx-schema resolver-key db-response ctx args & returned-keys]
  (let [db-mock (pico/mock)
        resolvers (make-resolvers calyx-schema db-mock db-response)
        resolver (get resolvers resolver-key)
        response (resolver ctx args nil)
        [s args] (-> db-mock
                     pico/mock-args
                     first)]
    (select-keys
      {:sql-string s
       :sql-args args
       :response response}
      (or (seq returned-keys)
          [:sql-string
           :sql-args
           :response]))))

(deftest table-connection
  (let [calyx-schema {:Account {:table "account"
                                :order-by [["name" :ASC :accountName]
                                           ["id" :ASC]]
                                :where (fn [_ctx _args] [])
                                :select (fn [_ctx _args] "id,name")}}]

    (testing "next page"
      (is (= {:response {:edges [{:cursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                                  :node {"id" "1"
                                         "name" "beatrice"}}
                                 {:cursor "{\"name\":\"charlie\",\"id\":\"2\"}"
                                  :node {"id" "2"
                                         "name" "charlie"}}]
                         :pageInfo {:hasNextPage true
                                    :hasPreviousPage false
                                    :startCursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                                    :endCursor "{\"name\":\"charlie\",\"id\":\"2\"}"}}}
             (query calyx-schema
                    :AccountConnection
                    (return-values nil
                                   [{"id" "1"
                                     "name" "beatrice"}
                                    {"id" "2"
                                     "name" "charlie"}
                                    {"id" "3"
                                     "name" "daniel"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "backwards paging"
      (is (= {:response {:edges [{:cursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                                  :node {"id" "1"
                                         "name" "beatrice"}}
                                 {:cursor "{\"name\":\"charlie\",\"id\":\"2\"}"
                                  :node {"id" "2"
                                         "name" "charlie"}}]
                         :pageInfo {:hasNextPage false
                                    :hasPreviousPage true
                                    :startCursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                                    :endCursor "{\"name\":\"charlie\",\"id\":\"2\"}"}}}
             (query calyx-schema
                    :AccountConnection
                    (return-values [{"id" "0"
                                     "name" "andrew"}]
                                   [{"id" "1"
                                     "name" "beatrice"}
                                    {"id" "2"
                                     "name" "charlie"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "forward query"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account ORDER BY name ASC,id ASC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account ORDER BY name DESC,id DESC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name ASC,id ASC")
              :sql-args [3 1]}
             (query calyx-schema
                    :AccountConnection
                    nil
                    {}
                    {:first 2}
                    :sql-string
                    :sql-args))))

    (testing "backward querying"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account ORDER BY name DESC,id DESC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account ORDER BY name ASC,id ASC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name DESC,id DESC")
              :sql-args [3 1]}
             (query calyx-schema
                    :AccountConnection
                    nil
                    {}
                    {:last 2}
                    :sql-string
                    :sql-args))))

    (testing "custom where clauses"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account WHERE COLUMN = ? ORDER BY name DESC,id DESC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account WHERE COLUMN = ? ORDER BY name ASC,id ASC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name DESC,id DESC")
              :sql-args ["ARG" 3
                         "ARG" 1]}
             (query (assoc-in calyx-schema [:Account :where]
                              (constantly ["COLUMN = ?" ["ARG"]]))
                    :AccountConnection
                    nil
                    {}
                    {:last 2}
                    :sql-string
                    :sql-args))))

    (testing "cursor where clause"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account WHERE name>? AND id>? ORDER BY name ASC,id ASC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account WHERE name<=? AND id<=? ORDER BY name DESC,id DESC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name ASC,id ASC")
              :sql-args ["beatrice" "1" 3
                         "beatrice" "1" 1]}
             (query calyx-schema
                    :AccountConnection
                    nil
                    {}
                    {:first 2
                     :after "{\"name\":\"beatrice\",\"id\":\"1\"}"}
                    :sql-string
                    :sql-args))))

    (testing "say no to hackers"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account WHERE id>? ORDER BY name ASC,id ASC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account WHERE id<=? ORDER BY name DESC,id DESC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name ASC,id ASC")
              :sql-args ["1" 3
                         "1" 1]}
             (query calyx-schema
                    :AccountConnection
                    nil
                    {}
                    {:first 2
                     :after "{\"name do my hack\":\"beatrice\",\"id\":\"1\"}"} ; hacks in the cursor :O
                    :sql-string
                    :sql-args))))))

(deftest table-edge
  (let [calyx-schema {:Account {:table "account"
                                :order-by [["name" :ASC :accountName]
                                           ["id" :ASC]]
                                :where (fn [_ctx _args] [])
                                :select (fn [_ctx _args] "id,name")}}]

    (testing "query"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account ORDER BY name ASC,id ASC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account ORDER BY name DESC,id DESC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name ASC,id ASC")
              :sql-args [2 1]}
             (query calyx-schema
                    :AccountEdge
                    (return-values nil
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :sql-string
                    :sql-args))))

    (testing "one thing found"
      (is (= {:response {:cursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                         :node {"id" "1"
                                "name" "beatrice"}}}
             (query calyx-schema
                    :AccountEdge
                    (return-values nil
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "previous thing found"
      (is (= {:response {:cursor "{\"name\":\"beatrice\",\"id\":\"1\"}"
                         :node {"id" "1"
                                "name" "beatrice"}}}
             (query calyx-schema
                    :AccountEdge
                    (return-values [{"id" "0"
                                     "name" "andrew"}]
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "multiple things found"
      (is (thrown-with-msg? ExceptionInfo
                            #"Edge resolver cannot handle multiple edge results."
                            (query calyx-schema
                                   :AccountEdge
                                   (return-values nil
                                                  [{"id" "1"
                                                    "name" "beatrice"}
                                                   {"id" "2"
                                                    "name" "charlie"}]
                                                  nil)
                                   {}
                                   {:first 2}))))))

(deftest table-node
  (let [calyx-schema {:Account {:table "account"
                                :order-by [["name" :ASC :accountName]
                                           ["id" :ASC]]
                                :where (fn [_ctx _args] [])
                                :select (fn [_ctx _args] "id,name")}}]

    (testing "query"
      (is (= {:sql-string (str
                            ;; Major direction query
                            "(SELECT id,name,true AS calyx_included,true AS calyx_major FROM account ORDER BY name ASC,id ASC LIMIT ?)"
                            " UNION "
                            ;; Minor direction query
                            "(SELECT id,name,false AS calyx_included,false AS calyx_major FROM account ORDER BY name DESC,id DESC LIMIT ?)"
                            ;; Ordering
                            " ORDER BY name ASC,id ASC")
              :sql-args [2 1]}
             (query calyx-schema
                    :Account
                    (return-values nil
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :sql-string
                    :sql-args))))

    (testing "one thing found"
      (is (= {:response {"id" "1"
                         "name" "beatrice"}}
             (query calyx-schema
                    :Account
                    (return-values nil
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "previous thing found"
      (is (= {:response {"id" "1"
                         "name" "beatrice"}}
             (query calyx-schema
                    :Account
                    (return-values [{"id" "0"
                                     "name" "andrew"}]
                                   [{"id" "1"
                                     "name" "beatrice"}]
                                   nil)
                    {}
                    {:first 2}
                    :response))))

    (testing "multiple things found"
      (is (thrown-with-msg? ExceptionInfo
                            #"Edge resolver cannot handle multiple edge results."
                            (query calyx-schema
                                   :Account
                                   (return-values nil
                                                  [{"id" "1"
                                                    "name" "beatrice"}
                                                   {"id" "2"
                                                    "name" "charlie"}]
                                                  nil)
                                   {}
                                   {:first 2}))))))

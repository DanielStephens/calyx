(ns calyx.filter-test
  (:require [clojure.test :refer :all]
            [calyx.filter :as sut]))

(def where
  (sut/where-fn {"max-age" "age <= ?"
                 "min-age" "age >= ?"
                 "age" "age = ?"
                 "name" "name = ?"
                 "arg-fn" (fn [_ctx arg] ["arg = ?" [arg]])
                 "ctx-fn" (fn [ctx _arg] ["ctx = ?" [ctx]])}))

(def a [[:filter {"max-age" 99
                  "min-age" 18
                  "and" {"foo" 3 "or" {"bar" 8}}
                  "or" {"bar" 4
                        "foo" 9
                        "or" {"bar" 7}}}]
        ["foo" 5]
        ["other-foo" 6]
        ["unused" 24]
        ["funky" {:a 53}]])

(deftest where-sql
  #_(testing "simple and"
    (is (= ["(age <= ? AND age >= ?)"
            [90 18]]
           (where nil [["max-age" 90]
                       ["min-age" 18]]))))

  (testing "filter"
    (is (= ["(age <= ? AND age >= ?)"
            [90 18]]
           (where nil [[:filter {"max-age" 90
                                 "min-age" 18}]])))

    (is (= ["((age <= ? AND age >= ?) OR age = ?)"
            [90 18 0]]
           (where nil [[:filter {"max-age" 90
                                 "min-age" 18
                                 "or" {"age" 0}}]])))))

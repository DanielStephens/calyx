(ns calyx.filter
  (:require [clojure.string :as str]
            [clojure.zip :as zip]
            [clojure.walk :as walk]))

(defn spy [x]
  (clojure.pprint/pprint x)
  x)

(def special-filter?
  (comp #{:and "and"
          :or "or"}
        first))

(defn- restructure-filter
  [v]
  {::type :or
   ::children (concat [{::type :and
                        ::children (concat (remove special-filter? v)
                                           (->> v
                                                (filter (comp #{:and "and"} first))
                                                (map (comp restructure-filter second))))}]
                      (->> v
                           (filter (comp #{:or "or"} first))
                           (map (comp restructure-filter second))))})

(defn- restructure
  [args]
  (zip/zipper
    ::type
    ::children
    (fn [node children] (assoc node ::children children))
    {::type :and
     ::children (map (fn [[k v :as f]]
                       (if (= :filter k)
                         (restructure-filter v)
                         f))
                     args)}))

(defn- arg-type
  [arg]
  (or (::type arg)
      :simple))

(defn postwalk [f loc]
  (let [loc (if-some [loc (zip/down loc)]
              (loop [loc loc]
                (let [loc (postwalk f loc)]
                  (if-some [loc (zip/right loc)]
                    (recur loc)
                    (zip/up loc))))
              loc)]
    (f loc)))

(defn- simplify-children
  [branch-node children]
  (let [t (arg-type branch-node)
        children-of-type (fn [t] (filter #(t %) children))

        squashable #(or (= t (arg-type %))
                        (and (::type %)
                             (-> % ::children count (< 2))))
        to-squash (children-of-type squashable)
        others (children-of-type (complement squashable))

        new-children (concat others
                             (mapcat ::children to-squash))]
    (if (= children new-children)
      new-children
      (recur branch-node new-children))))

(defn- simplify
  [m]
  (let [z (zip/root (postwalk
                      (fn [loc]
                        (if (zip/branch? loc)
                          (let [children (zip/children loc)
                                node (zip/node loc)]
                            (zip/replace loc (assoc (zip/node loc)
                                               ::children (simplify-children node children))))
                          loc))
                      m))]
    (case (count (::children z))
      0 nil
      1 (-> z ::children first)
      z)))

(defn sqlise [schema ctx m]
  (cond
    (nil? m) nil

    (::type m)
    (let [join (if (= :and (::type m)) " AND " " OR ")
          parts (remove nil? (map (partial sqlise schema ctx) (::children m)))]
      [(str "("
            (str/join join (map first parts))
            ")")
       (mapcat second parts)])

    (string? (get schema (first m))) [(get schema (first m)) [(second m)]]

    (fn? (get schema (first m))) ((get schema (first m)) ctx (second m))))

(defn where-fn [schema]
  (fn [ctx arg-v]
    (->> arg-v
         restructure
         ;spy
         simplify
         ;spy
         (sqlise schema ctx))))

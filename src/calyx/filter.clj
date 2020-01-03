(ns calyx.filter
  (:require [clojure.string :as str]))

(def special-filter?
  (comp #{:and "and"
          :or "or"}
        first))

(defn- make-arg
  [arg]
  {:id (first arg)
   :value (second arg)})

(defn- restructure-filter
  [f]
  {:or (concat [{:and (concat (->> f
                                   (remove special-filter?)
                                   (map make-arg))
                              (->> f
                                   (filter (comp #{:and "and"} first))
                                   (map (comp restructure-filter second))))}]
               (->> f
                    (filter (comp #{:or "or"} first))
                    (map (comp restructure-filter second))))})

(defn- restructure-stack
  [args]
  (cond
    (#{:and "and"
       :or "or"} (first args)) {(keyword (first args)) (map restructure (second args))}

    (= :filter (first args)) (restructure-filter (second args))

    :else (make-arg args)))

(defn- restructure
  [args]
  {:and (map restructure-stack args)})

(defn- arg-type
  [arg]
  (cond
    (:and arg) :and
    (:or arg) :or
    :else :arg))

(defn- simplify-noops
  [m]
  (cond
    (:id m) [m]
    (< (count (some m [:and :or])) 2) (mapcat simplify-noops (some m [:and :or]))
    :else [{(arg-type m) (mapcat simplify-noops (some m [:and :or]))}]))

(defn- flatten-ops
  [m]
  (let [t (arg-type m)]
    (if (= :arg t)
      [m]
      [{t (mapcat (fn [n]
                    (let [nt (arg-type n)]
                      (cond
                        ;; If same type then raise things up a level, i.e. get rid of the op
                        (= t nt) (mapcat flatten-ops (get n nt))
                        ;; If this is a different op then keep it but simplify down the branch
                        (#{:and :or} nt) [{nt (mapcat flatten-ops (get n nt))}]
                        ;; If this is a simple arg then just simplify it directly
                        :else (flatten-ops n))))
                  (get m t))}])))

(defn combine-ops [m]
  (let [t (arg-type m)]
    (if (= :arg t)
      [m]
      (let [{:keys [arg
                    and
                    or]} (group-by arg-type (get m t))
            n (concat (apply concat arg)
                      (when (seq and) [{:and (apply concat (map :and and))}])
                      (when (seq or) [{:or (apply concat (map :or or))}]))
            _ (println "b" m)
            _ (calyx.core/spy (mapcat combine-ops n))
            _ (println n)]
        [{t (mapcat combine-ops n)}]))))

(defn- simplify
  [m]
  (println "a")
  (->> m
       combine-ops
       first
       calyx.core/spy
       simplify-noops
       first
       calyx.core/spy
       flatten-ops
       first
       calyx.core/spy
       ))

(defn sqlise [schema ctx args]
  (cond
    (or (:and args)
        (:or args))
    (let [and? (:and args)
          join (if and? " AND " " OR ")
          parts (remove nil? (map (partial sqlise schema ctx) (some args [:and :or])))]
      [(str "("
            (str/join join (map first parts))
            ")")
       (mapcat second parts)])

    (string? (get schema (first args))) [(get schema (first args)) [(second args)]]

    (fn? (get schema (first args))) ((get schema (first args)) ctx (second args))))

(defn where-fn [schema]
  (fn [ctx arg-v]
    (->> arg-v
         restructure
         ;calyx.core/spy
         simplify
         calyx.core/spy
         (sqlise schema ctx))))

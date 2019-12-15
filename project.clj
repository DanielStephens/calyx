(defproject calyx "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 ;; GraphQL
                 [com.walmartlabs/lacinia "0.34.0"]
                 ;; Lacinia 0.29.0-rc-1 has a bug that it only declares this as
                 ;; a dev dependency. Ensure it's available as a workaround.
                 ;; https://github.com/walmartlabs/lacinia/issues/224
                 [org.clojure/data.json "0.2.6"]
                 [better-cond "1.0.1"]
                 [com.rpl/specter "1.1.1"]]
  :profiles {:dev {:dependencies [[audiogum/picomock "0.1.11"]]}})

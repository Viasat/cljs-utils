;; Copyright (c) 2024, Viasat, Inc
;; Licensed under EPL 2.0

(ns viasat.util
  (:require [cljs.pprint :refer [pprint]]
            [clojure.string :as S]
            [clojure.walk :refer [postwalk]]
            [promesa.core :as P]
            [cljs-bean.core :refer [->clj]]
            ["util" :refer [promisify]]
            ["fs" :as fs]
            ["child_process" :as cp]
            ["neodoc" :as neodoc]
            ["yaml$default" :as yaml]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Argument processing
(defn clean-opts [arg-map]
  (reduce (fn [o [a v]]
            (let [k (keyword (S/replace a #"^[-<]*([^>]*)[>]*$" "$1"))]
              (assoc o k (or (get o k) v))))
          {} arg-map))

(defn parse-opts [usage argv & [opts]]
  (-> usage
      (neodoc/run (clj->js (merge {:optionsFirst true
                                   :smartOptions true
                                   :argv (or argv [])}
                                  opts)))
      js->clj
      clean-opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; General utilities
(defn snake->pascal [v]
  (S/join
    "" (for [[chr1 & chrN] (-> v .toLowerCase (S/split #"-"))]
         (apply str (.toUpperCase chr1) chrN))))

(defn pascal->snake [v]
  (.toLowerCase
    (S/join "-" (re-seq #"[^a-z]+[a-z]*[^A-Z]*" v))))

(defn trim [s] (S/replace s #"\s*$" ""))

(defn right-pad [s pad]
  (.padEnd (str s) pad " "))

(defn left-pad [s pad]
  (.padStart (str s) pad " "))

(def Eprn     #(binding [*print-fn* *print-err-fn*] (apply prn %&)))
(def Eprintln #(binding [*print-fn* *print-err-fn*] (apply println %&)))
(def Epprint  #(binding [*print-fn* *print-err-fn*] (pprint %)))

(defn fatal [code & args]
  (when (seq args)
    (apply Eprintln args))
  (js/process.exit code))

;; From: https://github.com/LonoCloud/conlink/blob/master/src/conlink/util.cljs#L78
(def INTERPOLATE-RE (js/RegExp. "[$](?:([$])|([_a-z][_a-z0-9]*)|{([_a-z][_a-z0-9]*)(?:(:?[-?])([^}]*))?}|())" "gi"))

(defn interpolate [s env]
  (.replaceAll
    s INTERPOLATE-RE
    (fn [_ escaped named braced sep value invalid offset groups]
      (cond escaped "$"
            named   (get env named "")
            sep     (let [unset? (not (contains? env braced))
                          unset-or-null? (empty? (get env braced nil))
                          env-val (get env braced)]
                      (condp = sep
                        ":-" (if unset-or-null? value env-val)
                        "-"  (if unset? value env-val)
                        ":?" (if unset-or-null? (throw (js/Error. value)) env-val)
                        "?"  (if unset? (throw (js/Error. value)) env-val)))
            braced (get env braced "")
            invalid (str "$" invalid)))))

(defn interpolate-walk [o env]
    (postwalk #(if (string? %) (interpolate % env) %) o))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def exec-promise (promisify cp/exec))
(defn exec [cmd & [opts]]
  (P/let [opts (merge {:encoding "utf8" :stdio "pipe"} opts)
          res (exec-promise cmd (clj->js opts))]
    (->clj res)))
(defn spawn [cmd & [opts]]
  (P/create
    (fn [resolve reject]
      (let [opts (merge {:stdio "pipe" :shell true} opts)
            res (atom {:stdout [] :stderr []})
            res-fn (fn [code]
                     {:code code
                      :stdout (S/join "" (:stdout @res))
                      :stderr (S/join "" (:stderr @res))})
            child (doto (cp/spawn cmd (clj->js opts))
                    (.on "close" (fn [code]
                                   (if (= 0 code)
                                     (resolve (res-fn code))
                                     (reject (res-fn code))))))]
        (when-let [stdout (.-stdout child)]
          (.setEncoding stdout "utf8")
          (.on stdout "data" #(swap! res update :stdout conj %)))
        (when-let [stderr (.-stderr child)]
          (.setEncoding stderr "utf8")
          (.on stderr "data" #(swap! res update :stderr conj %)))))))

(def read-file (promisify fs/readFile))

(def write-file (promisify fs/writeFile))

(defn load-yaml [path]
  (P/-> (read-file path "utf8") yaml/parse ->clj))


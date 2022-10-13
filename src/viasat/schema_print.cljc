(ns viasat.schema-print
  (:require [cljs.pprint :refer [pprint]]
            [cljs-bean.core :refer [->clj]]
            [clojure.string :as S]
            #?(:org.babashka/nbb ["easy-table$default" :as Table]
               :cljs             ["easy-table" :as Table])))

(defn trim [s] (S/replace s #"\s*$" ""))

(defn get-in++
  "Advanced get-in.
    Params: [obj [p1 ... pN]]
    Where: pX is a scalar or [key-fn match val-fn]

  Takes an object and sequence path elements. If path element p is
  a scalar then a simple lookup/get is performed at the current cursor
  to get a new cursor. If p is a vector, then it has the form [key-fn
  match val-fn] and the cursor is assumed to be a collection.  The
  resulting cursor is the result of applying val-fn to the first
  element of the cursor where (= match (key-fn %)) is true. If match
  is a function then the test is (match (key-fn %))."
  [obj [p & path]]
  (if p
    (recur (if (vector? p)
             (let [[kfn m vfn] p
                   mfn (if (fn? m) m #(= m %))]
               (vfn (some #(if (mfn (kfn %)) % nil) obj)))
             (get obj p))
           path)
    obj))

(def DEFAULT-TIME-REGEX #"Time$|-time$")

(defn format-field [{:keys [verbose field-max time-regex]
                     :or {field-max 40
                          time-regex DEFAULT-TIME-REGEX}} k v]
  (cond
    (re-seq time-regex (name k))
    (if (contains? #{0 "0" "" nil} v)
      nil
      (try (-> v js/Date. .toISOString)
           (catch :default e
             (-> v js/parseInt (* 1000) js/Date. .toISOString))))

    (and (not verbose) (string? v) (> (count v) field-max))
    (str (.substr v 0 (- field-max 3)) "...") ;; shorten

    :else
    v))

(defn scalar-field [xs k] (every? #(not (coll? %)) (map #(get % k) xs)))
(defn scalar-fields [xs] (filter #(scalar-field xs %) (keys (first xs))))
(defn coll-fields [xs] (filter #(not (scalar-field xs %)) (keys (first xs))))

(defn uniq-by [uniq data]
  (loop [result []
         found #{}
         [row & data] data]
    (if row
      (let [jv (get row uniq)]
        (if (contains? found jv)
          (recur result found data)
          (recur (conj result row) (conj found jv) data)))
      result)))

(defn arg->keyword [arg]
  (keyword (second (first (re-seq #"^:?(.*)$" arg)))))

(defn schema-print [data schema opts]
  (let [[data schema opts] (map ->clj [data schema opts]) ;; JS lib compat
        {:keys [dbg no-headers fields sort rsort edn json pretty short]
         :or {dbg identity}} opts
        ;; Update schema from opts keys if given
        schema (merge schema
                      (select-keys opts [:verbose :field-max :time-regex]))
        schema (cond-> schema
                 fields (assoc :fields (map keyword (S/split fields #"[, ]")))
                 sort   (assoc :sort (keyword sort))
                 rsort  (assoc :rsort (keyword rsort)))
        raw? (-> schema :extract last :action (= :raw))]
    (cond
      (string? data)
      (println data)

      edn
      (if pretty (pprint data) (prn data))

      (or json raw?)
      (if pretty
        (println (js/JSON.stringify (clj->js data) nil 2))
        (println (js/JSON.stringify (clj->js data))))

      (string? (first data))
      (println (S/join "\n" data))

      short
      (doseq [x (map #(get-in++ % [(first schema)]) data)]
        (println x))

      :else
      (let [fields (or (:fields schema)
                       (do
                         (dbg "Skipping non-scalar fields:"
                              (coll-fields data))
                         (scalar-fields data)))
            k-paths (for [s fields] (if (keyword? s) [s [s]] s))
            mung #(reduce (fn [m [k path]]
                            (let [fv (format-field schema k (get-in++ % path))]
                              (assoc m k fv)))
                          % k-paths)
            rows (map mung data)
            rows (if-let [s (:sort schema)] (sort-by s rows) rows)
            rows (if-let [s (:rsort schema)] (reverse (sort-by s rows)) rows)
            rows (if-let [u (:uniq schema)] (uniq-by u rows) rows)
            table (Table.)]

        (doseq [row rows]
	  (doseq [[k path] k-paths]
	    (.cell table (name k) (get row k)))
	  (.newRow table))

        (if no-headers
          (println (trim (.print table)))
          (println (trim (.toString table))))))))


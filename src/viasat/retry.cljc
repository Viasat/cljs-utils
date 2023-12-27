(ns viasat.retry
  (:require [promesa.core :as P]))

(def Eprn     #(binding [*print-fn* *print-err-fn*] (apply prn %&)))

;; Promise/Promesa-based retry
(defn retry
  "Repeat action-fn (promise or plain function) until it returns truthy.
  Optional opts:
    :max-retries - Maxiumum number of retries. On last retry always
                   returns action-fn result (whether truty or not)
                   [default: infinite]
    :delay-ms    - Milliseconds to delay between attempts
                   [default: 1000]
    :delay-fn    - Fn taking delay-ms and optional opts. Delays, then
                   returns a Promise with a map to merge onto opts
                   [default: P/delay]
    :check-fn    - Check result of action-fn. [default: identity]
  "
  [action-fn & [opts]]
  (let [opts (merge {:attempt 0 :delay-ms 1000 :verbose false
                     :delay-fn P/delay :check-fn identity} opts)]
    (P/loop [opts opts]
      (P/let [{:keys [attempt max-retries delay-ms delay-fn check-fn]} opts
              res (action-fn)]
        (when (:verbose opts) (Eprn :retry  :res res :opts opts))
        (if (or (check-fn res) (and max-retries (>= attempt max-retries)))
          res
          (P/let [new-opts (P/->> opts (delay-fn delay-ms) (merge opts))]
            (P/recur (assoc new-opts :attempt (inc attempt)))))))))

(defn retry-times
  "Retry action-fn up to max-retries."
  [action-fn max-retries & [opts]]
  (retry action-fn (assoc opts :max-retries max-retries)))

(defn retry-backoff
  "Retry action-fn with :delay-ms that doubles after attempt. Never
  delays more than :max-backoff [default: 60000]."
  [action-fn & [{:as opts :keys [max-backoff]
                 :or {max-backoff 60000}}]]
  (P/let [df #(P/do (P/delay %1)
                    (assoc %2 :delay-ms (min max-backoff (max 1 (* 2 %1)))))]
    (retry action-fn (assoc opts :delay-fn df) )))


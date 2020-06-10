(ns com.breezeehr.open-api.dynamic-client-v2
  (:require [com.breezeehr.open-api.dynamic-client :as dc]
            [aleph.http :as http]))

(defn make-method-v2
  [& args] args)

(defn create-client
  [{:keys [base-url version]}]
  (for [[path {:keys [parameters] :as config}] (->> {:method           :get
                                                     :url              (str base-url "/openapi/" version)
                                                     :throw-exceptions false
                                                     :as               :json}
                                                 http/request
                                                 deref
                                                 :body
                                                 :paths)
        ;;TODO better names
        [k v] config :when (not= k :parameters)]
    ;;[base-url parameters path]
    ;;TODO make-method-v2
    (make-method-v2 base-url parameters path)))

(comment
  (def client (create-client {:base-url "http://127.0.0.1:8001" :version "v2"}))




  ;; for reference this is what the first arg to make-method looks like
  (first @dc/d)
;; => ["http://127.0.0.1:8001"
;;     [{"uniqueItems" true,
;;       "type" "string",
;;       "description" "name of the APIService",
;;       "name" "name",
;;       "in" "path",
;;       "required" true}
;;      {"uniqueItems" true,
;;       "type" "string",
;;       "description" "If 'true', then the output is pretty printed.",
;;       "name" "pretty",
;;       "in" "query"}]
;;     "/apis/apiregistration.k8s.io/v1beta1/apiservices/{name}/status"]

  )

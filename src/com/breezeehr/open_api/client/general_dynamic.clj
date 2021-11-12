(ns com.breezeehr.open-api.client.general-dynamic
  (:require
    [com.breezeehr.open-api.definition :refer [spec-methods
                                               get-openapi-spec]]
    [aleph.http :as http]
    [clj-wrap-indent.core :as wrap]
    [cheshire.core]))

(defn fast-select-keys [map ks]
  (->
    (reduce
      (fn [acc k]
        (if-some [val (find map k)]
          (conj! acc val)
          acc))
      (transient {})
      ks)
    persistent!
    (with-meta (meta map))))


(defn make-path-fn [{:strs [parameters path operationId]
                     :as   method-discovery}]
  (let [path-params  (into {}
                           (comp
                             (filter (comp #(= % "path") #(get % "in")))
                             (map #(get % "name"))
                             (map (juxt keyword #(str "{" % "}"))))
                           parameters)]
    (fn [vals]
      (reduce-kv
        (fn [acc parameter-key match]
          (let [nv (get vals parameter-key)]
            (assert nv (str "your input " (pr-str vals) " must contains key " parameter-key " for " operationId " path."))
            (clojure.string/replace
              acc
              match
              nv)))
        path
        path-params))))

(defn make-key-sel-fn [{:strs [parameters path operationId]
                        :as   method-discovery}]
  (let [query-ks     (into []
                           (comp
                             (filter (comp #(= % "query") #(get % "in")))
                             (map (comp keyword #(get % "name"))))
                           parameters)]
    (fn [m]
      (fast-select-keys m query-ks))))
(defn augment-request [request {:keys [pool get-token-fn]
                                :as client}]
  (cond-> request
    pool (assoc :pool pool)
    get-token-fn (assoc-in [:headers :authorization] (str "Bearer " (get-token-fn)))))

(defn request-body-encoding [{content "content"}]
  (let [[content-type data] (-> content first)])
  )

(defn new-operation-request-fn [{:strs [servers parameters requestBody
                                        httpMethod operationId]
                                 :as method-discovery}]
  (let [init-map     {:method httpMethod
                      :as     :json}
        path-fn      (make-path-fn method-discovery)
        key-sel-fn   (make-key-sel-fn method-discovery)
        body-params  (into []
                           (comp
                             (filter (comp #(= % "body") #(get % "in"))))
                           parameters)
        requestBody (or (when-some [x (first body-params)]
                          {"content"  {"application/json" {}}
                           "required" true})
                        requestBody)
        baseUrl (get (first servers) "url")]
    (assert baseUrl)
    {:id          operationId
     :description (get method-discovery "description")
     :parameters  parameters
     :request     (fn [client op]
                    ;;(prn method-discovery)
                    (let [enc-body (:request op)
                          content (get requestBody "content")
                          content-type (or (:content-type op)
                                           (and (= 1 (count content))
                                                (some-> content
                                                        first key)))]
                      (when (get requestBody "required")
                        (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                        (assert content-type (str "Request must suply content-type for operation " (:op op))))
                      (-> init-map
                          (assoc :url (str baseUrl (path-fn op)))

                          (assoc :query-params (key-sel-fn op)
                                 :save-request? true
                                 :throw-exceptions false)
                          (augment-request client)
                          (cond->
                            (:request op)
                            (assoc :form-params (:request op))
                            content-type
                            (assoc :content-type (case content-type
                                                   "application/json" :json
                                                   "application/x-www-form-urlencoded" :x-www-form-urlencoded))))))}))

(defn init-client [config]
  config)

(defn add-operation-support [client api-discovery]
  (assoc client ::ops (into {}
                            (map (juxt
                                   #(keyword (get % "operationId"))
                                   new-operation-request-fn))
                            (spec-methods api-discovery))))

(defn operation-dynamic-client
  ([config]
   (let [client (init-client config)
         api-discovery
         (get-openapi-spec config)]
     (add-operation-support client api-discovery)))
  ([config base-uri path]
   (let [client (init-client config)
         api-discovery
         (get-openapi-spec client base-uri path)]
     (add-operation-support client api-discovery))))

(defn print-params [params]
  (doseq [{:strs          [required name] opdesc "description"
           {:strs [type]} "schema"} params]
    (println "      " name
             " type: "
             type
             (if required
               " required "
               " optional "))
    (when opdesc
      (wrap/println opdesc 80 14))))

(defn ops [client]
  (run!
    (fn [[id {:keys [description parameters] :as x}]]
      (println "* " id)
      (when-some [params (not-empty (filter (comp #{"path"} #(get % "in")) parameters))]
        (println "   path-parameters:")
        (print-params params))
      (when-some [params (not-empty (filter (comp #{"query"} #(get % "in") ) parameters))]
        (println "   query-parameters:")
        (print-params params))
      (println "   description:")
      (wrap/println description))
    (->> client ::ops
         (sort-by key))))

(defn request [client {:keys [op] :as operation}]
  (let [opfn (-> client ::ops (get op) :request)]
    (assert opfn (str op
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (opfn client operation)))

(defn invoke [client {:keys [op] :as operation}]
  (let [r (request client operation)]
    (http/request r)))


(comment
  (def base-url "http://127.0.0.1:8001")

  (def api-data (get-openapi-spec {} base-url "/openapi/v2"))


  (keys api-data)
  api-data
  (def kube-op-client (operation-dynamic-client {} base-url "/openapi/v2"))
  )



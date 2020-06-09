(ns com.breezeehr.open-api.dynamic-client
  (:require [cheshire.core :as json]
            [aleph.http :as http]
            [cemerick.url :as url]
            [clojure.string :as str]))

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

(def lastmessage (atom nil))


(defn make-path-fn [path path-params id]
  (fn [vals]
    (reduce-kv
      (fn [acc parameter v]
        (let [nv (get vals parameter)]
          (assert nv (str "your input " (pr-str vals) " must contains key " parameter " for " id " path."))
          (clojure.string/replace
            acc
            (str "{" (name parameter) "}")
            nv)))
      path
      path-params)))

(def d (atom nil))


;;TODO loop over content-type and produce a function for Patch "application/apply-patch+yaml" & Update "TODO"
;; content type is in the produces e.g [ "application/json" "application/yaml" "application/vnd.kubernetes.protobuf" "application/json;stream=watch" "application/vnd.kubernetes.protobuf;stream=watch" ]
(defn make-method [{:keys [baseUrl] :as api-discovery}
                   upper-parameters
                   {:strs [httpMethod path parameters operationId produces]
                    :as   method-discovery}]
  (reset! d {:api-discovery api-discovery :upper-parameters upper-parameters :method-discovery method-discovery})
  ;(prn baseUrl operationId (keys method-discovery) path)
  (let [parameters (into upper-parameters parameters)
        init-map     {:method httpMethod
                      :as     :json}
        path-params  (into {}
                           (comp
                             (filter (comp #(= % "path") #(get % "in")))
                             (map (juxt (comp keyword #(get % "name")) identity)))
                           parameters)
        path-fn      (make-path-fn path path-params operationId)
        query-params (into {}
                           (comp
                             (filter (comp #(= % "query") #(get % "in")))
                             (map (juxt (comp keyword #(get % "name")) identity)))
                           parameters)
        body-params (into []
                           (comp
                             (filter (comp #(= % "body") #(get % "in"))))
                           parameters)
        request      (first body-params)
        query-ks     (into [] (map key) query-params)
        key-sel-fn   (fn [m]
                       (fast-select-keys m query-ks))]
    ;(prn (keys method-discovery))
    [(keyword operationId) {:id          operationId
                            :description (get method-discovery "description")
                            :preform
                                         (fn [client op]
                                           (prn method-discovery)
                                           (-> init-map
                                               (assoc :url (str baseUrl (path-fn op)))

                                               (assoc :query-params (key-sel-fn op)
                                                      :aleph/save-request-message lastmessage
                                                      :throw-exceptions false)
                                               (cond->
                                                 request (assoc :body (let [enc-body (:request op)]
                                                                        (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                                                                        (doto (cheshire.core/generate-string enc-body)
                                                                          prn))))
                                               (doto prn)
                                               http/request))}]))

@d

(defn prepare-methods [api-discovery path parameters methods]
  (reduce-kv
    (fn [acc httpMethod method-data]
      (conj acc (make-method
                  api-discovery
                  parameters
                  (assoc method-data
                    "httpMethod" httpMethod
                    "path" path))))
    {}
    methods))

(defn prepare-paths [api-discovery paths]
  (reduce-kv
    (fn [acc path {:strs [parameters] :as premethod}]
      (into acc (prepare-methods
                  api-discovery
                  path
                  parameters
                  (dissoc premethod "parameters"))))
    {}
    paths))

(defn init-client [config]
  config)

(defn get-openapi-spec [client base-url path]
  (->
    {:method :get
     :url    (str base-url path)}
    (assoc :throw-exceptions false
           ;:aleph/save-request-message lastmessage
           :as :json-string-keys)
    http/request
    deref
    :body
    (assoc :baseUrl base-url)))

(defn dynamic-create-client
  ([config base-uri path]
   (let [client (init-client config)
         api-discovery
                (get-openapi-spec client base-uri path)]
     (assoc client
       ;:api api
       :ops (prepare-paths
              api-discovery
              (get api-discovery "paths"))))))

(defn ops [client]
  (run!
    (fn [[id {:keys [description]}]]
      (println "* " id)
      (print description)
      (println \newline))
    (->> client :ops
         (sort-by key))))

(defn invoke [client {:keys [op] :as operation}]
  (let [opfn (-> client :ops (get op) :preform)]
    (assert opfn (str op
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (opfn client operation)))

(comment
  (def base-url "http://127.0.0.1:8001")

  (def api-data (get-openapi-spec {} base-url "/openapi/v2"))

  (keys api-data)

  (def kubeapi (dynamic-create-client {} base-url "/openapi/v2"))

  (ops kubeapi)

  (invoke kubeapi {:op :listCoreV1NamespacedPod
                   :namespace "production"})
  )

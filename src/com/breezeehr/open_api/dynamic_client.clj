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

(defn patch->apply [operation]
  (str/replace-first operation #"^patch" "apply")
  )

(defn make-method [{:keys [baseUrl] :as api-discovery}
                   upper-parameters
                   path]
  ;(prn baseUrl operationId (keys method-discovery) path)
  (fn [acc httpMethod {:strs [parameters operationId]
                       :as   method-discovery}]
    (let [parameters   (into upper-parameters parameters)
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
          body-params  (into []
                             (comp
                               (filter (comp #(= % "body") #(get % "in"))))
                             parameters)
          request      (first body-params)
          query-ks     (into [] (map key) query-params)
          key-sel-fn   (fn [m]
                         (fast-select-keys m query-ks))]
      ;(prn (keys method-discovery))
      (cond->
        (assoc acc (keyword operationId) {:id          operationId
                                          :description (get method-discovery "description")
                                          :request
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
                                                                                        prn))))))})
        (some #(= %  "application/apply-patch+yaml") (get method-discovery "consumes"))
        (assoc (keyword (patch->apply operationId)) (do
                                                   (prn (patch->apply operationId))
                                                   {:id          operationId
                                                   :description (get method-discovery "description")
                                                   :request
                                                                (fn [client op]
                                                                  (prn method-discovery)
                                                                  (-> init-map
                                                                      (assoc :url (str baseUrl (path-fn op)))

                                                                      (assoc
                                                                        :query-params (key-sel-fn op)
                                                                        :content-type "application/apply-patch+yaml"
                                                                        :aleph/save-request-message lastmessage
                                                                        :throw-exceptions false)
                                                                      (cond->
                                                                          request (assoc :body (let [enc-body (:request op)]
                                                                                                 (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                                                                                                 (doto (cheshire.core/generate-string enc-body)
                                                                                                   prn))))))}))))))

(defn prepare-methods [api-discovery path parameters methods]
  (reduce-kv
    (make-method api-discovery parameters path)
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

(defn request [client {:keys [op] :as operation}]
  (let [opfn (-> client :ops (get op) :request)]
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

  (def kubeapi (dynamic-create-client {} base-url "/openapi/v2"))

  (ops kubeapi)

  (invoke kubeapi {:op :listCoreV1NamespacedPod
                   :namespace "production"})


  ;; Patch dromon deployment

  ;; *  :patchAppsV1NamespacedDeployment
  ;; partially update the specified Deployment


  ;; deploy kuberentes
  ;;TODO add request body
  (invoke kubeapi {:op        :patchAppsV1NamespacedDeployment
                   :namespace "development"})

 
  )

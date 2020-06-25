(ns com.breezeehr.open-api.dynamic-client
  (:require [cheshire.core :as json]
            [aleph.http :as http]
            [byte-streams :as bs]
            [cheshire.core :as json]
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

(defn patch->apply [operation]
  (str/replace-first operation #"^patch" "apply"))

(defn operation-indexer [acc {:strs [operationId]} data]
  (assoc acc (keyword operationId) data))

(def after-name #"(.*)\{name\}(.*)")

(defn kube-kind-indexer [acc {{:strs [group version kind] :as gvc}
                              "x-kubernetes-group-version-kind"
                              path "path"} data]
  (let [sub (not-empty (first (drop 2 (re-matches after-name path))))
        there (get-in acc [kind (if (and group (not (empty? group)))
                                (str group "/" version)
                                version)
                           sub])]
    (assert (not there) (pr-str path))
    (if (and gvc (not sub))
      (assoc-in acc [kind (if (and group (not (empty? group)))
                            (str group "/" version)
                            version)
                     sub] data)
      acc)))

(defn kube-kind-method-indexer [acc {{:strs [group version kind]
                                      :as   gvc}
                                     "x-kubernetes-group-version-kind"
                                     path "path"
                                     httpMethod "httpMethod"
                                     :as method-data} data]
  (let [sub   (not-empty (first (drop 2 (re-matches after-name path))))
        there (get-in acc [kind (if (and group (not (empty? group)))
                                  (str group "/" version)
                                  version)
                           sub])]
    (assert (not there) (pr-str path))
    (if (and gvc (not sub))
      (assoc-in acc [kind (if (and group (not (empty? group)))
                            (str group "/" version)
                            version) httpMethod] data)
      (do                                                   ;(prn method-data)
        acc))))

(defn default-request-fn [{:strs [baseUrl parameters httpMethod operationId] :as method-discovery}]
  (let [init-map     {:method httpMethod
                      :as     :json}
        path-fn      (make-path-fn method-discovery)
        key-sel-fn (make-key-sel-fn method-discovery)
        body-params  (into []
                           (comp
                             (filter (comp #(= % "body") #(get % "in"))))
                           parameters)
        request      (first body-params)]
    {:id          operationId
     :description (get method-discovery "description")
     :request (fn [client op]
                ;;(prn method-discovery)
                (-> init-map
                    (assoc :url (str baseUrl (path-fn op)))

                    (assoc :query-params (key-sel-fn op)
                           :save-request? true
                           :throw-exceptions false)
                    (cond->
                      request (assoc :body (let [enc-body (:request op)]
                                             (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                                             (doto (cheshire.core/generate-string enc-body)
                                               prn))))))})
  )

(defn apply-request-fn [{:strs [baseUrl parameters httpMethod operationId token] :as method-discovery}]
  (when (some #(= % "application/apply-patch+yaml") (get method-discovery "consumes"))
    (let [init-map    {:method httpMethod
                       :as     :json}
          path-fn     (make-path-fn method-discovery)
          key-sel-fn  (make-key-sel-fn method-discovery)
          body-params (into []
                            (comp
                              (filter (comp #(= % "body") #(get % "in"))))
                            parameters)
          request     (first body-params)]
      {:id          (patch->apply operationId)
       :description (get method-discovery "description")
       :request     (fn [client op]
                      ;;(prn method-discovery)
                      (-> init-map
                          (assoc :url (str baseUrl (path-fn op)))
                          (assoc-in [:headers :authorization] (str "Bearer " token))

                          (assoc :query-params (key-sel-fn op)
                                 :save-request? true
                                 :content-type "application/apply-patch+yaml"
                                 :throw-exceptions false)
                          (cond->
                            request (assoc :body (let [enc-body (:request op)]
                                                   (assert enc-body (str "Request cannot be nil for operation " (:op op)))
                                                   (doto (cheshire.core/generate-string enc-body)
                                                     prn))))))}))
  )

(defn make-method [{:keys [baseUrl] :as api-discovery}
                   upper-parameters
                   path
                   {:keys [make-request-fn indexer ]}
                   {:keys [token]}]
  (fn [acc httpMethod method-discovery]
    (let [method-discovery (-> method-discovery
                               (update  "parameters" into upper-parameters)
                               ;;TODO will need to be added conditionally
                               (assoc "path" path
                                      "token" token
                                      "baseUrl" baseUrl
                                      "httpMethod" httpMethod))]
      (if-some [req-fn (make-request-fn method-discovery)]
        (indexer acc method-discovery req-fn)
        acc))))

(defn prepare-methods [acc api-discovery path parameters method-generator methods opts]
  (reduce-kv
    (make-method api-discovery parameters path method-generator opts)
    acc
    methods))

(defn prepare-paths [api-discovery method-generator paths opts]
  (reduce-kv
    (fn [acc path {:strs [parameters] :as premethod}]
      (prepare-methods
        acc
        api-discovery
        path
        parameters
        method-generator
        (dissoc premethod "parameters")
        opts))
    {}
    paths))

(defn init-client [config]
  config)

(defn get-openapi-spec [client base-url path]
  (->
    {:method :get
     :url    (str base-url path)}
    (assoc :throw-exceptions false
           :save-request? true
           :as :json-string-keys)
    http/request
    deref
    :body
    (assoc :baseUrl base-url)))


(def default-method-generator {:index :ops
                               :indexer operation-indexer
                               :make-request-fn default-request-fn})

(defn dynamic-create-client
  ([config base-uri path {:keys [token] :as opt}]
   (let [client (init-client config)
         api-discovery
                (get-openapi-spec client base-uri path)
         method-generators (get config :method-generators
                                [default-method-generator])]
     (reduce
       (fn [acc method-generator]
         (assoc acc
           (:index method-generator)

           (prepare-paths
                  api-discovery
                  method-generator
                  (get api-discovery "paths")
                  opt))
         )
       client
       method-generators)
     )))

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


(defn kube-apply-request [client operation]
  (let [{:keys [kind apiVersion sub]} (:request operation)
        opfn (-> client :apply (get kind) (get apiVersion) (get sub) :request)]
    (assert opfn (str kind apiVersion
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (-> client
        (opfn operation)
        )))

(defn kube-apply [client operation]
  (let [r (kube-apply-request client operation)]
    (http/request r)))

(defn kube-request [client method operation]
  (let [{:keys [kind apiVersion]} (:request operation)
        opfn (-> client :http-method (get kind) (get apiVersion) (get method) :request)]
    (assert opfn (str kind " " apiVersion " " method
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (opfn client operation)))

(defn kube [client method operation]
  (let [r (kube-request client method operation)]
    (http/request r)))

(comment
  (def base-url "http://127.0.0.1:8001")

  (def api-data (get-openapi-spec {} base-url "/openapi/v2"))


  (keys api-data)
  api-data


  (def kubeapi (dynamic-create-client {:method-generators
                                       [default-method-generator
                                        {:index :apply
                                         :indexer kube-kind-indexer
                                         :make-request-fn apply-request-fn}
                                        {:index :http-method
                                         :indexer kube-kind-method-indexer
                                         :make-request-fn default-request-fn}]}
                                      base-url "/openapi/v2" {:token "client token"}))

  (ops kubeapi)

  (invoke kubeapi {:op :listCoreV1NamespacedPod
                   :namespace "production"})


  (def dromon-service
    {:apiVersion "v1",
     :kind       "Service",
     :metadata   {:name "dromon", :labels {:name "dromon", :app "dromon"}},
     :spec
     {:type     "NodePort",
      :ports    [{:name "dromon", :port 443, :protocol "TCP", :targetPort 8443}],
      :selector {:app "dromon", :name "dromon"}}})

  (kube-apply-request
   kubeapi
   {:name         "dromon"
    :namespace    "development"
    :fieldManager "testmanager"
    :request      dromon-service}
   "token" )

;; => {:method "patch",
;;     :as :json,
;;     :save-request? true,
;;     :headers {:authorization "Bearer token"},
;;     :url "http://127.0.0.1:8001/api/v1/namespaces/development/services/dromon",
;;     :query-params {:fieldManager "testmanager"},
;;     :content-type "application/apply-patch+yaml",
;;     :throw-exceptions false,
;;     :body
;;     "{\"apiVersion\":\"v1\",\"kind\":\"Service\",\"metadata\":{\"name\":\"dromon\",\"labels\":{\"name\":\"dromon\",\"app\":\"dromon\"}},\"spec\":{\"type\":\"NodePort\",\"ports\":[{\"name\":\"dromon\",\"port\":443,\"protocol\":\"TCP\",\"targetPort\":8443}],\"selector\":{\"app\":\"dromon\",\"name\":\"dromon\"}}}"}


  (kube-apply-request
   kubeapi
   {:name         "dromon"
    :namespace    "development"
    :fieldManager "testmanager"
    :request      dromon-service}
   )

  (def kubeapi (dynamic-create-client {:method-generators
                                       [default-method-generator
                                        {:index           :apply
                                         :indexer         kube-kind-indexer
                                         :make-request-fn apply-request-fn}
                                        {:index           :http-method
                                         :indexer         kube-kind-method-indexer
                                         :make-request-fn default-request-fn}]}
                                      base-url "/openapi/v2" {:token "client token"}))

  (kube-apply-request
   kubeapi
   {:name         "dromon"
    :namespace    "development"
    :fieldManager "testmanager"
    :request      dromon-service}
   )

  )


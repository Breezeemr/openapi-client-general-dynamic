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

(defn kube-kind-indexer [acc {{:strs [group version kind] :as blah}
                              "x-kubernetes-group-version-kind"} data]
  (assert blah )
  (assoc-in acc [kind (str group "/" version)] data))

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

(defn apply-request-fn [{:strs [baseUrl parameters httpMethod operationId] :as method-discovery}]
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
                   {:keys [make-request-fn indexer]}]
  (fn [acc httpMethod method-discovery]
    (let [method-discovery (-> method-discovery
                               (update  "parameters" into upper-parameters)
                               (assoc "path" path
                                      "baseUrl" baseUrl
                                      "httpMethod" httpMethod))]
      (if-some [req-fn (make-request-fn method-discovery)]
        (indexer acc method-discovery req-fn)
        acc))))

(defn prepare-methods [api-discovery path parameters method-generator methods]
  (reduce-kv
    (make-method api-discovery parameters path method-generator)
    {}
    methods))

(defn prepare-paths [api-discovery method-generator paths]
  (reduce-kv
    (fn [acc path {:strs [parameters] :as premethod}]
      (into acc (prepare-methods
                  api-discovery
                  path
                  parameters
                  method-generator
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
  ([config base-uri path]
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
                  (get api-discovery "paths")))
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
  (let [{:keys [kind apiVersion]} (:request operation)
        opfn (-> client :apply (get kind) (get apiVersion) :request)]
    (assert opfn (str kind apiVersion
                      " is not implemented in "
                      (:api client)
                      (:version client)))
    (opfn client operation)))

(defn kube-apply [client operation]
  (let [r (kube-apply-request client operation)]
    (http/request r)))

(comment
  (def base-url "http://127.0.0.1:8001")

  (def api-data (get-openapi-spec {} base-url "/openapi/v2"))

  (keys api-data)

  (def kubeapi (dynamic-create-client {:method-generators
                                       [default-method-generator
                                        {:index :apply
                                         :indexer kube-kind-indexer
                                         :make-request-fn apply-request-fn}
                                        ]} base-url "/openapi/v2"))

  (ops kubeapi)

  (invoke kubeapi {:op :listCoreV1NamespacedPod
                   :namespace "production"})


  ;; Patch dromon deployment

  ;; *  :patchAppsV1NamespacedDeployment
  ;; partially update the specified Deployment

  (def dev-dromon {:apiVersion "apps/v1",
                   :kind       "Deployment",
                   :metadata   {:name "dromon", :labels {:app "dromon", :name "dromon"}},
                   :spec
                               {:replicas    1,
                                ;:serviceName "dromon",
                                :selector    {:matchLabels {:app "dromon"}},
                                :template
                                             {:metadata {:labels {:app "dromon", :name "dromon" :newly-added "label"}},
                                              :spec
                                                        {:containers
                                                         [{:volumeMounts
                                                                            [{:name "dromon-service-config", :mountPath "/app/conf"}
                                                                             {:name "datomic-creds", :mountPath "/app/creds"}
                                                                             {:name "breezeehr-cert", :mountPath "/app/certs"}
                                                                             {:name "dromon-service-account", :mountPath "/app/service-account"}
                                                                             {:name "dromon-app-cfg", :mountPath "app/resources"}],
                                                           :readinessProbe
                                                                            {:httpGet             {:port 8443, :scheme "HTTPS", :path "/health"},
                                                                             :initialDelaySeconds 35,
                                                                             :periodSeconds       5},
                                                           :name            "dromon",
                                                           :env
                                                                            [{:name "JAVA_TOOL_OPTIONS",
                                                                              :value
                                                                                    "-Xmx4G -Xms4G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication -XX:+AlwaysPreTouch -Djava.awt.headless=true -XX:ActiveProcessorCount=4"}
                                                                             {:name "BASE_URL", :value "https://localhost:8443"}
                                                                             {:name "SERVER_PORT", :value "8443"}
                                                                             {:name "SSL", :value "true"}
                                                                             {:name "DATABASE_URI",
                                                                              :valueFrom
                                                                                    {:secretKeyRef {:name "dromon-env-config", :key "phi-fhir-test"}}}
                                                                             {:name  "OPENID_PROVIDER_URL",
                                                                              :value "https://auth-dev.breezeehr.com/auth/realms/patient-portal"}
                                                                             {:name "AUTHORIZATION", :value "true"}
                                                                             {:name "CORS_ALLOWED_ORIGINS", :value "https://portal.breezeehr.com"}],
                                                           :ports           [{:containerPort 8443, :name "https"
                                                                              :protocol     "TCP"}],
                                                           :livenessProbe
                                                                            {:httpGet             {:port 8443, :scheme "HTTPS", :path "/health"},
                                                                             :initialDelaySeconds 120,
                                                                             :periodSeconds       20},
                                                           :imagePullPolicy "Always",
                                                           :image
                                                                            "gcr.io/breezeehr.com/breeze-ehr/dromon:DROMNG-98257bc6b9b42851f10c5388f5bca8755599e363",
                                                           :resources
                                                                            {:limits   {:memory "6Gi", :cpu "4"},
                                                                             :requests {:cpu "1", :memory "5Gi"}}}],
                                                         :volumes
                                                         [{:name "datomic-creds", :secret {:secretName "datomic-mysql-creds"}}
                                                          {:name "dromon-service-config", :secret {:secretName "service-config"}}
                                                          {:name "breezeehr-cert", :secret {:secretName "breezeehr-cert"}}
                                                          {:name   "dromon-service-account",
                                                           :secret {:secretName "dromon-service-account"}}
                                                          {:name "dromon-app-cfg", :configMap {:name "dromon-app-cfg"}}]}}}})

  (->
    @(kube-apply kubeapi {                                  ;:op        :applyAppsV1NamespacedDeployment
                      :name      "dromon"
                      :namespace "development"
                      :fieldManager "testmanager"
                      :request dev-dromon})

    ;   :body
    ;bs/to-string
    ;(json/parse-string  true)

    )

;; => {:kind "Status",
;;     :apiVersion "v1",
;;     :metadata {},
;;     :status "Failure",
;;     :message "415: Unsupported Media Type",
;;     :reason "UnsupportedMediaType",
;;     :details {},
;;     :code 415}

  (->
    @(kube-apply kubeapi {:name      "dromon"
                      :namespace "development"
                      :request
                      {:body dev-dromon}})

    :body
    bs/to-string
    (json/parse-string  true)

    )
;; => {:kind "Status",
;;     :apiVersion "v1",
;;     :metadata {},
;;     :status "Failure",
;;     :message
;;     "PatchOptions.meta.k8s.io \"\" is invalid: fieldManager: Required value: is required for apply patch",
;;     :reason "Invalid",
;;     :details
;;     {:group "meta.k8s.io",
;;      :kind "PatchOptions",
;;      :causes
;;      [{:reason "FieldValueRequired",
;;        :message "Required value: is required for apply patch",
;;        :field "fieldManager"}]},
;;     :code 422}

  )


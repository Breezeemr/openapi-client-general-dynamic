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
                                                                        :save-request? true
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
           :save-request? true
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

  (def dev-dromon {:apiVersion "apps/v1",
                   :kind       "Deployment",
                   :metadata   {:name "dromon", :labels {:app "dromon", :name "dromon"}},
                   :spec
                               {:replicas    1,
                                ;:serviceName "dromon",
                                :selector    {:matchLabels {:app "dromon"}},
                                :template
                                             {:metadata {:labels {:app "dromon", :name "dromon"}},
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
    @(invoke kubeapi {:op        :applyAppsV1NamespacedDeployment
                      :name      "dromon"
                      :namespace "development"
                      :fieldManager "testmanager"
                      :request dev-dromon})

       :body
    bs/to-string
    (json/parse-string  true)

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
    @(invoke kubeapi {:op        :applyAppsV1NamespacedDeployment
                      :name      "dromon"
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


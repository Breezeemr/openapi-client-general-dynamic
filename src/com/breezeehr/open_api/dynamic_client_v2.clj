(ns com.breezeehr.open-api.dynamic-client-v2
  (:require [com.breezeehr.open-api.dynamic-client :as dc]
            [aleph.http :as http]))

(def d (atom nil))

(defn make-method
  "outputs a request map e.g  "
  [{baseUrl :base-url
    path :path
    upper-parameters :parameters
    {description :description
     consumes :consumes
     upper-operation-id :operationId
     ;;NOTE not sure params are here
     parameters :parameters} :method-config}]
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
          path-fn      (dc/make-path-fn path path-params operationId)
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
                         (dc/fast-select-keys m query-ks))]
                                        ;(prn (keys method-discovery))
      (cond->
          (assoc acc (keyword operationId) {:id          operationId
                                            :description (get method-discovery "description")
                                            :request
                                            (fn [client op]
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
          (assoc (keyword (dc/patch->apply operationId)) (do
                                                        (prn (dc/patch->apply operationId))
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
        [method method-config] config :when (not= method :parameters)]
    (make-method {:base-url      base-url
                  :parameters    parameters
                  :path          path
                  :method        method
                  :method-config method-config})))

(comment
  (def client (create-client {:base-url "http://127.0.0.1:8001" :version "v2"}))

  (def example (first client))

  ;;TODO failing - fix me
  (dc/invoke client {:op        :listCoreV1NamespacedPod
                     :namespace "development"})




  )

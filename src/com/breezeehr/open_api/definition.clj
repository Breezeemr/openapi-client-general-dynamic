(ns com.breezeehr.open-api.definition
  (:require [aleph.http :as http]))

(defn get-openapi-spec [{:keys [get-token-fn pool] :as client} base-url path]
  (let [api-discovery (->
                        {:method :get
                         :url    (str base-url path)}
                        (assoc :throw-exceptions false
                               :save-request? true
                               :as :json-string-keys)
                        (cond->
                          pool (assoc :pool pool)
                          get-token-fn (assoc-in [:headers :authorization] (str "Bearer " (get-token-fn))))
                        http/request
                        deref
                        :body)]
    (cond-> api-discovery
            (not (not-empty (get api-discovery "servers")))
            (assoc "servers" [{"url" base-url}]))))


(defn enrich-method [path lower-servers lower-parameters]
  (fn [m]
    (let [method-key (key m)
          {:strs [servers parameters] :as method} (val m)]
      (case method-key
        ("get" "put" "post" "delete" "head" "options" "patch" "trace")
        (cond-> (assoc method "path" path "httpMethod" method-key)
                (and (not servers)
                     lower-servers) (assoc "servers" lower-servers)
                lower-servers
                (assoc "parameters" (into lower-parameters parameters))
                )
        "$ref" (throw (ex-info "\"$ref\" on path object is not supported" {}))
        ("summary" "description" "servers" "parameters") nil))))

(defn get-methods
  "Flatten all the methods from the open api spec"
  [{:strs [servers] :as spec}]
  (fn [p]
    (let [path (key p)
          path-item (val p)
          servers (or (get path-item "servers") servers)]
      (eduction
        (keep (enrich-method path servers (get path-item "parameters")))
        path-item))))

(defn spec-methods
  "Flatten all the methods from the open api spec"
  [spec]
  (eduction
    (mapcat (get-methods spec))
    (get spec "paths")))

(comment
  (def base-url "http://127.0.0.1:8001")

  (def api-data (get-openapi-spec {} base-url "/openapi/v2"))
  (keys api-data)
  (into [] (spec-methods api-data))

  )
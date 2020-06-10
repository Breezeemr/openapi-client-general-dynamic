(ns com.breezeehr.open-api.dynamic-client-v2
  (:require [com.breezeehr.open-api.dynamic-client :as dc]))

(defn create-client
  [{:keys [base-url version]}]
  (for [[path {:keys [parameters]}] (->> {:method           :get
                                          :url              (str base-url "/openapi/" version)
                                          :throw-exceptions false
                                          :as               :json}
                                      http/request
                                      deref
                                      :body
                                      :paths)]
    (dc/make-method base-url parameters path)))

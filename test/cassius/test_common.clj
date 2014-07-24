(ns cassius.test-common
  (:use midje.sweet)
  (:require [cassius.common :refer :all]))
  
(fact "assoc-nil will only assocate if there is no existing key in the map"
  
  (assoc-nil {} :a 1 :b 2)
  => {:a 1 :b 2}
  
  (assoc-nil {:b 1} :a 1 :b 2)
  => {:a 1 :b 1})
  
(fact "vectorise will put an object in a vector if it is not one"
  
  (vectorize 1) 
  => [1]
  
  (vectorize [1]) 
  => [1])
  

(fact "remove-same will remove all entries in a map if the values are equal"

  (remove-same {:a 1} {:a 1}) 
  => {}
  
  (remove-same {:a 1} {:a 2}) 
  => {:a 1}

  (remove-same {:a 1} {:a 3} #(= (even? %1) (even? %2)))
  => {})
  

(fact "merge-nil will only merge values into the first map if there is no existing key."
  
  (merge-nil {} {:a 1 :b 2}) 
  => {:a 1 :b 2}

  
  (merge-nil {:b 1} {:a 1 :b 2}) 
  => {:a 1 :b 1})
  
(fact "select-keys-in gets a value if a keyword or a the map if a vector."
  
  (select-keys-in {:a 1} :a) 
  => 1
  
  (select-keys-in {:a 1} [:a]) 
  => {:a 1}
  
  (select-keys-in {:a 1 :b 2} [:a :b]) 
  => {:a 1 :b 2}
)

(fact "namify-keys will return a map with all keys turned into string"
  (namify-keys {:a {:b {:c 1}}})
  => {"a" {"b" {"c" 1}}}
  
  (namify-keys {:a {:b {:c :d}}})
  => {"a" {"b" {"c" :d}}}
)


(fact "keyify-keys will return a map with all keys turned into keywords"
  (keyify-keys {"a" {"b" {"c" 1}}})
  => {:a {:b {:c 1}}}
)
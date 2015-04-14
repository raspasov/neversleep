(defproject neversleep-db "1.0.0-alpha1"
            :description "We choose to go the moon not because it's easy but because it's hard"
            :url "https://github.com/raspasov/neversleep"
            :license {:name "GNU AFFERO GENERAL PUBLIC LICENSE"
                      :url  "https://www.gnu.org/licenses/agpl-3.0.html"}
            :java-source-paths ["src/jv"]
            :dependencies [

                           ;[org.clojure/clojure "1.6.0"]
                           ;stay on the edge
                           [org.clojure/clojure "1.7.0-beta1"]
                           [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                           [aprint "0.1.0"]

                           [aleph "0.4.0-beta3"]
                           ;serialization
                           [com.taoensso/nippy "2.8.0"]
                           ;data compression
                           [cc.qbits/nippy-lz4 "0.1.0"]
                           ;protocol buffers
                           [org.flatland/protobuf "0.8.1"]

                           ;logging/profiling
                           [com.taoensso/timbre "3.4.0"]

                           ;mysql
                           [org.clojure/java.jdbc "0.3.6"]
                           [mysql/mysql-connector-java "5.1.31"]
                           [com.jolbox/bonecp "0.8.0.RELEASE"]

                           [environ "1.0.0"]

                           ;util fns
                           [org.clojure/core.incubator "0.1.3"]

                           ;test.check
                           [org.clojure/test.check "0.5.9"]

                           [leinjacker "0.4.2"]

                           ;nrepl
                           [org.clojure/tools.nrepl "0.2.5"]

                           ;rrb vectors from contrib
                           [org.clojure/core.rrb-vector "0.0.11"]

                           ;perf benchmarking
                           [criterium "0.4.3"]

                           ;hashing, murmur
                           [com.google.guava/guava "18.0"]

                           ;caching
                           [org.clojure/core.cache "0.6.4"]

                           ;netty
                           [io.netty/netty-all "4.0.25.Final"]

                           ;gloss
                           [gloss "0.2.4"]

                           ;json
                           [cheshire "5.4.0"]

                           ;dynamodb client
                           [com.raspasov/faraday "1.6.0-beta1"]

                           ;rpm
                           [org.codehaus.mojo/rpm-maven-plugin "2.1-alpha-1"]
                           [org.clojure/java.data "0.1.1"]

                           ;lein itself
                           [leiningen-core "2.5.1"]

                           ]
            ;:global-vars {*warn-on-reflection* true *unchecked-math* :warn-on-boxed}

            :javac-options ["-target" "1.6" "-source" "1.6"]

            :plugins [[lein-environ "1.0.0"]
                      [lein-rpm "0.0.5"]
                      [lein-protobuf "0.4.2-LOCAL"]]

            :main ^:skip-aot neversleep-db.core
            :jvm-opts ^:replace ["-XX:+AggressiveOpts"
                                 "-XX:+UseFastAccessorMethods"
                                 "-XX:+UseG1GC"
                                 "-Xms3g"
                                 "-Xmx3g"
                                 "-XX:+PerfDisableSharedMem"
                                 ;enable remote debugger
                                 ;"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
                                 ;YourKit
                                 "-agentpath:/Users/raspasov/Downloads/YourKit_Java_Profiler_2015_EAP_build_15040.app/Contents/Resources/bin/mac/libyjpagent.jnilib"
                                 ]
            :target-path "target/%s"
            :profiles {:uberjar {:aot :all}
                       :local   {}
                       :dev     {}
                       :test    {}
                       }
            :repl-options {:timeout 120000})

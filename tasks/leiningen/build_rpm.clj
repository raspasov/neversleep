(ns leiningen.build-rpm
  (:refer-clojure :exclude [replace])
  (:require [clojure.java.io :as java-io]
            [clojure.java.shell :as shell]
            [clojure.pprint]
            [clojure.string :refer [join capitalize trim-newline replace]])
  (:import java.util.Date
           java.text.SimpleDateFormat
           (org.codehaus.mojo.rpm RPMMojo
                                  AbstractRPMMojo
                                  Mapping Source
                                  Scriptlet)
           (org.apache.maven.project MavenProject)
           (org.apache.maven.shared.filtering DefaultMavenFileFilter)
           (org.codehaus.plexus.logging.console ConsoleLogger)
           (java.lang.reflect AccessibleObject)
           (java.io Writer)))

(defn write
  "Write string to file, plus newline"
  [file string]
  (with-open [^Writer w (java-io/writer file)]
    (.write w (str (trim-newline string) "\n"))))

(defn workarea
  [project]
  (java-io/file (:root project) "target" "rpm"))

(defn cleanup
  [project]
  (shell/sh "rm" "-rf" (str (workarea project))))

(defn reset
  [project]
  (cleanup project)
  (shell/sh "rm" (str (:root project) "/target/*.rpm")))

(defn get-version
  [project]
  (let [df   (SimpleDateFormat. ".yyyyMMdd.HHmmss")]
    (replace (:version project) #"-SNAPSHOT" (.format df (Date.)))))

(defn set-mojo!
  "Set a field on an AbstractRPMMojo object."
  [object name value]
  (let [^AccessibleObject field (.getDeclaredField AbstractRPMMojo name)]
    (.setAccessible field true)
    (.set field object value))
  object)

(defn array-list
  [list]
  (let [list (java.util.ArrayList.)]
    (doseq [item list] (.add list item))
    list))

(defn scriptlet
  "Creates a scriptlet backed by a file"
  [filename]
  (doto (Scriptlet.)
    (.setScriptFile (java-io/file filename))))

(defn source
  "Create a source with a local location and a destination."
  ([] (Source.))
  ([location]
   (doto (Source.)
     (.setLocation (str location))))
  ([location destination]
   (doto (Source.)
     (.setLocation (str location))
     (.setDestination (str destination)))))

(defn mapping
  [m]
  (doto (Mapping.)
    (.setArtifact           (:artifact m))
    (.setConfiguration      (case (:configuration m)
                              true  "true"
                              false "false"
                              nil   "false"
                              (:configuration m)))
    (.setDependency         (:dependency m))
    (.setDirectory          (:directory m))
    (.setDirectoryIncluded  (boolean (:directory-included? m)))
    (.setDocumentation      (boolean (:documentation? m)))
    (.setFilemode           (:filemode m))
    (.setGroupname          (:groupname m))
    (.setRecurseDirectories (boolean (:recurse-directories? m)))
    (.setSources            (:sources m))
    (.setUsername           (:username m))))

(defn mappings
  [project]
  (map (comp mapping
             (partial merge {:username "neversleep-db"
                             :groupname "neversleep-db"}))

       [
        ;/etc
        ;===========
        ; Init scripts
        {:directory "/etc/init.d"
         :filemode "755"
         :sources [(source (java-io/file (:root project) "redhat" "etc" "init.d" "neversleep-db")
                           "neversleep-db")]}
        ; Config dir
        {:directory "/etc/neversleep-db"
         :filemode "755"
         :directory-included? true}
        ; Config file
        {:directory "/etc/neversleep-db"
         :filemode "644"
         :configuration true
         :sources [(source (java-io/file (:root project) "redhat" "etc" "neversleep-db" "config-original.clj")
                           "config-original.clj")]}
        ;JVM config
        {:directory "/etc/neversleep-db"
         :filemode "644"
         :configuration true
         :sources [(source (java-io/file (:root project) "redhat" "etc" "neversleep-db" "jvm-config-original")
                           "jvm-config-original")]}
        ;/usr
        ;===========
        ; bin dir
        {:directory "/usr/bin/neversleep-db"
         :filemode "755"
         :directory-included? true}

        ;start.sh file
        {:directory "/usr/bin/neversleep-db"
         :filemode "755"
         :sources [(source (str (java-io/file (:root project) "redhat" "usr" "bin" "neversleep-db" "start.sh"))
                           "start.sh")]}

        ; jar
        {:directory "/usr/bin/neversleep-db/"
         :filemode "755"
         :sources [(source (str (java-io/file (:root project)
                                      "target"
                                      "uberjar"
                                      (str "neversleep-db-"
                                           (:version project)
                                           "-standalone.jar")))
                           "neversleep-db.jar")]}

        ; data dirs
        {:directory "/usr/lib/neversleep-db"
         :filemode "755"
         :directory-included? true}

        {:directory "/usr/lib/neversleep-db/b_plus_tree_root_logs"
         :filemode "755"
         :directory-included? true}

        {:directory "/usr/lib/neversleep-db/blob_id"
         :filemode "755"
         :directory-included? true}

        {:directory "/usr/lib/neversleep-db/command_log"
         :filemode "755"
         :directory-included? true}

        ;/var
        ;==========
        ;log dir
        {:directory "/var/log/neversleep-db"
         :filemode "755"
         :directory-included? true}
        ;error log
        {:directory "/var/log/neversleep-db"
         :filemode "755"
         :sources [(source (java-io/file (:root project) "redhat" "var" "log" "neversleep-db" "error.log")
                           "error.log")]}]))

(defn blank-rpm
  "Create a new RPM file"
  []
  (let [mojo (RPMMojo.)
        fileFilter (DefaultMavenFileFilter.)]
    (set-mojo! mojo "project" (MavenProject.))
    (.enableLogging fileFilter (ConsoleLogger. 0 "Logger"))
    (set-mojo! mojo "mavenFileFilter" fileFilter)))

(defn create-dependency
  [rs]
  (let [hs (java.util.LinkedHashSet.)]
    (doseq [r rs] (.add hs r))
    hs))

(defn make-rpm
  "Create and execute a Mojo RPM."
  [project]
  (doto (blank-rpm)
    (set-mojo! "projversion" (get-version project))
    (set-mojo! "name" (:name project))
    (set-mojo! "summary" (:description project))
    (set-mojo! "description" "Immutable data structure server")
    (set-mojo! "copyright" "Rangel Spasov")
    (set-mojo! "workarea" (workarea project))
    (set-mojo! "mappings" (mappings project))
    (set-mojo! "targetOS" "any")
    (set-mojo! "targetVendor" "any")
    (set-mojo! "group" "neversleep-db")
    (set-mojo! "preinstallScriptlet" (scriptlet
                                       (java-io/file (:root project)
                                                     "debian" "DEBIAN" "preinst")))
    (set-mojo! "postinstallScriptlet" (scriptlet
                                       (java-io/file (:root project)
                                                     "debian" "DEBIAN" "postinst")))
    (set-mojo! "requires" (create-dependency ["jre >= 1.7.0"
                                              ;"daemonize >= 1.7.3"
                                              ]))
    (.execute)))


(defn build-rpm [project]
  (make-rpm project))
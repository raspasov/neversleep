# NeverSleep

NeverSleep is an immutable data structure server, written in Clojure


## Getting started

### Ubuntu

Tested on Ubuntu 14.04 LTS

Make sure you have the latest version of **Java 8**:

```sh
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update
$ sudo apt-get install oracle-java8-installer
```

Download and install NeverSleep:
[neversleep-1.0.0-alpha1.deb](https://s3-us-west-2.amazonaws.com/neversleep/neversleep-1.0.0-alpha1.deb)

```sh
$ wget https://s3-us-west-2.amazonaws.com/neversleep/neversleep-1.0.0-alpha1.deb
$ sudo dpkg -i neversleep-1.0.0-alpha1.deb
```

NeverSleep uses third party storage backends to store your data. 

Currently there's support only for **MySQL** (DynamoDB and other SQL-compatible servers are **coming soon**).

1. Edit **/etc/neversleep-db/config.clj** to configure **MySQL** - specify your own :host, :port, :database-name, :user and :password
2. In the database that you specified under :database-name, import **[those tables](https://github.com/raspasov/neversleep/blob/d5cafea8b995396d1d120576c0c7ed1f658b753d/mysql-schema.sql)**

Optionally (but strongly recommended), edit **/etc/neversleep-db/jvm-config** to specify your JVM heap size; Xms and Xmx values are strongly recommended to be the same; defaults to a modest 256MB of JVM heap


Ready to go? Start the server:

```sh
$ sudo service neversleep-db start
```

## Usage

- [NeverSleep Clojure Client Library](https://github.com/raspasov/neversleep-clojure-client)
- [NeverSleep PHP Client Library](https://github.com/raspasov/neversleep-php-client)

## License

Copyright © 2015 Rangel Spasov


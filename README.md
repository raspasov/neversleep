# NeverSleep

NeverSleep is an immutable data structure server, written in Clojure


## Getting started

### Ubuntu

Tested on Ubuntu 14.04 LTS

Make sure you have the latest version of Java 8:

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

Start the server:

```sh
$ sudo service neversleep-db start
```

## Usage

- [NeverSleep Clojure Client Library](https://github.com/raspasov/neversleep-clojure-client)
- [NeverSleep PHP Client Library](https://github.com/raspasov/neversleep-php-client)

## License

Copyright © 2015 Rangel Spasov


#!/bin/bash
cp target/uberjar/neversleep-db-1.0.0-alpha1-standalone.jar debian/usr/bin/neversleep-db/neversleep-db.jar

dpkg-deb --build debian
#sudo dpkg -i debian.deb
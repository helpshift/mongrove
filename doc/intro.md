# Introduction to mongrove

A Clojure library designed to interact with MongoDB.

## Motivation

The current de-facto Clojure Mongo driver [Monger](https://github.com/michaelklishin/monger) that we are using is quite out-dated. Most of the java API that it uses is now deprecated. More importantly, it does not have support for all of the classes which are required for doing multi-document transactions.

To that end, we have started efforts of creating a new Clojure driver with an API similar to monger, wrapping over the latest official [Java drivers](https://mongodb.github.io/mongo-java-driver/4.1/driver/) By open sourcing this, we are hoping to give back to the clojure community in a small way.

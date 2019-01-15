# CanoeDB  
###### A simple NoSQL database on the front-end, and just a directory of CSV files on the back-end. 

- **Relational:** CSV files become tables with relationships to other tables  
- **Auto-dereferencing:** API queries spanning multiple tables are dereferenced automatically  
- **Simple Referencing:** left column is always the reference ID  
- **Reliable:** data is *appended* to CSV files (O_APPEND) and cannot be deleted.
- **Built-in Multithreaded HTTP Server:** The Database class can be called directly, or run via the CanoeServer class.

### Prerequisites

- Java
- not much else

### Installing

A step by step series of examples that tell you how to get a development env running
Say what the step will be
```
Give the example
```
End with an example of getting some data out of the system or using it for a little demo

## Getting Started

CanoeServer class provides an HTTP webserver and wrapper for the Database class in the canoedb package.  The Database class and canoedb package may also be used standalone.

Start the server ([] indicate optional):
```
java CanoeServer /directory [port_number] [>logfile]
```

Access JSON API:
```
http://localhost:8080/json?table1.column1.Transform=filter_string
```
Access old-fashioned HTML form UI:
```
http://localhost:8080/form?table1.column1.Transform=filter_string
```
Where:
- Transform is a class such as First, Last, TimeStamp, StoreBase64, TransmitBase64.

## Deployment


## Built With


## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/gabrielwilson3/canoedb/tags). 

## Authors

* **Gabe Wilson** - *Initial work* - [PurpleBooth](https://github.com/gabrielwilson3)

See also the list of [contributors](https://github.com/gabrielwilson3/canoedb/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* The CanoeServer multithreaded webserver is based on Webserver.java by Matt Mahoney (mmahoney@cs.fit.edu)

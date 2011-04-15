# xPL-Logger-MongoDB 

xPL-Logger-MongoDB store all the [xPL](http://xplproject.org.uk/) messages 
in a [MongoDB database](http://www.mongodb.org/).

## Dependencies 

* [xPL-Perl](https://github.com/beanz/xpl-perl/)
* MongoDB
* DateTime
* Time::HiRes
* DBI
* IO::Handle
* Readonly
* Getopt::Long

## Usage

### xPL-Logger-MongoDB

./xpl-logger-mongodb.pl

### Convert-xPL-Logger-MySQL-to-MongoDB

./convert-xpl-logger-mysql-to-mongodb.pl --usage
Usage:
      convert-xpl-logger-mysql-to-mongodb.pl [options]
      where valid options are (default shown in brackets):
        -usage - show this help text
        -in_host - input hostname (localhost)
    	-in_db - input database name (xpl)
    	-in_user - input username (root)
    	-in_password - input password (me)
    	-out_host - output hostname (localhost)
    	-out_db - output database name (xpl)
    	-out_collection - output collection (msg)

## Contributing

Please report issues on the [Github issue tracker](https://github.com/cscm/XPL-Logger-MongoDB/issues).

## License

Copyright © 2011 Christophe Nowicki.

# couchmigrate

A command-line tool to assist with the migration of CouchDB Design Documnents

## Installation

    npm install -g couchmigrate
    
## Usage

Create a file with your design document in e.g. dd.json

    {
        "_id": "_design/fetch",
        "views": {
            "byx": {
                "map": "function (doc) {\n  emit(doc.x, doc.y);\n  }\n}",
                "reduce": "_stats"
            }
        },
        "language": "javascript"
    }

Then setup environment variables to point to your instance of CouchDB

    export COUCH_URL=http://127.0.0.1:5984
  
or

    export COUCH_URL=https://myusername:mypassword@myhost.cloudant.com

Then run `couchmigrate`:

    couchmigrate --dd dd.json --db mydatabase

* dd - the path to the file containing the design documnet
* db - the name of the database

If the design document is already present and is identical to the one in the file, no migration will occur, otherwise

* copy old design document to _OLD
* import new design document to _NEW
* trigger the view to make sure it builds
* poll _active_tasks to see if it has finished building
* copy _NEW to the real design document name
* delete _NEW 
* delete _OLD
* exit

In other words, couchmigrate will only return when the design document has been uploaded, built and has been moved into place.

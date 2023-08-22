#!/usr/bin/env python2

import tarfile
import sys
import os
from mbslave import Config, connect_db, parse_name, check_table_exists, fqn


def load_tar(filename, db, config, ignored_schemas, ignored_tables):
    print ("Importing data from", filename)
    tar = tarfile.open(filename, 'r:bz2')
    cursor = db.cursor()
    for member in tar:
        if not member.name.startswith('mbdump/'):
            continue
        name = member.name.split('/')[1].replace('_sanitised', '')
        schema, table = parse_name(config, name)
        fulltable = fqn(schema, table)
        if schema in ignored_schemas:
            print (f" - Ignoring {name} from schema {schema}: Ignored schemas are {ignored_schemas}")
            continue
        if table in ignored_tables:
            print (" - Ignoring", name, " from ", ignored_tables)
            continue
        if not check_table_exists(db, schema, table):
            print (" - Skipping %s (table %s does not exist)" % (name, fulltable))
            continue
        cursor.execute("SELECT 1 FROM %s LIMIT 1" % fulltable)
        print(f"Table {fulltable} exists and could run query against it")
        if cursor.fetchone():
            print (" - Skipping %s (table %s already contains data)" % (name, fulltable))
            continue
        print (" - Loading %s to %s" % (name, fulltable))

        # TODO why won't public work
        frig_table = fulltable.split('.')[-1]
        print(frig_table)

        cursor.copy_from(tar.extractfile(member), frig_table) #fulltable)
        db.commit()


config = Config(os.path.dirname(__file__) + '/mbslave.conf')
db = connect_db(config)

ignored_schemas = set(config.get('schemas', 'ignore').split(','))
ignored_tables = set(config.get('TABLES', 'ignore').split(','))
for filename in sys.argv[1:]:
    load_tar(filename, db, config, ignored_schemas, ignored_tables)

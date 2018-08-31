Generate package
================

$ python3 setup.py sdist bdist_wheel

$ cd dist

$ ls

pycaz-0.0.1-py3-none-any.whl  pycaz-0.0.1.tar.gz

Install package
===============

$ tar xvf pycaz-0.0.1.tar.gz

$ cd pycaz-0.0.1/

$ python3 -m venv env

$ source env/bin/activate

(env) $ python3 setup.py install

Run caz
=======

(env) $ python3 -m pycaz.caz -h
usage: caz [-h] [-i] [-s] [-p] [-f bucket] [-l] [-k key] [-a]
           [-r key version-id] [-t date]

optional arguments:
  -h, --help            show this help message and exit
  -i, --init-db         initialize database
  -s, --info-db         show database info
  -p, --purge-db        purge database
  -f bucket, --fetch-bucket bucket
                        fetch bucket
  -l, --list-objects    list objects (versions and delete markers)
  -k key, --list-objects-by-key key
                        list objects by key (versions and delete markers)
  -a, --list-keys       list all available object keys
  -r key version-id, --recover-object key version-id
                        recover object
  -t date, --trim-objects date
                        trim objects

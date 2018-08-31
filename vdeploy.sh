python3 -m venv env && . env/bin/activate
python3 setup.py install
cat > env/bin/caz <<EOF
#!/bin/bash
python3 -m pycaz.caz \$@
EOF
chmod +x env/bin/caz
. env/bin/activate

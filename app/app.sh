#!/bin/bash
# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

echo "Creating"
python3 -m venv .venv
echo "Activating"
source .venv/bin/activate

pip install -r requirements.txt

echo "Packing"
venv-pack -o .venv.tar.gz

bash prepare_data.sh

echo "INDEXING DATA"
bash index.sh /data

echo "Successful data indexing"
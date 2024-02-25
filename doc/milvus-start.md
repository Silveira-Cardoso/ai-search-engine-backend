# Start Milvus

wget https://raw.githubusercontent.com/milvus-io/milvus/master/scripts/standalone_embed.sh
bash standalone_embed.sh start

# Download milvus-standalone-docker-compose.yml and save it as docker-compose.yml manually
wget https://github.com/milvus-io/milvus/releases/download/v2.3.3/milvus-standalone-docker-compose.yml -O docker-compose.yml
# In the same directory as the docker-compose.yml file, start up Milvus
sudo docker compose up -d
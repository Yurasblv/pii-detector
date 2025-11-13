docker compose -f docker-compose.local.yml stop app
docker compose -f docker-compose.local.yml up -d app
id=$(docker ps -qf "name=app")
echo $id
docker exec -it $id bash
# -c "./customer-start.sh"
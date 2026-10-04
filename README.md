## Como rodar
docker compose up --build -d

# teste automatizado 
docker compose run --rm cliente python -u src/cliente.py --demo

# menu interativo
docker compose run --rm cliente

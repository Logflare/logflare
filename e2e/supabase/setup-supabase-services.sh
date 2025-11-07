if [ -d supabase ]; then
    cd supabase/docker
    docker compose down -v
    cd ../..
    sudo rm -rf supabase
fi

git clone --filter=blob:none --no-checkout https://github.com/supabase/supabase

cd supabase

git sparse-checkout set --cone docker && git checkout master

cd docker

cp .env.example .env

docker compose -f docker-compose.yml -f ../../docker-compose.e2e.yml up --build -d


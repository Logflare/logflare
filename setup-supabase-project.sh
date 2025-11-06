# git clone --filter=blob:none --no-checkout https://github.com/supabase/supabasecd supabasegit sparse-checkout set --cone docker && git checkout mastercd ..

SUPABASE_REPO_PATH=../supabase

PROJECT_DIR=supabase-project

if [ -d $PROJECT_DIR ]; then
    cd $PROJECT_DIR
    docker compose -f docker-compose.yml -f compose.integration-supabase.yml  down -v
    cd ..
fi

mkdir -p $PROJECT_DIR

cp -rf $SUPABASE_REPO_PATH/docker/* $PROJECT_DIR

cp $SUPABASE_REPO_PATH/docker/.env.example $PROJECT_DIR/.env
cp compose.integration-supabase.yml $PROJECT_DIR/compose.integration-supabase.yml

cd $PROJECT_DIR || exit 1

# sed -i 's/^POSTGRES_PORT=.*/POSTGRES_PORT=9432/' .env

# if [[ "$(uname)" == "Linux" ]]; then
#       export HOST_IP=$(ip addr show docker0 | grep -Po 'inet \K[\d.]+')
# fi

docker compose pull

docker compose -f docker-compose.yml -f compose.integration-supabase.yml up -d

cd simulator
docker build -t simulator .
cd ..
cd predictor
docker build -t predictor .
cd ..

docker-compose up -d

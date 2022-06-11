docker build -f Dockerfile-precomputed-app . -t run_precomputed_app:1.0
docker run -p 8082:8082 -v /Users/alexa/Desktop/big_data_project:/big_data_project --network general-network --rm run_precomputed_app:1.0

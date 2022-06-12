docker build -f Dockerfile-precomputed-app . -t run_precomputed_app:1.0
docker run -p 8082:8082 --network general-network -v /Users/alexa/Desktop/University/3year/2semester/big_data/big_data_project:/big_data_project --rm run_precomputed_app:1.0

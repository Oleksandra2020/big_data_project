docker build -f Dockerfile-precomputed . -t run_precomputed:1.0
docker run -v /Users/alexa/Desktop/big_data_project:/big_data_project --network general-network --rm run_precomputed:1.0

docker build -f Dockerfile-precomputed-app . -t run_precomputed_app:1.0
docker run -p 8082:8082 --network general-network --rm run_precomputed_app:1.0

docker build -f Dockerfile-precomputed . -t run_precomputed:1.0
docker run --network general-network --rm run_precomputed:1.0

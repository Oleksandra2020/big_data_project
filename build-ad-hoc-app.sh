docker build -f Dockerfile-ad-hoc-app . -t run_ad_hoc:1.0
docker run -p 8081:8081 --network general-network --rm run_ad_hoc:1.0

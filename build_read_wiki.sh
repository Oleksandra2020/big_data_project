docker build -f Dockerfile-read-wiki . -t run_read_wiki:1.0

while :
do
    docker run -p 8080:8080 --network general-network --rm run_read_wiki:1.0
done

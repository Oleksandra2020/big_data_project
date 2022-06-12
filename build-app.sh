docker build -f Dockerfile-app . -t run_app:1.0
docker run -p 8081:8081 --network general-network -v /Users/alexa/Desktop/University/3year/2semester/big_data/big_data_project:/big_data_project --rm run_app:1.0

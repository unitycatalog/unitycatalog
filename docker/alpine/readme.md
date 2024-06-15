# How to use
This dockerfile builds the sbt project and downloads all the dependencies into the image so that you don't have to 
build it everytime you `start-uc-server`. 

After building, start by running it as a detach container

By default, it runs on port `8080` but if you are already using that port in your service, you can change it in the `dockerfile`

# Build it
```docker
docker build -f Dockerfile -t unitycatalog:v0.0.1 .
```

# Run it
```docker
docker run -d -p 8080:8080 <image_id>
```

# Test it
```bash
curl http://localhost:8080/api/2.1/unity-catalog/catalogs # list catalogs output.
```


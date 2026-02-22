name: unitycatalog

services:

  server:
    image: unitycatalog/unitycatalog:latest
    ports:
      - "8080:8080"
    volumes:
      - type: bind
        source: ./etc/conf
        target: /opt/unitycatalog/etc/conf
      - type: volume
        source: unitycatalog_data
        target: /opt/unitycatalog/etc/data
  ui:
    build:
      context: ui/
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    depends_on:
      - server

volumes:
  # Persist docker volume across container restarts
  unitycatalog_data:

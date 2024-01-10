FROM python:3.10-alpine

WORKDIR /etl

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run the command during container startup to ingest the data into postgres
# ENV PGPASSWORD='password'
# CMD ["postgres", "-h", "localhost", "-p", "5433", "-U", "postgres", "-d", "bc_str", "-f", "/sql/neighbourhoods_geojson.sql"]

CMD [ "python", "etl.py" ]
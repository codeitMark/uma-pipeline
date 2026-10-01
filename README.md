# uma-pipeline

Likely on hold/slow progress due to university, and hunting for internships. The courses are lowkey no joke lol.

There is definitely some scuff/bloat regarding docker-compose and dockerfiles, but I am no senior. I am open to feedback.

Personal notes: (will move to the bottom when I eventually write a proper README, likely after the semester)

I finally found something I want to make. This is both for personal growth, interest and portfolio.

A personal project related to uma, which I will expand on. Data from uma.moe API.

Will expand on README.md eventually with instructions on how to run (e.g. docker-compose up -d)

I do consider chess-pipeline to end up somewhat better than this repo, but it was rushed, as well as with great help from AI due to the small timeline of 3 weeks (for someone who hasn't used these technologies at all!).

chess-pipeline was also a group project related to a University subject, I mainly worked with setting up the Dockerfile and docker-compose.yaml, ingesting data, transforming streaming data from Kafka to DataFrames in PySpark there, loading the data into MongoDB, and writing the project report. This reflects my current capabilities more (as of 2026, as a student).

Note: chess-pipeline is currently private, and can be shared upon request.

Also don't be surprised if some of the code ends up being worse here than in chess-pipeline, as I started this before chess-pipeline, as an introduction to connecting these technologies together.

This project will (eventually) be using the lambda architecture. chess-pipeline is leaning towards a Kappa architecture (only real-time streaming as single source of truth, for both real-time ingestion and historical data). Only leaning, because in truth, it is a pure real-time streaming ETL pipeline than any of these (due to the lack of historical data).

# Roadmap

Implement ETL data pipeline with Kafka and PySpark. Will store data in local mongodb. Probably with both real-time and batch (lambda architecture).

Using direct approach for this, rather than consumer-based approach.

Eventually implement cloud computing (likely databricks) within the project. Will run together with MongoDB.

Note: databricks lakehouse-architecture (both data lake and warehouse.)

Above is strictly data engineering growth. For back-end, I will eventually build my own API, with full CRUD operations, with Java Spring Boot.

Documentation is important, and I will use this README for that.
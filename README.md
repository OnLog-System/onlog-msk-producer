# onlog-msk-producer

Kafka Producer for OnLog Edge → MSK ingestion.

## Run (RPi)

```bash
cp env.example .env
export $(cat .env | xargs)
./run.sh

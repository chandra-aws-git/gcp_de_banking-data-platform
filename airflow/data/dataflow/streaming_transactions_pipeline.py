import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


REQUIRED_FIELDS = {
    "event_id",
    "transaction_id",
    "account_id",
    "customer_id",
    "amount",
    "currency",
    "transaction_type",
    "channel",
    "merchant",
    "event_ts",
}


class BankingStreamingOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_subscription", required=True)
        parser.add_argument("--output_table", required=True)
        parser.add_argument("--deadletter_topic", required=True)


class ParseAndValidateEvent(beam.DoFn):
    def process(self, message):
        raw_message = message.decode("utf-8", errors="replace")
        try:
            event = json.loads(raw_message)
            missing_fields = REQUIRED_FIELDS - event.keys()
            if missing_fields:
                raise ValueError(f"Missing fields: {sorted(missing_fields)}")

            event["event_ts"] = datetime.fromisoformat(
                str(event["event_ts"]).replace("Z", "+00:00")
            )
            event["ingest_ts"] = datetime.now(timezone.utc)
            yield event
        except Exception as exc:
            logging.exception("Invalid transaction event: %s", exc)
            deadletter_payload = {
                "error": str(exc),
                "raw_message": raw_message,
                "failed_at": datetime.now(timezone.utc).isoformat(),
            }
            yield beam.pvalue.TaggedOutput(
                "deadletter",
                json.dumps(deadletter_payload).encode("utf-8"),
            )


def format_for_bigquery(event):
    return {
        "event_id": event["event_id"],
        "transaction_id": event["transaction_id"],
        "account_id": event["account_id"],
        "customer_id": event["customer_id"],
        "amount": event["amount"],
        "currency": event["currency"],
        "transaction_type": event["transaction_type"],
        "channel": event["channel"],
        "merchant": event["merchant"],
        "event_ts": event["event_ts"].isoformat(),
        "ingest_ts": event["ingest_ts"].isoformat(),
    }


def deduplicate_by_event_id(events):
    return (
        events
        | "KeyByEventId" >> beam.Map(lambda event: (event["event_id"], event))
        | "GroupByEventId" >> beam.GroupByKey()
        | "KeepFirstEvent" >> beam.Map(lambda item: next(iter(item[1])))
    )


def run(argv=None):
    pipeline_options = PipelineOptions(argv, streaming=True, save_main_session=True)
    options = pipeline_options.view_as(BankingStreamingOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        parsed = (
            pipeline
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=options.input_subscription)
            | "ParseAndValidate"
            >> beam.ParDo(ParseAndValidateEvent()).with_outputs("deadletter", main="valid")
        )

        (
            parsed.deadletter
            | "WriteDeadLetter" >> WriteToPubSub(topic=options.deadletter_topic)
        )

        windowed_events = (
            parsed.valid
            | "AssignEventTime"
            >> beam.Map(
                lambda event: beam.window.TimestampedValue(
                    event,
                    event["event_ts"].timestamp(),
                )
            )
            | "WindowIntoFixedWindows"
            >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
        )

        (
            deduplicate_by_event_id(windowed_events)
            | "FormatForBigQuery" >> beam.Map(format_for_bigquery)
            | "WriteToBigQuery"
            >> WriteToBigQuery(
                table=options.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()

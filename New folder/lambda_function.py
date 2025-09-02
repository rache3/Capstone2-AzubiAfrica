import boto3
import os
import json
import urllib.parse
from botocore.exceptions import ClientError, BotoCoreError

s3 = boto3.client("s3")
translate = boto3.client("translate")

# Get response bucket from environment variable
RESPONSE_BUCKET = os.environ.get("RESPONSE_BUCKET", "mozal-response-bucket")
RESPONSE_PREFIX = os.environ.get("RESPONSE_PREFIX", "responses/")

CHUNK_SIZE = 4500  # safe chunk size for TranslateText

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        try:
            # Get the uploaded file from request bucket
            obj = s3.get_object(Bucket=bucket, Key=key)
            payload = json.loads(obj["Body"].read().decode("utf-8"))

            src = payload.get("source_language", "auto")
            tgt = payload["target_language"]

            texts = payload.get("texts") or [payload.get("text")]

            translations = []
            for idx, txt in enumerate(texts):
                if not txt:
                    continue
                if len(txt) <= CHUNK_SIZE:
                    resp = translate.translate_text(
                        Text=txt,
                        SourceLanguageCode=src,
                        TargetLanguageCode=tgt
                    )
                    translations.append(resp["TranslatedText"])
                else:
                    # chunk very long input
                    translated_parts = []
                    for i in range(0, len(txt), CHUNK_SIZE):
                        part = txt[i:i+CHUNK_SIZE]
                        resp = translate.translate_text(
                            Text=part,
                            SourceLanguageCode=src,
                            TargetLanguageCode=tgt
                        )
                        translated_parts.append(resp["TranslatedText"])
                    translations.append("".join(translated_parts))

            result = {
                "metadata": {
                    "source_language": src,
                    "target_language": tgt,
                    "count": len(texts)
                },
                "translations": translations
            }

            # Save translated JSON into response bucket
            base_name = key.split("/")[-1]
            out_name = base_name.replace(".json", f".{tgt}.json")
            out_key = f"{RESPONSE_PREFIX}{out_name}"

            s3.put_object(
                Bucket=RESPONSE_BUCKET,
                Key=out_key,
                Body=json.dumps(result).encode("utf-8"),
                ContentType="application/json"
            )

        except Exception as e:
            print(f"Error processing {key}: {e}")

    return {"status": "done"}
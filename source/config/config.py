

config = {
    "kafka": {
        "bootstrap.servers": "pkc-4r087.us-west2.gcp.confluent.cloud:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "api_keyL",
        "sasl.password": "secret",
        "session_timeout_ms": 50000

    },
    "schema_registry": {
        "schema.registry.url": "https://psrc-5j7x8.us-central1.gcp.confluent.cloud",
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "api_key:secret"
    }
}

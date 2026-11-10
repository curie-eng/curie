"""Run the API with service-owned logging configuration."""

import argparse

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    # Preserve the shared service logging handlers instead of Uvicorn's defaults.
    server = uvicorn.Server(
        uvicorn.Config(
            "curie_api.main:app", host=args.host, port=args.port, log_config=None
        )
    )
    server.run()


if __name__ == "__main__":
    main()
